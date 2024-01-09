#include <filesystem>
#include <iostream>
#include <string>

#include <arrow/compute/kernel.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>

#include <parquet/arrow/reader.h>
#include "storage/DataReader.h"

namespace storage {

    arrow::Status DataReader::load(std::filesystem::path &filePath, bool useBatchRead){
        path = filePath;
        batchRead = useBatchRead;
        if (std::filesystem::path(path).has_filename()){
            isFolder = false;

            arrow::MemoryPool* pool = arrow::default_memory_pool();

            // Enable parallel column decoding
            auto reader_properties = parquet::ReaderProperties(pool);

            reader_properties.set_buffer_size(bufferSize);
            reader_properties.enable_buffered_stream();

            // Configure Arrow-specific Parquet reader settings
            auto arrow_reader_props = parquet::ArrowReaderProperties(/*use_threads=*/false);
            arrow_reader_props.set_batch_size(batchSize);  // default 64 * 1024

            parquet::arrow::FileReaderBuilder reader_builder;
            ARROW_RETURN_NOT_OK(reader_builder.OpenFile(path, /*memory_map=*/false, reader_properties));
            reader_builder.memory_pool(pool);
            reader_builder.properties(arrow_reader_props);

            ARROW_ASSIGN_OR_RAISE(reader, reader_builder.Build());

            std::cout << "[DataReader] Loaded file reader for file " << path << std::endl;

            metadata = reader->parquet_reader()->metadata();

            displayFileProperties();

            return arrow::Status::OK();
        }
        else{
            return arrow::Status::Invalid("Multi-file read not supported for now");
        }
    }

    void DataReader::displayFileProperties(){
        std::cout << "[DataReader] File has " << metadata->num_columns() << " columns" << std::endl;
        std::cout << "[DataReader] File has " << metadata->num_row_groups() << " row groups" << std::endl;
        std::cout << "[DataReader] File has " << metadata->num_rows() << " rows" << std::endl;
    }

    std::filesystem::path DataReader::getReaderPath(){
        return path;
    }

    uint32_t DataReader::getNumRows() {
        return metadata->num_rows();
    }

    uint32_t DataReader::getExpectedNumBatches(){
        return (uint32_t) std::ceil((float) getNumRows() / (float) batchSize);
    }

    arrow::Result<std::shared_ptr<arrow::Table>> DataReader::getTable(std::filesystem::path &inputFile){
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::shared_ptr<arrow::io::RandomAccessFile> input;
        ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(inputFile));

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

        std::shared_ptr<arrow::Table> table;
        ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));
        std::cout << "[DataReader] Read table from file " << inputFile.string() << std::endl;
        return table;
    }

    arrow::Result<std::shared_ptr<arrow::Table>> DataReader::readTable() {
        std::shared_ptr<arrow::Table> table;
        ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
        std::cout << "[DataReader] Read entire table" << std::endl;
        return table;
    }

    std::filesystem::path DataReader::getDatasetPath(const std::filesystem::path &folder, const std::string &datasetName,
                                         const std::string &partitioningScheme){
        return folder / datasetName / partitioningScheme / (datasetName + ".parquet");
    }

    // TODO: reduce memory usage, see:
    //  https://github.com/lsst/qserv/blob/a5dbf4175159b874da1cb0907533ba6e3ffd5e7d/src/partition/ParquetInterface.cc#L114
    arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> DataReader::getTableBatchReader() {
        std::shared_ptr<arrow::RecordBatchReader> rb_reader;
        ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(&rb_reader));
        std::cout << "[DataReader] Obtained record batch reader" << std::endl;
        return rb_reader;
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::Array>>>
    DataReader::getColumnsOld(const std::shared_ptr<arrow::Table> &table, const std::vector<std::string> &columns) {
        if (table == nullptr){
            throw std::invalid_argument("Cannot getColumns from empty table");
        }
        std::vector<arrow::compute::InputType> inputTypes;
        std::vector<std::shared_ptr<arrow::Array>> columnData;
        for (const auto &column: columns){
            // Infer the data types of the columns
            auto columnByName = table->schema()->GetFieldByName(column);
            if (columnByName == nullptr){
                throw std::invalid_argument("Column with name <" + column + "> could not be loaded, check the column name");
            }
            auto columnType = columnByName->type();
            std::cout << "Reading column <" << column << "> of type " << columnType->ToString() << std::endl;
            inputTypes.emplace_back(columnType);
            // Extract column data by getting the chunks and casting them to an arrow array
            auto chunkedColumn = table->GetColumnByName(column);
            auto chunk = chunkedColumn->chunk(0);
            columnData.emplace_back(chunk);
        }
        return columnData;
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::ChunkedArray>>> DataReader::getColumns(const std::vector<std::string> &columns){
        std::vector<std::shared_ptr<arrow::ChunkedArray>> columnsArray;
        columnsArray.reserve(columns.size());
        for (const auto &column: columns){
            columnsArray.emplace_back(getColumn(column).ValueOrDie());
        }
        return columnsArray;
    }

    arrow::Result<std::shared_ptr<arrow::ChunkedArray>> DataReader::getColumn(const std::string &columnName){
        std::cout << "[DataReader] Reading column <" << columnName << "> " << std::endl;
        std::shared_ptr<arrow::ChunkedArray> columnArray;
        auto fieldIndex = getColumnIndex(columnName).ValueOrDie();
        if (fieldIndex == -1){
            return arrow::Status::Invalid("Column name <" + columnName + "> not found in schema");
        }
        PARQUET_THROW_NOT_OK(reader->ReadColumn(fieldIndex, &columnArray));
        std::cout << "[DataReader] Column has type <" << columnArray->type()->ToString() << "> " << std::endl;
        return columnArray;
    }

    arrow::Result<int> DataReader::getColumnIndex(const std::string &columnName){
        std::shared_ptr<arrow::Schema> schema;
        ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));
        return schema->GetFieldIndex(columnName);
    }

    arrow::Result<std::pair<double_t, double_t>> DataReader::getColumnStats(const std::string &columnName){
        auto columnIndex = getColumnIndex(columnName).ValueOrDie();
        auto numRowGroups = metadata->num_row_groups();
        std::unique_ptr<parquet::RowGroupMetaData> rowGroup = metadata->RowGroup(0);
        std::shared_ptr<parquet::Statistics> stats = (rowGroup->ColumnChunk(columnIndex))->statistics();
        const auto *typed_stats = static_cast<const parquet::TypedStatistics<arrow::Int32Type>*>(stats.get());
        auto minValue = typed_stats->min();
        auto maxValue = typed_stats->max();
        for (int i = 1; i < numRowGroups; ++i){
            rowGroup = metadata->RowGroup(i);
            stats = (rowGroup->ColumnChunk(columnIndex))->statistics();
            typed_stats = static_cast<const parquet::TypedStatistics<arrow::Int32Type>*>(stats.get());
            minValue = std::min(minValue, typed_stats->min());
            maxValue = std::max(maxValue, typed_stats->max());
        }
        return std::make_pair(minValue, maxValue);
    }

} // storage