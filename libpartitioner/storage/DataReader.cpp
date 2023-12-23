#include <iostream>
#include <filesystem>
#include <arrow/filesystem/api.h>
#include <arrow/filesystem/localfs.h>
#include <string>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include "storage/DataReader.h"

namespace storage {

    void exploreDataset(std::filesystem::path &base_dir){
        // Create a filesystem
        std::shared_ptr<arrow::fs::LocalFileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();

        // Create a file selector which describes which files are part of
        // the dataset.  This selector performs a recursive search of a base
        // directory which is typical with partitioned datasets.  You can also
        // create a dataset from a list of one or more paths.
        arrow::fs::FileSelector selector;
        selector.base_dir = base_dir;
        selector.recursive = true;

        // List out the files, so we can see how our data is partitioned.
        // This step is not necessary for reading a dataset
        std::vector<arrow::fs::FileInfo> file_infos = fs->GetFileInfo(selector).ValueOrDie();
        int num_printed = 0;
        for (const auto& path : file_infos) {
            if (path.IsFile()) {
                std::cout << path.path().substr(base_dir.string().size()) << std::endl;
                if (++num_printed == 10) {
                    std:: cout << "..." << std::endl;
                    break;
                }
            }
        }
    }

    uint64_t getTotalRowNumber(std::string& filePath) {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(filePath, arrow::default_memory_pool()));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
        std::shared_ptr<parquet::FileMetaData> metadata = reader->parquet_reader()->metadata();
        return metadata->num_rows();
    }

    arrow::Result<std::shared_ptr<arrow::Table>> DataReader::readTable(std::filesystem::path &inputFile) {
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

    // TODO: reduce memory usage, see:
    //  https://github.com/lsst/qserv/blob/a5dbf4175159b874da1cb0907533ba6e3ffd5e7d/src/partition/ParquetInterface.cc#L114
    arrow::Result<std::shared_ptr<arrow::Table>> DataReader::batchReadTable(std::filesystem::path &inputFile) {
        arrow::MemoryPool* pool = arrow::default_memory_pool();

        // Enable parallel column decoding
        auto reader_properties = parquet::ReaderProperties(pool);

        reader_properties.set_buffer_size(4096 * 4);
        reader_properties.enable_buffered_stream();

        // Configure Arrow-specific Parquet reader settings
        auto arrow_reader_props = parquet::ArrowReaderProperties(/*use_threads=*/true);
        arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024

        parquet::arrow::FileReaderBuilder reader_builder;
        ARROW_RETURN_NOT_OK(
                reader_builder.OpenFile(inputFile, /*memory_map=*/false, reader_properties));
        reader_builder.memory_pool(pool);
        reader_builder.properties(arrow_reader_props);

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ARROW_ASSIGN_OR_RAISE(arrow_reader, reader_builder.Build());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(&rb_reader));

        uint64_t numRows = 0;
        auto nin = rb_reader->ToRecordBatches();
        for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *rb_reader) {
            // Operate on each batch...
            auto b = maybe_batch.ValueOrDie();
            auto batchNumRows = b->num_rows();
            numRows += batchNumRows;
            std::cout << "Read record batch with " << batchNumRows << " rows";
            b.reset();
        }

        // auto a = rb_reader->ToTable();
        std::shared_ptr<arrow::Table> table;
        // ARROW_RETURN_NOT_OK(arrow_reader->ReadColumn(1, *table));
        std::cout << "[DataReader] Read table from file " << inputFile.string() << std::endl;
        // return table;
        return nullptr;
    }



    arrow::Result<std::vector<std::shared_ptr<arrow::Array>>>
    DataReader::getColumns(std::shared_ptr<arrow::Table> &table, std::vector<std::string> &columns) {
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

} // storage