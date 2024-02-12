#include <filesystem>
#include <iostream>
#include <string>
#include <time.h>

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

            reader_properties.set_buffer_size(common::Settings::bufferSize);
            reader_properties.enable_buffered_stream();

            // Configure Arrow-specific Parquet reader settings
            auto arrow_reader_props = parquet::ArrowReaderProperties(/*use_threads=*/false);
            arrow_reader_props.set_batch_size(common::Settings::batchSize);  // default 64 * 1024

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
        // TODO: maybe replace with DuckDB query metadata
        return metadata->num_rows();
    }

    uint32_t DataReader::getExpectedNumBatches(){
        return (uint32_t) std::ceil((float) getNumRows() / (float) common::Settings::batchSize);
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

    std::filesystem::path DataReader::getDatasetPath(const std::filesystem::path &folder, const std::string &datasetName,
                                         const std::string &partitioningScheme){
        return folder / datasetName / partitioningScheme / (datasetName + common::Settings::fileExtension);
    }

    // TODO: reduce memory usage, see:
    //  https://github.com/lsst/qserv/blob/a5dbf4175159b874da1cb0907533ba6e3ffd5e7d/src/partition/ParquetInterface.cc#L114
    arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> DataReader::getBatchReader() {
        std::shared_ptr<arrow::RecordBatchReader> recordBatchReader;
        ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(&recordBatchReader));
        std::cout << "[DataReader] Obtained record batch reader" << std::endl;
        return recordBatchReader;
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

        duckdb::DuckDB db(nullptr);
        duckdb::Connection con(db);

        std::string datasetFilePath = getReaderPath();
        const std::string query = "SELECT stats_min, stats_max "
                                  "FROM parquet_metadata('" + datasetFilePath + "') "
                                  "WHERE path_in_schema = '" + columnName + "'";
        auto results = con.Query(query);

        std::vector<double> minValues;
        std::vector<double> maxValues;

        for (const auto &row : *results) {
            double currentMin;
            double currentMax;
            // TODO: check column type int96 instead of name of column
            // int96/timestamp and parquet are not a good match, see
            // https://issues.apache.org/jira/browse/PARQUET-840
            std::set<std::string> timeColumns = {"tpep_pickup_datetime", "tpep_dropoff_datetime",
                                                 "created_at",
                                                 "o_orderdate", "l_shipdate"};
            if (timeColumns.find(columnName) != timeColumns.end()){
                std::string sMin{row.GetValue<std::string>(0)};
                std::string sMax{row.GetValue<std::string>(1)};
                std::tm tMin{};
                std::tm tMax{};
                std::istringstream ssMin(sMin);
                std::istringstream ssMax(sMax);

                ssMin >> std::get_time(&tMin, "%Y-%m-%d %H:%M:%S");
                ssMax >> std::get_time(&tMax, "%Y-%m-%d %H:%M:%S");

                currentMin = mktime(&tMin);
                currentMax = mktime(&tMax);
            }
            else{
                currentMin = row.GetValue<double>(0);
                currentMax = row.GetValue<double>(1);
            }
            minValues.emplace_back(currentMin);
            maxValues.emplace_back(currentMax);
        }

        double minValue = *std::min_element(std::begin(minValues), std::end(minValues));
        double maxValue = *std::max_element(std::begin(maxValues), std::end(maxValues));

        return std::make_pair(minValue, maxValue);
    }

    arrow::Result<double_t> DataReader::getMedian(const std::string &columnName){

        duckdb::DuckDB db(nullptr);
        duckdb::Connection con(db);

        std::string datasetFilePath = getReaderPath();

        // Load parquet file into memory and sort it
        std::string loadQuery = "CREATE TABLE tbl AS "
                                "SELECT * FROM read_parquet('" + datasetFilePath + "') "
                                "ORDER BY '" + columnName + "'";
        auto loadQueryResult = con.Query(loadQuery);

        // Extract median rows
        auto numRows = getNumRows();
        auto limit = (numRows % 2 == 0) ? 2 : 1;

        std::string limitQuery = "SELECT AVG(" + columnName + ") "
                                 "FROM tbl "
                                 "LIMIT " + std::to_string(limit) +
                                 "OFFSET " + std::to_string(numRows / 2) + " )";

        auto limitQueryResult = con.Query(limitQuery);

        double medianValue;

        for (const auto &row : *limitQueryResult) {
            medianValue = row.GetValue<double>(0);
        }

        return medianValue;
    }

    arrow::Status DataReader::rangeFilter(const std::string &columnName,
                                          const std::filesystem::path &destinationFile,
                                          const std::pair<double, double> range){

        duckdb::DuckDB db(nullptr);
        duckdb::Connection con(db);

        std::string datasetFilePath = getReaderPath();

        // Load parquet file into memory and sort it
        std::string loadQuery = "CREATE TABLE tbl AS "
                                "SELECT * FROM read_parquet('" + datasetFilePath + "')";
        auto loadQueryResult = con.Query(loadQuery);

        // Apply filter
        std::string filterQuery = "COPY (SELECT * "
                                  "      FROM tbl "
                                  "      WHERE " + columnName + " >= " + std::to_string(range.first) + " AND "
                                                 + columnName + " <= " + std::to_string(range.second) + ") "
                                  "TO '" + destinationFile.string() + ".parquet' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 131072)";;
        auto filterQueryResult = con.Query(filterQuery);

        return arrow::Status::OK();
    }

} // storage