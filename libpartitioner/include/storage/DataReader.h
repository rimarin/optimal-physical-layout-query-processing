#ifndef STORAGE_DATA_READER_H
#define STORAGE_DATA_READER_H

#include <filesystem>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/api/reader.h>
#include <regex>

#include "duckdb.hpp"
#include "common/Settings.h"

namespace storage {

class DataReader {
    public:
        DataReader() = default;
        virtual ~DataReader() = default;
        arrow::Status load(std::filesystem::path &filePath, bool useBatchRead = false);
        std::filesystem::path getReaderPath();
        arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> getBatchReader();
        arrow::Result<std::vector<std::shared_ptr<arrow::ChunkedArray>>> getColumns(const std::vector<std::string> &columns);
        arrow::Result<std::shared_ptr<arrow::ChunkedArray>> getColumn(const std::string &columnName);
        void displayFileProperties();
        arrow::Result<std::pair<double_t, double_t>> getColumnStats(const std::string &columnName);
        int64_t getNumRows();
        int64_t getExpectedNumBatches();
        static arrow::Result<std::shared_ptr<arrow::Table>> getTable(std::filesystem::path &inputFile);
        static std::filesystem::path getDatasetPath(const std::filesystem::path &folder, const std::string &datasetName,
                                                    const std::string &partitioningScheme);
        arrow::Result<int> getColumnIndex(const std::string &columnName);
        arrow::Result<double_t> getMedian(const std::string &columnName);
        arrow::Status rangeFilter(const std::string &columnName, const std::filesystem::path &destinationFile,
                                  const std::pair<double, double> range);
    private:
        std::filesystem::path path;
        bool batchRead = false;
        bool isFolder = false;
        std::unique_ptr<parquet::arrow::FileReader> reader;
        std::shared_ptr<parquet::FileMetaData> metadata;
};
} // storage

#endif //STORAGE_DATA_READER_H
