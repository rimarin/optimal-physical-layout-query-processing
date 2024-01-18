#ifndef STORAGE_DATA_READER_H
#define STORAGE_DATA_READER_H

#include <filesystem>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/api/reader.h>

#include "common/Settings.h"

namespace storage {

class DataReader {
    public:
        DataReader() = default;
        virtual ~DataReader() = default;
        arrow::Status load(std::filesystem::path &filePath, bool useBatchRead = false);
        std::filesystem::path getReaderPath();
        arrow::Result<std::shared_ptr<arrow::Table>> readTable();
        arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> getBatchReader();
        static arrow::Result<std::vector<std::shared_ptr<arrow::Array>>> getColumnsOld(const std::shared_ptr<arrow::Table> &table,
                                                                                       const std::vector<std::string> &columns);
        arrow::Result<std::vector<std::shared_ptr<arrow::ChunkedArray>>> getColumns(const std::vector<std::string> &columns);
        arrow::Result<std::shared_ptr<arrow::ChunkedArray>> getColumn(const std::string &columnName);
        void displayFileProperties();
        arrow::Result<std::pair<double_t, double_t>> getColumnStats(const std::string &columnName);
        uint32_t getNumRows();
        uint32_t getExpectedNumBatches();
        static arrow::Result<std::shared_ptr<arrow::Table>> getTable(std::filesystem::path &inputFile);
        static std::filesystem::path getDatasetPath(const std::filesystem::path &folder, const std::string &datasetName,
                                                    const std::string &partitioningScheme);
        arrow::Result<int> getColumnIndex(const std::string &columnName);
    private:
        std::filesystem::path path;
        bool batchRead = false;
        bool isFolder = false;
        std::unique_ptr<parquet::arrow::FileReader> reader;
        std::shared_ptr<parquet::FileMetaData> metadata;
};
} // storage

#endif //STORAGE_DATA_READER_H
