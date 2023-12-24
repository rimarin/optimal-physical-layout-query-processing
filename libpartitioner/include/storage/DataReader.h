#ifndef STORAGE_DATA_READER_H
#define STORAGE_DATA_READER_H

#include <filesystem>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/api/reader.h>

#include "partitioning/Partitioning.h"

namespace storage {

class DataReader {
    public:
        DataReader() = default;
        virtual ~DataReader() = default;
        arrow::Status load(std::filesystem::path &filePath, bool useBatchRead = false);
        arrow::Result<std::shared_ptr<arrow::Table>> readTable();
        arrow::Result<std::shared_ptr<::arrow::RecordBatchReader>> getTableBatchReader();
        static arrow::Result<std::vector<std::shared_ptr<arrow::Array>>> getColumns(std::shared_ptr<arrow::Table> &table,
                                                                                    std::vector<std::string> &columns);
        arrow::Result<std::vector<std::shared_ptr<arrow::ChunkedArray>>> getColumns(const std::vector<std::string> &columns);
        void displayFileProperties();
        arrow::Result<std::pair<uint64_t, uint64_t>> getColumnStats(const std::string &columnName);
        uint64_t getNumRows();
        static arrow::Result<std::shared_ptr<arrow::Table>> getTable(std::filesystem::path &inputFile);
        static std::filesystem::path getDatasetPath(const std::filesystem::path &folder, const std::string &datasetName,
                                                    const std::string &partitioningTechnique);
    private:
        std::filesystem::path path;
        bool batchRead = false;
        bool isFolder = false;
        std::unique_ptr<parquet::arrow::FileReader> reader;
        std::shared_ptr<parquet::FileMetaData> metadata;
        arrow::Result<std::shared_ptr<arrow::ChunkedArray>> getColumn(const std::string &columnName);
        arrow::Result<int> getColumnIndex(const std::string &columnName);
};
} // storage

#endif //STORAGE_DATA_READER_H
