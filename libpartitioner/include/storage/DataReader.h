#ifndef STORAGE_DATAREADER_H
#define STORAGE_DATAREADER_H

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/api/reader.h>
#include "../partitioning/Partitioning.h"
#include <filesystem>

namespace storage {

class DataReader {
    public:
        DataReader() = default;
        virtual ~DataReader() = default;
        static arrow::Result<std::shared_ptr<arrow::Table>> readTable(std::filesystem::path &inputFile);
        static arrow::Result<std::vector<std::shared_ptr<arrow::Array>>> getColumns(std::shared_ptr<arrow::Table> &table,
                                                                                    std::vector<std::string> &columns);
};
} // storage

#endif //STORAGE_DATAREADER_H