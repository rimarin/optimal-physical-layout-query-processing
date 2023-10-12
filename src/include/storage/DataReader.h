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
        static arrow::Result<std::shared_ptr<arrow::Table>> ReadTable(std::filesystem::path &inputFile);
};
} // storage

#endif //STORAGE_DATAREADER_H
