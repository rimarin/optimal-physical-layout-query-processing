#ifndef STORAGE_DATAWRITER_H
#define STORAGE_DATAWRITER_H

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include "../partitioning/Partitioning.h"
#include <filesystem>

namespace storage {

class DataWriter {
    public:
        DataWriter() = default;
        virtual ~DataWriter() = default;
        static arrow::Status WriteTable(std::shared_ptr<arrow::Table>& table,
                                        std::string &filename,
                                        std::filesystem::path &outputFolder);
        static arrow::Status WritePartitions(std::vector<std::shared_ptr<arrow::Table>>& partitions,
                                             std::string &tableName,
                                             std::filesystem::path &outputFolder);
        static arrow::Result<std::shared_ptr<arrow::Table>> GenerateExampleWeatherTable();
        static arrow::Result<std::shared_ptr<arrow::Table>> GenerateExampleSchoolTable();
};
} // storage

#endif //STORAGE_DATAWRITER_H