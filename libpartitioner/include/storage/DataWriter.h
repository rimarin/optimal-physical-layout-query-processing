#ifndef STORAGE_DATA_WRITER_H
#define STORAGE_DATA_WRITER_H

#include <filesystem>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include "partitioning/Partitioning.h"

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
        static std::shared_ptr<parquet::ParquetFileWriter> getWriter();
        static std::shared_ptr<parquet::ArrowWriterProperties> getProperties();
};
} // storage

#endif //STORAGE_DATA_WRITER_H
