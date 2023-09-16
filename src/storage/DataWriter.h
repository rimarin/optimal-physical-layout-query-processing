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

namespace storage {

class DataWriter {
    public: explicit DataWriter(std::filesystem::path &outputFolder);
    public: arrow::Status WriteTable(const std::shared_ptr<arrow::Table>& table, std::string &filename);
    public: static std::shared_ptr<arrow::Table> GenerateExampleTable();
    private: std::filesystem::path outFolder;
    };
} // storage

#endif //STORAGE_DATAWRITER_H
