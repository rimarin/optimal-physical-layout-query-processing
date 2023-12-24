#ifndef STORAGE_TABLE_GENERATOR_H
#define STORAGE_TABLE_GENERATOR_H

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/api/reader.h>

namespace storage {

    class TableGenerator {
    public:
        static arrow::Result<std::shared_ptr<arrow::Table>> GenerateWeatherTable();
        static arrow::Result<std::shared_ptr<arrow::Table>> GenerateSchoolTable();
        static arrow::Result<std::shared_ptr<arrow::Table>> GenerateCitiesTable();
    };

} // storage

#endif //STORAGE_TABLE_GENERATOR_H
