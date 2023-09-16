#include "../../include/storage/DataWriter.h"
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <filesystem>

namespace storage {

    arrow::Result<std::shared_ptr<arrow::Table>> DataWriter::GenerateExampleTable() {
        arrow::Int8Builder int8builder;
        int8_t days_raw[5] = {1, 12, 17, 23, 28};
        ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
        std::shared_ptr<arrow::Array> days;
        ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());

        int8_t months_raw[5] = {1, 3, 5, 7, 1};
        ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
        std::shared_ptr<arrow::Array> months;
        ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());

        arrow::Int16Builder int16builder;
        int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
        ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
        std::shared_ptr<arrow::Array> years;
        ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());

        std::vector<std::shared_ptr<arrow::Array>> columns = {days, months, years};

        std::shared_ptr<arrow::Field> field_day, field_month, field_year;
        std::shared_ptr<arrow::Schema> schema;

        field_day = arrow::field("Day", arrow::int8());
        field_month = arrow::field("Month", arrow::int8());
        field_year = arrow::field("Year", arrow::int16());

        schema = arrow::schema({field_day, field_month, field_year});

        std::shared_ptr<arrow::Table> table;
        table = arrow::Table::Make(schema, columns);
        return table;
    }

    arrow::Status DataWriter::WriteTable(const std::shared_ptr<arrow::Table>& table,
                                         std::string &filename,
                                         std::filesystem::path &outputFolder,
                                         const std::shared_ptr<partitioning::MultiDimensionalPartitioning> &partitioningMethod) {
        auto partitions = partitioningMethod->partition(table);
        std::string outPath = outputFolder.string().append(filename.append(".parquet"));
        auto outfile = arrow::io::FileOutputStream::Open(outPath);
        PARQUET_THROW_NOT_OK(
                parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outfile, 5));

        return arrow::Status::OK();
    }

} // storage