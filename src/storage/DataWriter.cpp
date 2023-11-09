#include <iostream>
#include <filesystem>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include "../include/storage/DataWriter.h"

namespace storage {

    arrow::Result<std::shared_ptr<arrow::Table>> DataWriter::GenerateExampleWeatherTable() {
        arrow::Int32Builder int32builder;
        int32_t days_raw[5] = {1, 12, 17, 23, 28};
        ARROW_RETURN_NOT_OK(int32builder.AppendValues(days_raw, 5));
        std::shared_ptr<arrow::Array> days;
        ARROW_ASSIGN_OR_RAISE(days, int32builder.Finish());

        int32_t months_raw[5] = {1, 3, 5, 7, 1};
        ARROW_RETURN_NOT_OK(int32builder.AppendValues(months_raw, 5));
        std::shared_ptr<arrow::Array> months;
        ARROW_ASSIGN_OR_RAISE(months, int32builder.Finish());

        int32_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
        ARROW_RETURN_NOT_OK(int32builder.AppendValues(years_raw, 5));
        std::shared_ptr<arrow::Array> years;
        ARROW_ASSIGN_OR_RAISE(years, int32builder.Finish());

        arrow::DoubleBuilder doubleBuilder;
        double_t temperature_raw[5] = {0.4, 43.32, 23.43, 24.33, 13.34};
        ARROW_RETURN_NOT_OK(doubleBuilder.AppendValues(temperature_raw, 5));
        std::shared_ptr<arrow::Array> temperature;
        ARROW_ASSIGN_OR_RAISE(temperature, doubleBuilder.Finish());

        double_t humidity_raw[5] = {34, 23, 78, 48, 10};
        ARROW_RETURN_NOT_OK(doubleBuilder.AppendValues(humidity_raw, 5));
        std::shared_ptr<arrow::Array> humidity;
        ARROW_ASSIGN_OR_RAISE(humidity, doubleBuilder.Finish());

        std::vector<std::shared_ptr<arrow::Array>> columns = {days, months, years, temperature, humidity};

        std::shared_ptr<arrow::Field> field_day, field_month, field_year, field_temperature, field_humidity;
        std::shared_ptr<arrow::Schema> schema;

        field_day = arrow::field("Day", arrow::int32());
        field_month = arrow::field("Month", arrow::int32());
        field_year = arrow::field("Year", arrow::int32());
        field_temperature = arrow::field("Temperature", arrow::float64());
        field_humidity = arrow::field("Humidity", arrow::float64());

        schema = arrow::schema({field_day, field_month, field_year, field_temperature, field_humidity});

        std::shared_ptr<arrow::Table> table;
        table = arrow::Table::Make(schema, columns);
        std::cout << "[DataWriter] Generated ExampleWeatherTable" << std::endl;
        return table;
    }

    arrow::Result<std::shared_ptr<arrow::Table>> DataWriter::GenerateExampleSchoolTable() {
        arrow::DoubleBuilder doubleBuilder;
        double_t student_id_raw[8] = {16, 45, 21, 7, 74, 34, 111, 91};
        ARROW_RETURN_NOT_OK(doubleBuilder.AppendValues(student_id_raw, 8));
        std::shared_ptr<arrow::Array> student_id;
        ARROW_ASSIGN_OR_RAISE(student_id, doubleBuilder.Finish());

        double_t age_raw[8] = {30, 21, 18, 27, 41, 37, 23, 22};
        ARROW_RETURN_NOT_OK(doubleBuilder.AppendValues(age_raw, 8));
        std::shared_ptr<arrow::Array> age;
        ARROW_ASSIGN_OR_RAISE(age, doubleBuilder.Finish());

        std::vector<std::shared_ptr<arrow::Array>> columns = {student_id, age};

        std::shared_ptr<arrow::Field> field_student_id, field_age;
        std::shared_ptr<arrow::Schema> schema;

        field_student_id = arrow::field("Student_id", arrow::float64());
        field_age = arrow::field("Age", arrow::float64());

        schema = arrow::schema({field_student_id, field_age});

        std::shared_ptr<arrow::Table> table;
        table = arrow::Table::Make(schema, columns);
        std::cout << "[DataWriter] Generated ExampleSchoolTable" << std::endl;
        return table;
    }

    arrow::Status DataWriter::WriteTable(std::shared_ptr<arrow::Table>& table,
                                         std::string &filename,
                                         std::filesystem::path &outputFolder) {
        auto debug = table->ToString();
        std::string outPath = outputFolder.string();
        std::string outFilename = outPath + "/" + filename;
        auto outfile = arrow::io::FileOutputStream::Open(outFilename);
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outfile, table->num_rows()));
        std::cout << "[DataWriter] Written table to " << outFilename << std::endl;
        return arrow::Status::OK();
    }

    arrow::Status DataWriter::WritePartitions(std::vector<std::shared_ptr<arrow::Table>>& partitions,
                                              std::string &tableName,
                                              std::filesystem::path &outputFolder) {
        for (int32_t i = 0; i < partitions.size(); i++) {
            std::string filename = tableName + std::to_string(i) + ".parquet";
            ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTable(partitions[i], filename, outputFolder));
        }
        return arrow::Status::OK();
    }

} // storage