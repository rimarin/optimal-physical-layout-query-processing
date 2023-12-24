#include <iostream>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include "storage/TableGenerator.h"

namespace storage {

    arrow::Result<std::shared_ptr<arrow::Table>> TableGenerator::GenerateWeatherTable() {
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

    arrow::Result<std::shared_ptr<arrow::Table>> TableGenerator::GenerateSchoolTable() {
        arrow::Int32Builder int32Builder;
        int32_t student_id_raw[8] = {16, 45, 21, 7, 74, 34, 111, 91};
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(student_id_raw, 8));
        std::shared_ptr<arrow::Array> student_id;
        ARROW_ASSIGN_OR_RAISE(student_id, int32Builder.Finish());

        int32_t age_raw[8] = {30, 21, 18, 27, 41, 37, 23, 22};
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(age_raw, 8));
        std::shared_ptr<arrow::Array> age;
        ARROW_ASSIGN_OR_RAISE(age, int32Builder.Finish());

        std::vector<std::shared_ptr<arrow::Array>> columns = {student_id, age};

        std::shared_ptr<arrow::Field> field_student_id, field_age;
        std::shared_ptr<arrow::Schema> schema;

        field_student_id = arrow::field("Student_id", arrow::int32());
        field_age = arrow::field("Age", arrow::int32());

        schema = arrow::schema({field_student_id, field_age});

        std::shared_ptr<arrow::Table> table;
        table = arrow::Table::Make(schema, columns);
        std::cout << "[DataWriter] Generated ExampleSchoolTable" << std::endl;
        return table;
    }

    arrow::Result <std::shared_ptr<arrow::Table>> TableGenerator::GenerateCitiesTable() {
        arrow::StringBuilder stringBuilder;
        std::vector <std::string> city_name = {"Toronto", "Buffalo", "Denver", "Chicago", "Omaha", "Mobile", "Atlanta",
                                               "Miami"};
        ARROW_RETURN_NOT_OK(stringBuilder.AppendValues(city_name));
        std::shared_ptr <arrow::Array> city;
        ARROW_ASSIGN_OR_RAISE(city, stringBuilder.Finish());

        arrow::Int32Builder int32Builder;
        int32_t x[8] = {62, 82, 5, 35, 27, 52, 85, 90};
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(x, 8));
        std::shared_ptr <arrow::Array> coord_x;
        ARROW_ASSIGN_OR_RAISE(coord_x, int32Builder.Finish());

        int32Builder.Reset();
        int32_t y[8] = {77, 65, 45, 42, 35, 10, 15, 5};
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(y, 8));
        std::shared_ptr <arrow::Array> coord_y;
        ARROW_ASSIGN_OR_RAISE(coord_y, int32Builder.Finish());

        std::vector <std::shared_ptr<arrow::Array>> columns = {city, coord_x, coord_y};

        std::shared_ptr <arrow::Field> field_city, field_x, field_y;
        std::shared_ptr <arrow::Schema> schema;

        field_city = arrow::field("city", arrow::utf8());
        field_x = arrow::field("x", arrow::int32());
        field_y = arrow::field("y", arrow::int32());

        schema = arrow::schema({field_city, field_x, field_y});

        std::shared_ptr <arrow::Table> table;
        table = arrow::Table::Make(schema, columns);
        std::cout << "[DataWriter] Generated ExampleCitiesTable" << std::endl;
        return table;
    }
}