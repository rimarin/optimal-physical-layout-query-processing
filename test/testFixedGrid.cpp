#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/FixedGridPartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGrid){
    cleanUpFolder(fixedGridFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> exampleTable = storage::DataWriter::GenerateExampleWeatherTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / fixedGridFolder;
    std::vector<std::string> fixedGridPartitioningColumns = {"Day", "Month"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>(fixedGridPartitioningColumns, 20);
    arrow::Status statusFixedGrid = storage::DataWriter::WriteTable(*exampleTable, example1TableName, outputFolder,fixedGridPartitioning);
    auto expectedPath = std::filesystem::current_path() / fixedGridFolder / (example1TableName + "0.parquet");
    ASSERT_EQ(std::filesystem::exists(expectedPath), true);
    std::shared_ptr<arrow::Table> partition0 = storage::DataReader::ReadTable(expectedPath).ValueOrDie();
    std::vector<arrow::Datum> columnData;
    auto day = std::static_pointer_cast<arrow::Int32Array>(partition0->GetColumnByName("Day")->chunk(0));
    std::vector<int32_t> day_vector;
    for (int64_t i = 0; i < day->length(); ++i)
    {
        day_vector.push_back(day->Value(i));
    }
    ASSERT_EQ(day_vector, std::vector<int32_t>({1, 12, 17}));
    auto month = std::static_pointer_cast<arrow::Int32Array>(partition0->GetColumnByName("Month")->chunk(0));
    std::vector<int32_t> month_vector;
    for (int64_t i = 0; i < month->length(); ++i)
    {
        month_vector.push_back(month->Value(i));
    }
    ASSERT_EQ(month_vector, std::vector<int32_t>({1, 3, 5}));
    auto partitionId = std::static_pointer_cast<arrow::Int64Array>(partition0->GetColumnByName("partition_id")->chunk(0));
    std::vector<int32_t> partition_id_vector;
    for (int64_t i = 0; i < partitionId->length(); ++i)
    {
        partition_id_vector.push_back(partitionId->Value(i));
    }
    ASSERT_EQ(partition_id_vector, std::vector<int32_t>({0, 0, 0}));
}
