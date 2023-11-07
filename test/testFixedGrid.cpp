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
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleWeatherTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Day", "Month"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>(partitioningColumns);
    arrow::Status statusFixedGrid = storage::DataWriter::WriteTable(*table, datasetWeatherName, fixedGridFolder, fixedGridPartitioning, partitionSizeTest);
    auto pathPartition0 = fixedGridFolder / (datasetWeatherName + "0.parquet");
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Day"), std::vector<int32_t>({1, 12, 17}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Month"), std::vector<int32_t>({1, 3, 5}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition0, "partition_id"), std::vector<int64_t>({0, 0, 0}));
    auto pathPartition1 = fixedGridFolder / (datasetWeatherName + "1.parquet");
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Day"), std::vector<int32_t>({23, 28}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Month"), std::vector<int32_t>({7, 1}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition1, "partition_id"), std::vector<int64_t>({1, 1}));
}
