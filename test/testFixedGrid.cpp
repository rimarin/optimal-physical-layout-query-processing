#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "include/partitioning/FixedGridPartitioning.h"
#include "include/storage/DataReader.h"
#include "include/storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGrid){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto datasetWeather = getDatasetPath(ExperimentsConfig::datasetWeather);
    auto datasetCities = getDatasetPath(ExperimentsConfig::datasetCities);
    auto partitionSize = 2;
    cleanUpFolder(folder);
    auto dataReader = storage::DataReader();
    ASSERT_EQ(dataReader.load(datasetCities), arrow::Status::OK());
    std::vector<std::string> partitioningColumns = {"x", "y"};
    std::shared_ptr<partitioning::FixedGridPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>();
    ASSERT_EQ(fixedGridPartitioning->partition(dataReader, partitioningColumns, partitionSize, folder), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition0, "city"), std::vector<std::string>({"Denver", "Mobile"}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "x"), std::vector<int32_t>({5, 52}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition0, "y"), std::vector<uint32_t>({45, 10}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition1, "city"), std::vector<std::string>({"Omaha", "Atlanta"}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "x"), std::vector<int32_t>({27, 85}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition1, "y"), std::vector<uint32_t>({35, 15}));
    auto pathPartition2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition2, "city"), std::vector<std::string>({"Chicago", "Miami"}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "x"), std::vector<int32_t>({35, 90}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition2, "y"), std::vector<uint32_t>({42, 5}));
    auto pathPartition3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition3, "city"), std::vector<std::string>({"Toronto", "Buffalo"}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "x"), std::vector<int32_t>({62, 82}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition3, "y"), std::vector<uint32_t>({77, 65}));
    cleanUpFolder(folder);
    partitionSize = 3;
    partitioningColumns = {"Day", "Month"};
    ASSERT_EQ(dataReader.load(datasetWeather), arrow::Status::OK());
    ASSERT_EQ(fixedGridPartitioning->partition(dataReader, partitioningColumns, partitionSize, folder), arrow::Status::OK());
    pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Day"), std::vector<int32_t>({1, 12, 17}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Month"), std::vector<int32_t>({1, 3, 5}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition0, "partition_id"), std::vector<uint32_t>({0, 0, 0}));
    pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Day"), std::vector<int32_t>({23, 28}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Month"), std::vector<int32_t>({7, 1}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition1, "partition_id"), std::vector<uint32_t>({1, 1}));
}
