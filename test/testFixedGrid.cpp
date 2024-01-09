#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/DataReader.h"
#include "storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGrid){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto datasetWeather = getDatasetPath(ExperimentsConfig::datasetWeather);
    cleanUpFolder(folder);
    auto dataReader = std::make_shared<storage::DataReader>();
    std::vector<std::string> partitioningColumns = {"Day", "Month"};
    auto partitionSize = 2;
    ASSERT_EQ(dataReader->load(datasetWeather), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Day"), std::vector<int32_t>({1, 12}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Month"), std::vector<int32_t>({1, 3}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Day"), std::vector<int32_t>({17, 28}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Month"), std::vector<int32_t>({5, 1}));
    auto pathPartition2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Day"), std::vector<int32_t>({23}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Month"), std::vector<int32_t>({7}));
}
