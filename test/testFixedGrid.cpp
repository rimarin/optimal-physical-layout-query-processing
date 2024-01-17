#include <arrow/io/api.h>
#include <filesystem>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridWeather){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetWeather);
    cleanUpFolder(folder);
    auto dataReader = std::make_shared<storage::DataReader>();
    std::vector<std::string> partitioningColumns = {"Day", "Month"};
    auto partitionSize = 2;
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Day", std::vector<int32_t>({1, 28})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Month", std::vector<int32_t>({1, 1})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Day", std::vector<int32_t>({12, 17})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Month", std::vector<int32_t>({3, 5})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("2" + fileExtension), "Day", std::vector<int32_t>({23})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("2" + fileExtension), "Month", std::vector<int32_t>({7})), arrow::Status::OK());
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridCities){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetCities);
    cleanUpFolder(folder);
    auto dataReader = std::make_shared<storage::DataReader>();
    std::vector<std::string> partitioningColumns = {"x", "y"};
    auto partitionSize = 2;
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("0" + fileExtension), "city", std::vector<std::string>({"Moscow", "Madrid"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("1" + fileExtension), "city", std::vector<std::string>({"Oslo", "Amsterdam"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("2" + fileExtension), "city", std::vector<std::string>({"Dublin", "Copenhagen"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("3" + fileExtension), "city", std::vector<std::string>({"Tallinn", "Berlin"})), arrow::Status::OK());
}
