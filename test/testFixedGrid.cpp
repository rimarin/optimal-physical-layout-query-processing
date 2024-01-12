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
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition0, "city"), std::vector<std::string>({"Moscow", "Madrid"}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition1, "city"), std::vector<std::string>({"Oslo", "Amsterdam"}));
    auto pathPartition2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition2, "city"), std::vector<std::string>({"Dublin", "Copenhagen"}));
    auto pathPartition3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition3, "city"), std::vector<std::string>({"Tallinn", "Berlin"}));
}
