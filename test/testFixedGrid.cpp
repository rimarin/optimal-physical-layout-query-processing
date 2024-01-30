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
    ASSERT_EQ(std::filesystem::exists(folder / ("3" + fileExtension)), false);
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
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("0" + fileExtension), "city", std::vector<std::string>({"Madrid", "Moscow"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("1" + fileExtension), "city", std::vector<std::string>({"Amsterdam", "Oslo"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("2" + fileExtension), "city", std::vector<std::string>({"Dublin", "Copenhagen"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("3" + fileExtension), "city", std::vector<std::string>({"Berlin", "Tallinn"})), arrow::Status::OK());
    ASSERT_EQ(std::filesystem::exists(folder / ("4" + fileExtension)), false);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridTPCH){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTPCH1);
    auto partitionSize = 20000;
    auto numTotalRows = 239917;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / ("0" + ExperimentsConfig::fileExtension);
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    auto folderResults = getFolderResults(dataReader, folder);
    auto fileCount = folderResults.first;
    auto partitionsTotalRows = folderResults.second;
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
    ASSERT_EQ(fileCount, 14);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridTaxi){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTaxi);
    auto partitionSize = 50000;
    auto numTotalRows = 8760687;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"PULocationID", "DOLocationID"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / ("0" + ExperimentsConfig::fileExtension);
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    auto folderResults = getFolderResults(dataReader, folder);
    auto fileCount = folderResults.first;
    auto partitionsTotalRows = folderResults.second;
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
    ASSERT_EQ(fileCount, 177);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridOSM){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetOSM);
    auto partitionSize = 50000;
    auto numTotalRows = 7120245;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"min_lon", "max_lon"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / ("0" + ExperimentsConfig::fileExtension);
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    auto folderResults = getFolderResults(dataReader, folder);
    auto fileCount = folderResults.first;
    auto partitionsTotalRows = folderResults.second;
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
    ASSERT_EQ(fileCount, 144);
}