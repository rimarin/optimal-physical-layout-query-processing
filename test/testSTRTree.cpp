#include <arrow/io/api.h>
#include <filesystem>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTreeSchool){
    auto folder = ExperimentsConfig::strTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / folder;
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitionSize = 8;
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader,
                                                                  partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({16, 45, 21, 7, 74, 34, 111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({30, 21, 18, 27, 41, 37, 23, 22}));
    cleanUpFolder(folder);
    partitionSize = 2;
    auto partitioning2 = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader,
                                                                  partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning2->partition(), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Student_id", std::vector<int32_t>({21, 45})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Age", std::vector<int32_t>({18, 21})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Student_id", std::vector<int32_t>({91, 111})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Age", std::vector<int32_t>({22, 23})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("2" + fileExtension), "Student_id", std::vector<int32_t>({7, 16})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("2" + fileExtension), "Age", std::vector<int32_t>({27, 30})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("3" + fileExtension), "Student_id", std::vector<int32_t>({34, 74})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("3" + fileExtension), "Age", std::vector<int32_t>({37, 41})), arrow::Status::OK());
    ASSERT_EQ(std::filesystem::exists(folder / ("4" + fileExtension)), false);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTreeCities) {
    auto folder = ExperimentsConfig::strTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetCities);
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / folder;
    std::vector<std::string> partitioningColumns = {"x", "y"};
    auto partitionSize = 2;
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader,
                                                                  partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("0" + fileExtension), "city", std::vector<std::string>({"Moscow", "Oslo"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("1" + fileExtension), "city", std::vector<std::string>({"Copenhagen", "Dublin"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("2" + fileExtension), "city", std::vector<std::string>({"Madrid", "Amsterdam"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("3" + fileExtension), "city", std::vector<std::string>({"Berlin", "Tallinn"})), arrow::Status::OK());
    ASSERT_EQ(std::filesystem::exists(folder / ("4" + fileExtension)), false);
}

