#include <arrow/io/api.h>
#include <filesystem>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTreeSchool){
    auto folder = ExperimentsConfig::quadTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto partitionSize = 2;
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::QUAD_TREE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({23, 22}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition0, "partition_id"), std::vector<uint32_t>({0, 0}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Student_id"), std::vector<int32_t>({74}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Age"), std::vector<int32_t>({41}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition1, "partition_id"), std::vector<uint32_t>({1}));
    auto pathPartition2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Student_id"), std::vector<int32_t>({45}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Age"), std::vector<int32_t>({21}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition2, "partition_id"), std::vector<uint32_t>({2}));
    auto pathPartition3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Student_id"), std::vector<int32_t>({21}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Age"), std::vector<int32_t>({18}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition3, "partition_id"), std::vector<uint32_t>({3}));
    auto pathPartition4 = folder / ("4" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition4, "Student_id"), std::vector<int32_t>({7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition4, "Age"), std::vector<int32_t>({27}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition4, "partition_id"), std::vector<uint32_t>({4}));
    auto pathPartition5 = folder / ("5" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition5, "Student_id"), std::vector<int32_t>({16, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition5, "Age"), std::vector<int32_t>({30, 37}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition5, "partition_id"), std::vector<int32_t>({5, 5}));
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTreeCities){
    auto folder = ExperimentsConfig::quadTreeFolder;
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetCities);
    cleanUpFolder(folder);
    auto dataReader = std::make_shared<storage::DataReader>();
    std::vector<std::string> partitioningColumns = {"x", "y"};
    auto partitionSize = 2;
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::QUAD_TREE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition0, "city"), std::vector<std::string>({"Dublin", "Copenhagen"}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition1, "city"), std::vector<std::string>({"Tallinn", "Berlin"}));
    auto pathPartition2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition2, "city"), std::vector<std::string>({"Oslo"}));
    auto pathPartition3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition3, "city"), std::vector<std::string>({"Moscow"}));
    auto pathPartition4 = folder / ("4" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition4, "city"), std::vector<std::string>({"Amsterdam"}));
    auto pathPartition5 = folder / ("5" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition5, "city"), std::vector<std::string>({"Madrid"}));
}