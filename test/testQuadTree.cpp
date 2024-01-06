#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/QuadTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "include/storage/TableGenerator.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTree){
    auto folder = ExperimentsConfig::quadTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto partitionSize = 2;
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> quadTreePartitioning = std::make_shared<partitioning::QuadTreePartitioning>();
    auto dataReader = storage::DataReader();
    ASSERT_EQ(dataReader.load(dataset), arrow::Status::OK());
    arrow::Status statusQuadTree = quadTreePartitioning->partition(dataReader, partitioningColumns, partitionSize, folder);
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