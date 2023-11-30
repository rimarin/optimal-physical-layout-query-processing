#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/QuadTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTree){
    auto folder = ExperimentsConfig::quadTreeFolder;
    auto dataset = ExperimentsConfig::datasetSchool;
    auto partitionSize = 20;
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> quadTreePartitioning = std::make_shared<partitioning::QuadTreePartitioning>();
    auto partitions = quadTreePartitioning->partition(*table, partitioningColumns, partitionSize).ValueOrDie();
    arrow::Status statusQuadTree = storage::DataWriter::WritePartitions(partitions, dataset, folder);
    auto pathPartition0 = folder / (dataset + "0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({23, 22}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition0, "partition_id"), std::vector<int64_t>({0, 0}));
    auto pathPartition1 = folder / (dataset + "1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Student_id"), std::vector<int32_t>({74}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Age"), std::vector<int32_t>({41}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition1, "partition_id"), std::vector<int64_t>({1}));
    auto pathPartition2 = folder / (dataset + "2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Student_id"), std::vector<int32_t>({45}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Age"), std::vector<int32_t>({21}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2, "partition_id"), std::vector<int64_t>({2}));
    auto pathPartition3 = folder / (dataset + "3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Student_id"), std::vector<int32_t>({21}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Age"), std::vector<int32_t>({18}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition3, "partition_id"), std::vector<int64_t>({3}));
    auto pathPartition4 = folder / (dataset + "4" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition4, "Student_id"), std::vector<int32_t>({7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition4, "Age"), std::vector<int32_t>({27}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition4, "partition_id"), std::vector<int64_t>({4}));
    auto pathPartition5 = folder / (dataset + "5" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition5, "Student_id"), std::vector<int32_t>({16, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition5, "Age"), std::vector<int32_t>({30, 37}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition5, "partition_id"), std::vector<int64_t>({5, 5}));
}