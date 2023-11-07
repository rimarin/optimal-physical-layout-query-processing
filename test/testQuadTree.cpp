#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/QuadTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTree){
    cleanUpFolder(quadTreeFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> quadTreePartitioning = std::make_shared<partitioning::QuadTreePartitioning>(partitioningColumns);
    arrow::Status statusQuadTree = storage::DataWriter::WriteTable(*table, datasetSchoolName, quadTreeFolder, quadTreePartitioning, partitionSizeTest);
    auto pathPartition0 = quadTreeFolder / (datasetSchoolName + "0.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition0, "Student_id"), std::vector<double_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition0, "Age"), std::vector<double_t>({23, 22}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition0, "partition_id"), std::vector<int64_t>({0, 0}));
    auto pathPartition1 = quadTreeFolder / (datasetSchoolName + "1.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition1, "Student_id"), std::vector<double_t>({74}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition1, "Age"), std::vector<double_t>({41}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition1, "partition_id"), std::vector<int64_t>({1}));
    auto pathPartition2 = quadTreeFolder / (datasetSchoolName + "2.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition2, "Student_id"), std::vector<double_t>({45}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition2, "Age"), std::vector<double_t>({21}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2, "partition_id"), std::vector<int64_t>({2}));
    auto pathPartition3 = quadTreeFolder / (datasetSchoolName + "3.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition3, "Student_id"), std::vector<double_t>({21}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition3, "Age"), std::vector<double_t>({18}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition3, "partition_id"), std::vector<int64_t>({3}));
    auto pathPartition4 = quadTreeFolder / (datasetSchoolName + "4.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition4, "Student_id"), std::vector<double_t>({7}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition4, "Age"), std::vector<double_t>({27}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition4, "partition_id"), std::vector<int64_t>({4}));
    auto pathPartition5 = quadTreeFolder / (datasetSchoolName + "5.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition5, "Student_id"), std::vector<double_t>({16, 34}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition5, "Age"), std::vector<double_t>({30, 37}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition5, "partition_id"), std::vector<int64_t>({5, 5}));

}