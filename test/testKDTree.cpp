#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/KDTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"


TEST_F(TestOptimalLayoutFixture, TestPartitioningKDTree) {
    cleanUpFolder(kdTreeFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> kdTreePartitioning = std::make_shared<partitioning::KDTreePartitioning>(
            partitioningColumns);
    auto partitions = kdTreePartitioning->partition(*table, partitioningColumns, partitionSizeTest).ValueOrDie();
    arrow::Status statusKDTree = storage::DataWriter::WritePartitions(partitions, datasetSchoolName, kdTreeFolder);
    auto pathPartition0 = kdTreeFolder / (datasetSchoolName + "0.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition0, "Student_id"), std::vector<double_t>({45, 21}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition0, "Age"), std::vector<double_t>({21, 18}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition0, "partition_id"), std::vector<int64_t>({0, 0}));
    auto pathPartition1 = kdTreeFolder / (datasetSchoolName + "1.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition1, "Student_id"), std::vector<double_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition1, "Age"), std::vector<double_t>({23, 22}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition1, "partition_id"), std::vector<int64_t>({1, 1}));
    auto pathPartition2 = kdTreeFolder / (datasetSchoolName + "2.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition2, "Student_id"), std::vector<double_t>({16, 7}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition2, "Age"), std::vector<double_t>({30, 27}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2, "partition_id"), std::vector<int64_t>({2, 2}));
    auto pathPartition3 = kdTreeFolder / (datasetSchoolName + "3.parquet");
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition3, "Student_id"), std::vector<double_t>({74, 34}));
    ASSERT_EQ(readColumn<arrow::DoubleArray>(pathPartition3, "Age"), std::vector<double_t>({41, 37}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition3, "partition_id"), std::vector<int64_t>({3, 3}));
}
