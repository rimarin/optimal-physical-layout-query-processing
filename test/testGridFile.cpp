#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/GridFilePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningGridFile){
    cleanUpFolder(gridFileFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> gridFilePartitioning = std::make_shared<partitioning::GridFilePartitioning>();
    auto partitions = gridFilePartitioning->partition(*table, partitioningColumns, 2).ValueOrDie();
    arrow::Status statusGridFile = storage::DataWriter::WritePartitions(partitions, datasetSchoolName, gridFileFolder);
    auto pathPartition0 = gridFileFolder / (datasetSchoolName + "0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({16, 7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({30, 27}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition0, "partition_id"), std::vector<int64_t>({0, 0}));
    auto pathPartition1 = gridFileFolder / (datasetSchoolName + "1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Student_id"), std::vector<int32_t>({21, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Age"), std::vector<int32_t>({18, 37}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition1, "partition_id"), std::vector<int64_t>({1, 1}));
    auto pathPartition2 = gridFileFolder / (datasetSchoolName + "2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Student_id"), std::vector<int32_t>({45, 74}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Age"), std::vector<int32_t>({21, 41}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2, "partition_id"), std::vector<int64_t>({2, 2}));
    auto pathPartition3 = gridFileFolder / (datasetSchoolName + "3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Student_id"), std::vector<int32_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Age"), std::vector<int32_t>({23, 22}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition3, "partition_id"), std::vector<int64_t>({3, 3}));
}