#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/STRTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTree){
    cleanUpFolder(strTreeFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / strTreeFolder;
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> strTreePartitioning = std::make_shared<partitioning::STRTreePartitioning>();
    auto partitions1 = strTreePartitioning->partition(*table, partitioningColumns, 8).ValueOrDie();
    arrow::Status statusSTRTree1 = storage::DataWriter::WritePartitions(partitions1, datasetSchoolName, strTreeFolder);
    auto pathPartition1_0 = strTreeFolder / (datasetSchoolName + "0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1_0, "Student_id"), std::vector<int32_t>({16, 45, 21, 7, 74, 34, 111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1_0, "Age"), std::vector<int32_t>({30, 21, 18, 27, 41, 37, 23, 22}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition1_0, "partition_id"), std::vector<int64_t>({0, 0, 0, 0, 0, 0, 0, 0}));
    cleanUpFolder(strTreeFolder);
    auto partitions2 = strTreePartitioning->partition(*table, partitioningColumns, 2).ValueOrDie();
    arrow::Status statusSTRTree2 = storage::DataWriter::WritePartitions(partitions2, datasetSchoolName, strTreeFolder);
    auto pathPartition2_0 = strTreeFolder / (datasetSchoolName + "0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_0, "Student_id"), std::vector<int32_t>({45, 21}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_0, "Age"), std::vector<int32_t>({21, 18}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2_0, "partition_id"), std::vector<int64_t>({0, 0}));
    auto pathPartition2_1 = strTreeFolder / (datasetSchoolName + "1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_1, "Student_id"), std::vector<int32_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_1, "Age"), std::vector<int32_t>({23, 22}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2_1, "partition_id"), std::vector<int64_t>({1, 1}));
    auto pathPartition2_2 = strTreeFolder / (datasetSchoolName + "2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_2, "Student_id"), std::vector<int32_t>({16, 7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_2, "Age"), std::vector<int32_t>({30, 27}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2_2, "partition_id"), std::vector<int64_t>({2, 2}));
    auto pathPartition2_3 = strTreeFolder / (datasetSchoolName + "3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_3, "Student_id"), std::vector<int32_t>({74, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_3, "Age"), std::vector<int32_t>({41, 37}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2_3, "partition_id"), std::vector<int64_t>({3, 3}));
}