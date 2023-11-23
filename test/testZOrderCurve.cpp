#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/ZOrderCurvePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningZOrderCurve){
    auto zOrderCurve = common::ZOrderCurve();
    uint64_t arr1[2] = {18, 21};
    uint64_t arr2[2] = {21, 45};
    uint64_t arr3[2] = {22, 91};
    ASSERT_EQ(806, zOrderCurve.encode(arr1, 2));
    ASSERT_EQ(std::vector<uint64_t>(std::begin(arr1), std::end(arr1)), zOrderCurve.decode(806, 2));
    ASSERT_EQ(2483, zOrderCurve.encode(arr2, 2));
    ASSERT_EQ(std::vector<uint64_t>(std::begin(arr2), std::end(arr2)), zOrderCurve.decode(2483, 2));
    ASSERT_EQ(9118, zOrderCurve.encode(arr3, 2));
    ASSERT_EQ(std::vector<uint64_t>(std::begin(arr3), std::end(arr3)), zOrderCurve.decode(9118, 2));
    cleanUpFolder(zOrderCurveFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> zOrderCurvePartitioning = std::make_shared<partitioning::ZOrderCurvePartitioning>();
    auto partitions = zOrderCurvePartitioning->partition(*table, partitioningColumns, 2).ValueOrDie();
    arrow::Status statusHilbertCurve = storage::DataWriter::WritePartitions(partitions, datasetSchoolName, zOrderCurveFolder);
    auto pathPartition0 = zOrderCurveFolder / (datasetSchoolName + "0.parquet");
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({21, 7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({18, 27}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition0, "partition_id"), std::vector<int64_t>({0, 0}));
    auto pathPartition1 = zOrderCurveFolder / (datasetSchoolName + "1.parquet");
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Student_id"), std::vector<int32_t>({16, 45}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Age"), std::vector<int32_t>({30, 21}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition1, "partition_id"), std::vector<int64_t>({1, 1}));
    auto pathPartition2 = zOrderCurveFolder / (datasetSchoolName + "2.parquet");
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Student_id"), std::vector<int32_t>({34, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Age"), std::vector<int32_t>({37, 22}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition2, "partition_id"), std::vector<int64_t>({2, 2}));
    auto pathPartition3 = zOrderCurveFolder / (datasetSchoolName + "3.parquet");
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Student_id"), std::vector<int32_t>({74, 111}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Age"), std::vector<int32_t>({41, 23}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition3, "partition_id"), std::vector<int64_t>({3, 3}));
}