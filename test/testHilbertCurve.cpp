#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/HilbertCurvePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "include/storage/TableGenerator.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningHilbertCurve){
    auto folder = ExperimentsConfig::hilbertCurveFolder;
    auto dataset = ExperimentsConfig::datasetSchool;
    auto partitionSize = 5;
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto hilbertCurve = common::HilbertCurve();
    int64_t X1[3] = {5, 10, 20};
    int64_t X2[3] = {1, 1, 1};
    int64_t X3[2] = {1, 1};
    int64_t X4[2] = {0, 0};
    int64_t X5[3] = {1, 1, 0};
    hilbertCurve.axesToTranspose(X1, 5, 3);
    hilbertCurve.axesToTranspose(X2, 5, 3);
    hilbertCurve.axesToTranspose(X3, 5, 2);
    hilbertCurve.axesToTranspose(X4, 5, 2);
    hilbertCurve.axesToTranspose(X5, 2, 3);
    ASSERT_EQ(7865, hilbertCurve.interleaveBits(X1, 5, 3));
    ASSERT_EQ(5, hilbertCurve.interleaveBits(X2, 5, 3));
    ASSERT_EQ(2, hilbertCurve.interleaveBits(X3, 5, 2));
    ASSERT_EQ(0, hilbertCurve.interleaveBits(X4, 5, 2));
    ASSERT_EQ(2, hilbertCurve.interleaveBits(X5, 5, 3));
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> hilbertCurvePartitioning = std::make_shared<partitioning::HilbertCurvePartitioning>();
    arrow::Status statusHilbertCurve = hilbertCurvePartitioning->partition(*table, partitioningColumns, partitionSize, folder);
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({16, 45, 21, 7, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({30, 21, 18, 27, 37}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition0, "partition_id"), std::vector<int64_t>({0, 0, 0, 0, 0}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Student_id"), std::vector<int32_t>({74, 111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Age"), std::vector<int32_t>({41, 23, 22}));
    ASSERT_EQ(readColumn<arrow::Int64Array>(pathPartition1, "partition_id"), std::vector<int64_t>({1, 1, 1}));
}