#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/GridFilePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "include/storage/TableGenerator.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningGridFile){
    auto folder = ExperimentsConfig::gridFileFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto partitionSize = 5;
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> gridFilePartitioning = std::make_shared<partitioning::GridFilePartitioning>();
    auto dataReader = storage::DataReader();
    ASSERT_EQ(dataReader.load(dataset), arrow::Status::OK());
    ASSERT_EQ(gridFilePartitioning->partition(dataReader, partitioningColumns, partitionSize, folder), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({45, 21, 7, 111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({21, 18, 27, 23, 22}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition0, "partition_id"), std::vector<uint32_t>({0, 0, 0, 0, 0}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Student_id"), std::vector<int32_t>({16, 74, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Age"), std::vector<int32_t>({30, 41, 37}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition1, "partition_id"), std::vector<uint32_t>({1, 1, 1}));
}