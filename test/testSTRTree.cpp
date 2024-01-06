#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/STRTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "include/storage/TableGenerator.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTree){
    auto folder = ExperimentsConfig::strTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / folder;
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> strTreePartitioning = std::make_shared<partitioning::STRTreePartitioning>();
    auto dataReader = storage::DataReader();
    ASSERT_EQ(dataReader.load(dataset), arrow::Status::OK());
    arrow::Status statusSTRTree1 = strTreePartitioning->partition(dataReader, partitioningColumns, 8, folder);
    auto pathPartition1_0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1_0, "Student_id"), std::vector<int32_t>({16, 45, 21, 7, 74, 34, 111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1_0, "Age"), std::vector<int32_t>({30, 21, 18, 27, 41, 37, 23, 22}));
    // ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition1_0, "partition_id"), std::vector<uint32_t>({0, 0, 0, 0, 0, 0, 0, 0}));
    cleanUpFolder(folder);
    arrow::Status statusSTRTree2 = strTreePartitioning->partition(dataReader, partitioningColumns, 2, folder);
    auto pathPartition2_0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_0, "Student_id"), std::vector<int32_t>({45, 21}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_0, "Age"), std::vector<int32_t>({21, 18}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition2_0, "partition_id"), std::vector<uint32_t>({0, 0}));
    auto pathPartition2_1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_1, "Student_id"), std::vector<int32_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_1, "Age"), std::vector<int32_t>({23, 22}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition2_1, "partition_id"), std::vector<uint32_t>({1, 1}));
    auto pathPartition2_2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_2, "Student_id"), std::vector<int32_t>({16, 7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_2, "Age"), std::vector<int32_t>({30, 27}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition2_2, "partition_id"), std::vector<uint32_t>({2, 2}));
    auto pathPartition2_3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_3, "Student_id"), std::vector<int32_t>({74, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_3, "Age"), std::vector<int32_t>({41, 37}));
    ASSERT_EQ(readColumn<arrow::UInt32Array>(pathPartition2_3, "partition_id"), std::vector<uint32_t>({3, 3}));
}