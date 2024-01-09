#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/DataReader.h"
#include "storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTree){
    auto folder = ExperimentsConfig::strTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / folder;
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader, partitioningColumns, 8, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition1_0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1_0, "Student_id"), std::vector<int32_t>({16, 45, 21, 7, 74, 34, 111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1_0, "Age"), std::vector<int32_t>({30, 21, 18, 27, 41, 37, 23, 22}));
    cleanUpFolder(folder);
    partitioning->setPartitionSize(2);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition2_0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_0, "Student_id"), std::vector<int32_t>({45, 21}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_0, "Age"), std::vector<int32_t>({21, 18}));
    auto pathPartition2_1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_1, "Student_id"), std::vector<int32_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_1, "Age"), std::vector<int32_t>({23, 22}));
    auto pathPartition2_2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_2, "Student_id"), std::vector<int32_t>({16, 7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_2, "Age"), std::vector<int32_t>({30, 27}));
    auto pathPartition2_3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_3, "Student_id"), std::vector<int32_t>({74, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2_3, "Age"), std::vector<int32_t>({41, 37}));
}