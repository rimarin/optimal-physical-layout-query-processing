#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/DataWriter.h"
#include "storage/DataReader.h"
#include "storage/TableGenerator.h"


TEST_F(TestOptimalLayoutFixture, TestPartitioningKDTree) {
    auto folder = ExperimentsConfig::kdTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto partitionSize = 2;
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::KD_TREE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({45, 21}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({21, 18}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Student_id"), std::vector<int32_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Age"), std::vector<int32_t>({23, 22}));
    auto pathPartition2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Student_id"), std::vector<int32_t>({16, 7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Age"), std::vector<int32_t>({30, 27}));
    auto pathPartition3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Student_id"), std::vector<int32_t>({74, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Age"), std::vector<int32_t>({41, 37}));
}
