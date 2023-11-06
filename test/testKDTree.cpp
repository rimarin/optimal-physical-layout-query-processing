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
    arrow::Status statusKDTree = storage::DataWriter::WriteTable(*table, datasetSchoolName, kdTreeFolder,kdTreePartitioning, partitionSizeTest);
    auto pathPartition0 = kdTreeFolder / (datasetSchoolName + "0.parquet");
    ASSERT_EQ(readColumn<int32_t>(pathPartition0, "Student_id"), std::vector<int32_t>({45, 21}));
    ASSERT_EQ(readColumn<int32_t>(pathPartition0, "Age"), std::vector<int32_t>({21, 18}));
    ASSERT_EQ(readColumn<int32_t>(pathPartition0, "partition_id"), std::vector<int32_t>({0, 0}));
}
