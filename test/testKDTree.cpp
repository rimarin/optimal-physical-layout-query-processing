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
    arrow::Status statusKDTree = storage::DataWriter::WriteTable(*table, datasetSchoolName, kdTreeFolder,
                                                                 kdTreePartitioning, partitionSize);
    int numPartitions = 4;
    for (int i = 0; i < numPartitions; ++i) {
        auto expectedPath = kdTreeFolder / (datasetSchoolName + std::to_string(i) + ".parquet");
        ASSERT_EQ(std::filesystem::exists(expectedPath), true);
    }
}
