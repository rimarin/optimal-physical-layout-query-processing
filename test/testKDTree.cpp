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
    arrow::Result<std::shared_ptr<arrow::Table>> kdTreeExampleTable = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / kdTreeFolder;
    std::vector<std::string> KDTreePartitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> kdTreePartitioning = std::make_shared<partitioning::KDTreePartitioning>(
            KDTreePartitioningColumns);
    arrow::Status statusKDTree = storage::DataWriter::WriteTable(*kdTreeExampleTable, example2TableName, outputFolder,
                                                                 kdTreePartitioning);
    int numPartitions = 4;
    for (int i = 0; i < numPartitions; ++i) {
        auto expectedPath = std::filesystem::current_path() / kdTreeFolder / (example2TableName + std::to_string(i) + ".parquet");
        ASSERT_EQ(std::filesystem::exists(expectedPath), true);
    }
}
