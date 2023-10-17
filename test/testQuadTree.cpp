#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/QuadTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTree){
    cleanUpFolder(quadTreeFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> quadTreeExampleTable = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / quadTreeFolder;
    std::vector<std::string> quadTreePartitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> quadTreePartitioning = std::make_shared<partitioning::QuadTreePartitioning>(quadTreePartitioningColumns);
    arrow::Status statusQuadTree = storage::DataWriter::WriteTable(*quadTreeExampleTable, example2TableName, outputFolder,quadTreePartitioning);
    int numPartitions = 5;
    for (int i = 0; i < numPartitions; ++i) {
        auto expectedPath = std::filesystem::current_path() / quadTreeFolder / (example2TableName + std::to_string(i) + ".parquet");
        ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / quadTreeFolder / (example2TableName + std::to_string(i) + ".parquet")), true);
    }
}