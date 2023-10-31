#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/GridFilePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningGridFile){
    cleanUpFolder(gridFileFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> gridFilePartitioning = std::make_shared<partitioning::GridFilePartitioning>(partitioningColumns);
    arrow::Status statusGridFile = storage::DataWriter::WriteTable(*table, datasetSchoolName, gridFileFolder, gridFilePartitioning, partitionSizeTest);
    /*
    int numPartitions = 5;
    for (int i = 0; i < numPartitions; ++i) {
        auto expectedPath = std::filesystem::current_path() / gridFileFolder / (datasetSchoolName + std::to_string(i) + ".parquet");
        ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / gridFileFolder / (datasetSchoolName + std::to_string(i) + ".parquet")), true);
    }
    */
}