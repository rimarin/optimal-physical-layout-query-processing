#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/STRTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTree){
    cleanUpFolder(strTreeFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / strTreeFolder;
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> strTreePartitioning = std::make_shared<partitioning::STRTreePartitioning>(partitioningColumns, 5);
    arrow::Status statusGridFile = storage::DataWriter::WriteTable(*table, datasetSchoolName, outputFolder, strTreePartitioning, partitionSize);
    /*
    int numPartitions = 5;
    for (int i = 0; i < numPartitions; ++i) {
        auto expectedPath = std::filesystem::current_path() / gridFileFolder / (datasetSchoolName + std::to_string(i) + ".parquet");
        ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / gridFileFolder / (datasetSchoolName + std::to_string(i) + ".parquet")), true);
    }
     */
}