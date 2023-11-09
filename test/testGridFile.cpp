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
    auto partitions = gridFilePartitioning->partition(*table, partitioningColumns, partitionSizeTest).ValueOrDie();
    arrow::Status statusGridFile = storage::DataWriter::WritePartitions(partitions, datasetSchoolName, gridFileFolder);
}