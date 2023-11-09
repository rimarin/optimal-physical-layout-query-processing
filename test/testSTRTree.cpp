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
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> strTreePartitioning = std::make_shared<partitioning::STRTreePartitioning>();
    auto partitions = strTreePartitioning->partition(*table, partitioningColumns, partitionSizeTest).ValueOrDie();
    arrow::Status statusQuadTree = storage::DataWriter::WritePartitions(partitions, datasetSchoolName, strTreeFolder);
}