#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/FixedGridPartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridTPCH){
    cleanUpFolder(fixedGridFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = getDataset(datasetTPCHName);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>(partitioningColumns);
    arrow::Status statusFixedGrid = storage::DataWriter::WriteTable(*table, datasetTPCHName, fixedGridFolder, fixedGridPartitioning, partitionSizeReal);
    auto expectedPath = fixedGridFolder / (datasetTPCHName + "0.parquet");
    ASSERT_EQ(std::filesystem::exists(expectedPath), true);
}

