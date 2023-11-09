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
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>();
    auto partitions = fixedGridPartitioning->partition(*table, partitioningColumns, partitionSizeReal).ValueOrDie();
    arrow::Status statusTPCH = storage::DataWriter::WritePartitions(partitions, datasetTPCHName, fixedGridFolder);
    auto pathPartitionFirst = fixedGridFolder / (datasetTPCHName + "0" + fileExtension);
    ASSERT_EQ(std::filesystem::exists(pathPartitionFirst), true);
    auto pathPartitionLast = fixedGridFolder / (datasetTPCHName + "2399" + fileExtension);
    ASSERT_EQ(std::filesystem::exists(pathPartitionLast), true);
}

