#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "include/partitioning/FixedGridPartitioning.h"
#include "include/partitioning/KDTreePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridTPCH){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = ExperimentsConfig::datasetTPCH1;
    auto partitionSize = 20000;
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = getDataset(dataset);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    /*
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>();
    auto partitions1 = fixedGridPartitioning->partition(*table, partitioningColumns, partitionSizeReal).ValueOrDie();
    arrow::Status statusTPCH1 = storage::DataWriter::WritePartitions(partitions1, dataset, folder);
    auto pathPartitionFirst = folder / (dataset + "0" + fileExtension);
    ASSERT_EQ(std::filesystem::exists(pathPartitionFirst), true);
    auto pathPartitionLast = folder / (dataset + "2399" + fileExtension);
    ASSERT_EQ(std::filesystem::exists(pathPartitionLast), true);
    cleanUpFolder(folder);
    */
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> kdTreePartitioning = std::make_shared<partitioning::KDTreePartitioning>();
    auto partitions2 = kdTreePartitioning->partition(*table, partitioningColumns, partitionSize).ValueOrDie();
    arrow::Status statusTPCH2 = storage::DataWriter::WritePartitions(partitions2, dataset, folder);
}

