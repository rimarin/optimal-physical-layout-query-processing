#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "include/partitioning/FixedGridPartitioning.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridTPCH){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = ExperimentsConfig::datasetTPCH1;
    auto partitionSize = 20000;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = getDataset(dataset);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>();
    arrow::Status statusTPCH1 = fixedGridPartitioning->partition(*table, partitioningColumns, partitionSize, folder);
    auto dirIter = std::filesystem::directory_iterator(folder);
    int fileCount = std::count_if(
            begin(dirIter),
            end(dirIter),
            [](auto& entry) { return entry.is_regular_file(); }
    );
    ASSERT_EQ(fileCount, 2400);
}

