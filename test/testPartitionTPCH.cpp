#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "include/partitioning/FixedGridPartitioning.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridTPCH){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = ExperimentsConfig::datasetTPCH1;
    auto partitioningTechnique = ExperimentsConfig::noPartition;
    auto partitionSize = 20000;
    cleanUpFolder(folder);
    auto dataReader = storage::DataReader();
    auto datasetPath = storage::DataReader::getDatasetPath(folder, dataset, partitioningTechnique);
    ASSERT_EQ(dataReader.load(datasetPath), arrow::Status::OK());
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    std::shared_ptr<partitioning::FixedGridPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>();
    arrow::Status statusTPCH1 = fixedGridPartitioning->partitionNew(dataReader, partitioningColumns, partitionSize, folder);
    auto dirIter = std::filesystem::directory_iterator(folder);
    int fileCount = std::count_if(
            begin(dirIter),
            end(dirIter),
            [](auto& entry) { return entry.is_regular_file(); }
    );
    ASSERT_EQ(fileCount, 2400);
}

