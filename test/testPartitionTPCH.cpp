#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridTPCH){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTPCH1);
    auto partitionSize = 20000;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto totalNumRows = 239917;
    ASSERT_EQ(dataReader->getNumRows(), totalNumRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / "0.parquet";
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    std::filesystem::path partition1 = folder / "13.parquet";
    ASSERT_EQ(dataReader->load(partition1), arrow::Status::OK());
    auto dirIter = std::filesystem::directory_iterator(folder);
    int fileCount = std::count_if(
            begin(dirIter),
            end(dirIter),
            [](auto& entry) { return entry.is_regular_file(); }
    );
    ASSERT_EQ(fileCount, 14);
}

