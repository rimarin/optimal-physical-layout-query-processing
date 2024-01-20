#include <arrow/io/api.h>
#include <filesystem>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGridTPCH){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTPCH1);
    auto partitionSize = 20000;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), 239917);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / ("0" + ExperimentsConfig::fileExtension);
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    std::filesystem::path partition1 = folder / ("13" + ExperimentsConfig::fileExtension);
    ASSERT_EQ(dataReader->load(partition1), arrow::Status::OK());
    auto dirIter = std::filesystem::directory_iterator(folder);
    int fileCount = std::count_if(
            begin(dirIter),
            end(dirIter),
            [](auto& entry) { return entry.is_regular_file(); }
    );
    ASSERT_EQ(fileCount, 14);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTreeTPCH){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::strTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTPCH1);
    auto partitionSize = 20000;
    auto numTotalRows = 239917;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / "0.parquet";
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    std::filesystem::path partition1 = folder / "13.parquet";
    ASSERT_EQ(dataReader->load(partition1), arrow::Status::OK());
    uint32_t fileCount = 0;
    uint32_t partitionsTotalRows = 0;
    for (auto &fileSystemItem : std::filesystem::directory_iterator(folder)) {
        if (fileSystemItem.is_regular_file() &&
            fileSystemItem.path().extension() == common::Settings::fileExtension) {
            fileCount += 1;
            auto partitionPath = fileSystemItem.path();
            std::ignore = dataReader->load(partitionPath);
            partitionsTotalRows += dataReader->getNumRows();
        }
    }
    ASSERT_EQ(fileCount, 28);
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningHilbertCurveTPCH){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::hilbertCurveFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTPCH1);
    auto partitionSize = 20000;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), 239917);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::HILBERT_CURVE, dataReader, partitioningColumns, partitionSize, folder);
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

