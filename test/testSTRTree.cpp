#include <arrow/io/api.h>
#include <filesystem>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTreeSchool){
    auto folder = ExperimentsConfig::strTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / folder;
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitionSize = 8;
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader,
                                                                  partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({16, 45, 21, 7, 74, 34, 111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({30, 21, 18, 27, 41, 37, 23, 22}));
    cleanUpFolder(folder);
    partitionSize = 2;
    auto partitioning2 = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader,
                                                                  partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning2->partition(), arrow::Status::OK());
    pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Student_id"), std::vector<int32_t>({45, 21}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition0, "Age"), std::vector<int32_t>({21, 18}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Student_id"), std::vector<int32_t>({111, 91}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "Age"), std::vector<int32_t>({23, 22}));
    auto pathPartition2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Student_id"), std::vector<int32_t>({16, 7}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "Age"), std::vector<int32_t>({30, 27}));
    auto pathPartition3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Student_id"), std::vector<int32_t>({74, 34}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "Age"), std::vector<int32_t>({41, 37}));
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningSTRTreeCities) {
    auto folder = ExperimentsConfig::strTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetCities);
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / folder;
    std::vector<std::string> partitioningColumns = {"x", "y"};
    auto partitionSize = 2;
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader,
                                                                  partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    auto pathPartition0 = folder / ("0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition0, "city"), std::vector<std::string>({"Oslo", "Moscow"}));
    auto pathPartition1 = folder / ("1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition1, "city"), std::vector<std::string>({"Dublin", "Copenhagen"}));
    auto pathPartition2 = folder / ("2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition2, "city"), std::vector<std::string>({"Amsterdam", "Madrid"}));
    auto pathPartition3 = folder / ("3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::StringArray>(pathPartition3, "city"), std::vector<std::string>({"Tallinn", "Berlin"}));
}