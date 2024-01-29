#include <arrow/io/api.h>
#include <filesystem>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTreeSchool){
    auto folder = ExperimentsConfig::quadTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto partitionSize = 2;
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::QUAD_TREE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Student_id", std::vector<int32_t>({45})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Age", std::vector<int32_t>({21})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Student_id", std::vector<int32_t>({21})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Age", std::vector<int32_t>({18})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("2" + fileExtension), "Student_id", std::vector<int32_t>({7})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("2" + fileExtension), "Age", std::vector<int32_t>({27})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("3" + fileExtension), "Student_id", std::vector<int32_t>({111, 91})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("3" + fileExtension), "Age", std::vector<int32_t>({23, 22})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("4" + fileExtension), "Student_id", std::vector<int32_t>({74})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("4" + fileExtension), "Age", std::vector<int32_t>({41})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("5" + fileExtension), "Student_id", std::vector<int32_t>({16, 34})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("5" + fileExtension), "Age", std::vector<int32_t>({30, 37})), arrow::Status::OK());
    ASSERT_EQ(std::filesystem::exists(folder / ("6" + fileExtension)), false);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTreeCities){
    auto folder = ExperimentsConfig::quadTreeFolder;
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetCities);
    cleanUpFolder(folder);
    auto dataReader = std::make_shared<storage::DataReader>();
    std::vector<std::string> partitioningColumns = {"x", "y"};
    auto partitionSize = 2;
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::QUAD_TREE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("0" + fileExtension), "city", std::vector<std::string>({"Moscow"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("1" + fileExtension), "city", std::vector<std::string>({"Amsterdam"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("2" + fileExtension), "city", std::vector<std::string>({"Madrid"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("3" + fileExtension), "city", std::vector<std::string>({"Dublin", "Copenhagen"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("4" + fileExtension), "city", std::vector<std::string>({"Tallinn", "Berlin"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("5" + fileExtension), "city", std::vector<std::string>({"Oslo"})), arrow::Status::OK());
    ASSERT_EQ(std::filesystem::exists(folder / ("6" + fileExtension)), false);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTreeTPCH){
    // GTEST_SKIP();
    auto folder = ExperimentsConfig::quadTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTPCH1);
    auto partitionSize = 20000;
    auto numTotalRows = 239917;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::QUAD_TREE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / "0.parquet";
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    auto folderResults = getFolderResults(dataReader, folder);
    auto fileCount = folderResults.first;
    auto partitionsTotalRows = folderResults.second;
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
    ASSERT_EQ(fileCount, 16);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningQuadTreeTaxi){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::quadTreeFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTaxi);
    auto partitionSize = 20000;
    auto numTotalRows = 8760687;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"PULocationID", "DOLocationID"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::QUAD_TREE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / "0.parquet";
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    auto folderResults = getFolderResults(dataReader, folder);
    auto fileCount = folderResults.first;
    auto partitionsTotalRows = folderResults.second;
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
    ASSERT_EQ(fileCount, 1286);
}