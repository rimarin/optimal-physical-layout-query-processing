#include <arrow/io/api.h>
#include <filesystem>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/TableGenerator.h"


TEST_F(TestOptimalLayoutFixture, TestHilbertCurve) {
    auto hilbertCurve = structures::HilbertCurve();
    int64_t X1[3] = {5, 10, 20};
    int64_t X2[3] = {1, 1, 1};
    int64_t X3[2] = {1, 1};
    int64_t X4[2] = {0, 0};
    int64_t X5[3] = {1, 1, 0};
    hilbertCurve.axesToTranspose(X1, 5, 3);
    hilbertCurve.axesToTranspose(X2, 5, 3);
    hilbertCurve.axesToTranspose(X3, 5, 2);
    hilbertCurve.axesToTranspose(X4, 5, 2);
    hilbertCurve.axesToTranspose(X5, 2, 3);
    ASSERT_EQ(7865, hilbertCurve.interleaveBits(X1, 5, 3));
    ASSERT_EQ(5, hilbertCurve.interleaveBits(X2, 5, 3));
    ASSERT_EQ(2, hilbertCurve.interleaveBits(X3, 5, 2));
    ASSERT_EQ(0, hilbertCurve.interleaveBits(X4, 5, 2));
    ASSERT_EQ(2, hilbertCurve.interleaveBits(X5, 5, 3));
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningHilbertCurveSchool){
    auto folder = ExperimentsConfig::hilbertCurveFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetSchool);
    auto partitionSize = 5;
    auto fileExtension = ExperimentsConfig::fileExtension;
    cleanUpFolder(folder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::HILBERT_CURVE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Student_id", std::vector<int32_t>({7, 21, 16, 45, 34})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Age", std::vector<int32_t>({27, 18, 30, 21, 37})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Student_id", std::vector<int32_t>({74, 91, 111})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Age", std::vector<int32_t>({41, 22, 23})), arrow::Status::OK());
    ASSERT_EQ(std::filesystem::exists(folder / ("2" + fileExtension)), false);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningHilbertCurveCities){
    auto folder = ExperimentsConfig::hilbertCurveFolder;
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetCities);
    cleanUpFolder(folder);
    auto dataReader = std::make_shared<storage::DataReader>();
    std::vector<std::string> partitioningColumns = {"x", "y", "year"};
    auto partitionSize = 2;
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::HILBERT_CURVE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("0" + fileExtension), "city", std::vector<std::string>({"Dublin", "Moscow"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("1" + fileExtension), "city", std::vector<std::string>({"Oslo", "Copenhagen"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("2" + fileExtension), "city", std::vector<std::string>({"Amsterdam", "Madrid"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("3" + fileExtension), "city", std::vector<std::string>({"Berlin", "Tallinn"})), arrow::Status::OK());
    ASSERT_EQ(std::filesystem::exists(folder / ("4" + fileExtension)), false);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningHilbertCurveTPCH){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::hilbertCurveFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTPCH1);
    auto partitionSize = 20000;
    auto numTotalRows = 239917;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"c_custkey", "l_orderkey"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::HILBERT_CURVE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / "0.parquet";
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    auto folderResults = getFolderResults(dataReader, folder);
    auto fileCount = folderResults.first;
    auto partitionsTotalRows = folderResults.second;
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
    ASSERT_EQ(fileCount, 14);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningHilbertCurveTaxi){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::hilbertCurveFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetTaxi);
    auto partitionSize = 20000;
    auto numTotalRows = 8760687;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"PULocationID", "DOLocationID"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::HILBERT_CURVE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / ("0" + ExperimentsConfig::fileExtension);
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    auto folderResults = getFolderResults(dataReader, folder);
    auto fileCount = folderResults.first;
    auto partitionsTotalRows = folderResults.second;
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
    ASSERT_EQ(fileCount, 443);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningHilbertCurveOSM){
    GTEST_SKIP();
    auto folder = ExperimentsConfig::hilbertCurveFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetOSM);
    auto partitionSize = 50000;
    auto numTotalRows = 7120245;
    cleanUpFolder(folder);
    std::vector<std::string> partitioningColumns = {"min_lon", "max_lon"};
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    ASSERT_EQ(dataReader->getNumRows(), numTotalRows);
    auto partitioning = partitioning::PartitioningFactory::create(partitioning::HILBERT_CURVE, dataReader, partitioningColumns, partitionSize, folder);
    ASSERT_EQ(partitioning->partition(), arrow::Status::OK());
    std::filesystem::path partition0 = folder / ("0" + ExperimentsConfig::fileExtension);
    ASSERT_EQ(dataReader->load(partition0), arrow::Status::OK());
    auto folderResults = getFolderResults(dataReader, folder);
    auto fileCount = folderResults.first;
    auto partitionsTotalRows = folderResults.second;
    ASSERT_EQ(numTotalRows, partitionsTotalRows);
    ASSERT_EQ(fileCount, 144);
}