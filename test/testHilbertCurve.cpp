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
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Student_id", std::vector<int32_t>({16, 45, 21, 7, 34})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("0" + fileExtension), "Age", std::vector<int32_t>({30, 21, 18, 27, 37})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Student_id", std::vector<int32_t>({74, 111, 91})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("1" + fileExtension), "Age", std::vector<int32_t>({41, 23, 22})), arrow::Status::OK());
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
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("1" + fileExtension), "city", std::vector<std::string>({"Copenhagen", "Oslo"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("2" + fileExtension), "city", std::vector<std::string>({"Amsterdam", "Madrid"})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::StringArray>(folder / ("3" + fileExtension), "city", std::vector<std::string>({"Tallinn", "Berlin"})), arrow::Status::OK());
}