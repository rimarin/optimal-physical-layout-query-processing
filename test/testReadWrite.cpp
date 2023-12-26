#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/NoPartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "include/storage/TableGenerator.h"
#include "fixture.cpp"

#include "gtest/gtest.h"


TEST_F(TestOptimalLayoutFixture, TestGenerateParquetExamples){
    auto folder = ExperimentsConfig::noPartitionFolder;
    auto fileExtension = ExperimentsConfig::fileExtension;
    std::filesystem::path dataset1 = folder.string() + "/" + ExperimentsConfig::datasetWeather + "0" + fileExtension;
    std::filesystem::path dataset2 = folder.string() + "/" + ExperimentsConfig::datasetSchool + "0" + fileExtension;
    std::filesystem::path dataset3 = folder.string() + "/" + ExperimentsConfig::datasetCities + "0" + fileExtension;
    auto partitionSize = 20;
    arrow::Result<std::shared_ptr<arrow::Table>> weatherTable = storage::TableGenerator::GenerateWeatherTable().ValueOrDie();
    arrow::Result<std::shared_ptr<arrow::Table>> schoolTable = storage::TableGenerator::GenerateSchoolTable().ValueOrDie();
    arrow::Result<std::shared_ptr<arrow::Table>> citiesTable = storage::TableGenerator::GenerateCitiesTable().ValueOrDie();
    ASSERT_EQ(storage::DataWriter::WriteTable(*weatherTable, dataset1), arrow::Status::OK());
    ASSERT_EQ(storage::DataWriter::WriteTable(*schoolTable, dataset2), arrow::Status::OK());
    ASSERT_EQ(storage::DataWriter::WriteTable(*citiesTable, dataset3), arrow::Status::OK());
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> noPartitioning = std::make_shared<partitioning::NoPartitioning>();
    auto dataReader = storage::DataReader();
    auto datasetWeather = getDatasetPath(ExperimentsConfig::datasetWeather);
    ASSERT_EQ(dataReader.load(datasetWeather), arrow::Status::OK());
    arrow::Status statusWeather = noPartitioning->partition(dataReader, {std::string("")}, partitionSize, folder);
    auto dataseSchool = getDatasetPath(ExperimentsConfig::datasetWeather);
    ASSERT_EQ(dataReader.load(dataseSchool), arrow::Status::OK());
    arrow::Status statusSchool = noPartitioning->partition(dataReader, {std::string("")}, partitionSize, folder);
    auto datasetCities = getDatasetPath(ExperimentsConfig::datasetWeather);
    ASSERT_EQ(dataReader.load(datasetCities), arrow::Status::OK());
    arrow::Status statusCities = noPartitioning->partition(dataReader, {std::string("")}, partitionSize, folder);
    ASSERT_EQ(std::filesystem::exists( folder / (dataset1)), true);
    ASSERT_EQ(std::filesystem::exists( folder / (dataset2)), true);
    ASSERT_EQ(std::filesystem::exists( folder / (dataset3)), true);
}

TEST_F(TestOptimalLayoutFixture, TestReadParquet){
    auto folder = ExperimentsConfig::noPartitionFolder;
    auto dataset1 = ExperimentsConfig::datasetWeather;
    auto dataset2 = ExperimentsConfig::datasetSchool;
    auto dataset3 = ExperimentsConfig::datasetTPCH1;
    auto fileExtension = ExperimentsConfig::fileExtension;
    std::filesystem::path inputFile = folder / (dataset1 + "0" + fileExtension);
    arrow::Result<std::shared_ptr<arrow::Table>> tableFromDisk = storage::DataReader::getTable(inputFile);
    ASSERT_EQ(tableFromDisk.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> testDatasetSchool = getDataset(dataset2);
    ASSERT_EQ(testDatasetSchool.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> testDatasetWeather = getDataset(dataset1);
    ASSERT_EQ(testDatasetWeather.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> realDatasetTPCH = getDataset(dataset3);
    ASSERT_EQ(realDatasetTPCH.status(), arrow::Status::OK());
}
