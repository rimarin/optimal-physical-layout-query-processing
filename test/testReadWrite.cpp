#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/NoPartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"


TEST_F(TestOptimalLayoutFixture, TestGenerateParquetExamples){
    auto folder = ExperimentsConfig::noPartitionFolder;
    auto dataset1 = ExperimentsConfig::datasetWeather;
    auto dataset2 = ExperimentsConfig::datasetSchool;
    auto partitionSize = 20;
    auto fileExtension = ExperimentsConfig::fileExtension;
    arrow::Result<std::shared_ptr<arrow::Table>> weatherTable = storage::DataWriter::GenerateExampleWeatherTable().ValueOrDie();
    arrow::Result<std::shared_ptr<arrow::Table>> schoolTable = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> noPartitioning = std::make_shared<partitioning::NoPartitioning>();
    arrow::Status statusWeather = noPartitioning->partition(*weatherTable, {std::string("")}, partitionSize, folder);
    arrow::Status statusSchool = noPartitioning->partition(*schoolTable, {std::string("")}, partitionSize, folder);
    std::filesystem::path expectedPath = folder / (dataset1 + "0" + fileExtension);
    ASSERT_EQ(std::filesystem::exists( folder / (dataset1 + "0" + fileExtension)), true);
    ASSERT_EQ(std::filesystem::exists( folder / (dataset2 + "0" + fileExtension)), true);
}

TEST_F(TestOptimalLayoutFixture, TestReadParquet){
    auto folder = ExperimentsConfig::noPartitionFolder;
    auto dataset1 = ExperimentsConfig::datasetWeather;
    auto dataset2 = ExperimentsConfig::datasetSchool;
    auto dataset3 = ExperimentsConfig::datasetTPCH1;
    auto fileExtension = ExperimentsConfig::fileExtension;
    std::filesystem::path inputFile = folder / (dataset1 + "0" + fileExtension);
    arrow::Result<std::shared_ptr<arrow::Table>> tableFromDisk = storage::DataReader::readTable(inputFile);
    ASSERT_EQ(tableFromDisk.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> testDatasetSchool = getDataset(dataset2);
    ASSERT_EQ(testDatasetSchool.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> testDatasetWeather = getDataset(dataset1);
    ASSERT_EQ(testDatasetWeather.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> realDatasetTPCH = getDataset(dataset3);
    ASSERT_EQ(realDatasetTPCH.status(), arrow::Status::OK());
}
