#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/NoPartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"


TEST_F(TestOptimalLayoutFixture, TestGenerateParquetExamples){
    arrow::Result<std::shared_ptr<arrow::Table>> weatherTable = storage::DataWriter::GenerateExampleWeatherTable().ValueOrDie();
    arrow::Result<std::shared_ptr<arrow::Table>> schoolTable = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> noPartitioning = std::make_shared<partitioning::NoPartitioning>();
    arrow::Status statusNoPartitionWeather = storage::DataWriter::WriteTable(*weatherTable, datasetWeatherName, noPartitionFolder, noPartitioning, partitionSize);
    arrow::Status statusNoPartitionSchool = storage::DataWriter::WriteTable(*schoolTable, datasetSchoolName, noPartitionFolder, noPartitioning, partitionSize);
    std::filesystem::path expectedPath = noPartitionFolder / (datasetWeatherName + "0.parquet");
    ASSERT_EQ(std::filesystem::exists( noPartitionFolder / (datasetWeatherName + "0.parquet")), true);
    ASSERT_EQ(std::filesystem::exists( noPartitionFolder / (datasetSchoolName + "0.parquet")), true);
}

TEST_F(TestOptimalLayoutFixture, TestReadParquet){
    std::filesystem::path inputFile = noPartitionFolder / (datasetWeatherName + "0.parquet");
    arrow::Result<std::shared_ptr<arrow::Table>> tableFromDisk = storage::DataReader::ReadTable(inputFile);
    ASSERT_EQ(tableFromDisk.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> testDatasetSchool = getDataset(datasetSchoolName);
    ASSERT_EQ(testDatasetSchool.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> testDatasetWeather = getDataset(datasetWeatherName);
    ASSERT_EQ(testDatasetWeather.status(), arrow::Status::OK());
    arrow::Result<std::shared_ptr<arrow::Table>> realDatasetTPCH = getDataset(datasetTPCHName);
    ASSERT_EQ(realDatasetTPCH.status(), arrow::Status::OK());
}
