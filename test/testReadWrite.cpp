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
    std::filesystem::path outputFolder = std::filesystem::current_path() / noPartitionFolder;
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> noPartitioning = std::make_shared<partitioning::NoPartitioning>();
    arrow::Status statusNoPartitionWeather = storage::DataWriter::WriteTable(*weatherTable, datasetWeatherName, outputFolder, noPartitioning);
    arrow::Status statusNoPartitionSchool = storage::DataWriter::WriteTable(*schoolTable, datasetSchoolName, outputFolder, noPartitioning);
    std::filesystem::path expectedPath = std::filesystem::current_path() / noPartitionFolder / (datasetWeatherName + "0.parquet");
    ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / noPartitionFolder / (datasetWeatherName + "0.parquet")), true);
    ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / noPartitionFolder / (datasetSchoolName + "0.parquet")), true);
}

TEST_F(TestOptimalLayoutFixture, TestReadParquet){
    std::string parquetFile = noPartitionFolder + datasetWeatherName + "0.parquet";
    std::filesystem::path inputFile = std::filesystem::current_path() / parquetFile;
    arrow::Result<std::shared_ptr<arrow::Table>> tableFromDisk = storage::DataReader::ReadTable(inputFile);
}
