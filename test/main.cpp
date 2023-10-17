#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/FixedGridPartitioning.h"
#include "include/partitioning/KDTreePartitioning.h"
#include "include/partitioning/NoPartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"

#include "gtest/gtest.h"


class TestOptimalLayoutFixture: public ::testing::Test {
public:
    TestOptimalLayoutFixture( ) {
    // initialization;
    // can also be done in SetUp()
    }

    void SetUp( ) {
    // initialization or some code to run before each test
    }

    void TearDown( ) {
    // code to run after each test;
    // can be used instead of a destructor,
    // but exceptions can be handled in this function only
    }

    ~TestOptimalLayoutFixture( )  {
    // resources cleanup, no exceptions allowed
    }

    // shared user data
    std::string example1TableName = "weather";
    std::string example2TableName = "school";
    std::string noPartitionFolder = "NoPartition";
    std::string fixedGridFolder = "FixedGrid";
    std::string kdTreeFolder = "KDTree";
};

TEST_F(TestOptimalLayoutFixture, TestGenerateParquetExamples){
    arrow::Result<std::shared_ptr<arrow::Table>> weatherTable = storage::DataWriter::GenerateExampleWeatherTable().ValueOrDie();
    arrow::Result<std::shared_ptr<arrow::Table>> schoolTable = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / noPartitionFolder;
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> noPartitioning = std::make_shared<partitioning::NoPartitioning>();
    arrow::Status statusNoPartitionWeather = storage::DataWriter::WriteTable(*weatherTable, example1TableName, outputFolder, noPartitioning);
    arrow::Status statusNoPartitionSchool = storage::DataWriter::WriteTable(*schoolTable, example2TableName, outputFolder, noPartitioning);
    std::filesystem::path expectedPath = std::filesystem::current_path() / noPartitionFolder / (example1TableName + "0.parquet");
    ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / noPartitionFolder / (example1TableName + "0.parquet")), true);
    ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / noPartitionFolder / (example2TableName + "0.parquet")), true);
}

TEST_F(TestOptimalLayoutFixture, TestReadParquet){
    std::string parquetFile = noPartitionFolder + example1TableName + "0.parquet";
    std::filesystem::path inputFile = std::filesystem::current_path() / parquetFile;
    arrow::Result<std::shared_ptr<arrow::Table>> tableFromDisk = storage::DataReader::ReadTable(inputFile);
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningFixedGrid){
    arrow::Result<std::shared_ptr<arrow::Table>> exampleTable = storage::DataWriter::GenerateExampleWeatherTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / fixedGridFolder;
    std::vector<std::string> fixedGridPartitioningColumns = {"Day", "Month"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>(fixedGridPartitioningColumns, 20);
    arrow::Status statusFixedGrid = storage::DataWriter::WriteTable(*exampleTable, example1TableName, outputFolder,fixedGridPartitioning);
    int numPartitions = 2;
    for (int i = 0; i <numPartitions; ++i) {
        ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / fixedGridFolder / (example1TableName + std::to_string(i) + ".parquet")), true);
    }
}

TEST_F(TestOptimalLayoutFixture, TestPartitioningKDTree){
    arrow::Result<std::shared_ptr<arrow::Table>> kdTreeExampleTable = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::filesystem::path outputFolder = std::filesystem::current_path() / kdTreeFolder;
    std::vector<std::string> KDTreePartitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> kdTreePartitioning = std::make_shared<partitioning::KDTreePartitioning>(KDTreePartitioningColumns);
    arrow::Status statusKDTree = storage::DataWriter::WriteTable(*kdTreeExampleTable, example2TableName, outputFolder,kdTreePartitioning);
    int numPartitions = 4;
    for (int i = 0; i < numPartitions; ++i) {
        auto path = std::filesystem::current_path() / kdTreeFolder / (example2TableName + std::to_string(i) + ".parquet");
        ASSERT_EQ(std::filesystem::exists( std::filesystem::current_path() / kdTreeFolder / (example2TableName + std::to_string(i) + ".parquet")), true);
    }
}