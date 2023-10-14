#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "partitioning/FixedGridPartitioning.h"
#include "partitioning/KDTreePartitioning.h"
#include "partitioning/NoPartitioning.h"
#include "storage/DataWriter.h"
#include "storage/DataReader.h"


int main() {

    std::string parquetFile = "test/NoPartition/test0.parquet";
    std::filesystem::path inputFile = std::filesystem::current_path().parent_path() / parquetFile;
    arrow::Result<std::shared_ptr<arrow::Table>> tableFromDisk = storage::DataReader::ReadTable(inputFile).ValueOrDie();

    arrow::Result<std::shared_ptr<arrow::Table>> exampleTable = storage::DataWriter::GenerateExampleTable().ValueOrDie();
    arrow::Result<std::shared_ptr<arrow::Table>> kdTreeExampleTable = storage::DataWriter::GenerateKDTreeExampleTable().ValueOrDie();
    std::string tableName = "test";

    std::string noPartitionFolder = "test/NoPartition/";
    std::filesystem::path outputFolder = std::filesystem::current_path().parent_path() / noPartitionFolder;
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> noPartitioning = std::make_shared<partitioning::NoPartitioning>();
    arrow::Status statusNoPartition = storage::DataWriter::WriteTable(*exampleTable, tableName, outputFolder, noPartitioning);

    std::string fixedGridFolder = "test/FixedGrid/";
    outputFolder = std::filesystem::current_path().parent_path() / fixedGridFolder;
    std::vector<std::string> fixedGridPartitioningColumns = {"Day", "Month"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGridPartitioning>(fixedGridPartitioningColumns, 20);
    arrow::Status statusFixedGrid = storage::DataWriter::WriteTable(*tableFromDisk, tableName, outputFolder,fixedGridPartitioning);

    std::string kdTreeFolder = "test/KDTree/";
    outputFolder = std::filesystem::current_path().parent_path() / kdTreeFolder;
    std::vector<std::string> KDTreePartitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> kdTreePartitioning = std::make_shared<partitioning::KDTreePartitioning>(KDTreePartitioningColumns);
    arrow::Status statusKDTree = storage::DataWriter::WriteTable(*kdTreeExampleTable, tableName, outputFolder,kdTreePartitioning);
    return 0;
}
