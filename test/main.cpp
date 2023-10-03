#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "partitioning/FixedGrid.h"
#include "partitioning/NoPartitioning.h"
#include "storage/DataWriter.h"


int main() {
    arrow::Result<std::shared_ptr<arrow::Table>> exampleTable = storage::DataWriter::GenerateExampleTable().ValueOrDie();
    std::string tableName = "test";

    std::string noPartitionFolder = "test/noPartition/";
    std::filesystem::path outputFolder = std::filesystem::current_path().parent_path() / noPartitionFolder;
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> noPartitioning = std::make_shared<partitioning::NoPartitioning>();
    arrow::Status statusNoPartition = storage::DataWriter::WriteTable(*exampleTable, tableName, outputFolder, noPartitioning);

    std::string fixedGridFolder = "test/fixedGrid/";
    outputFolder = std::filesystem::current_path().parent_path() / fixedGridFolder;
    std::vector<std::string> partitioningColumns = {"Day", "Month"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> fixedGridPartitioning = std::make_shared<partitioning::FixedGrid>(partitioningColumns, 20);
    arrow::Status statusFixedGrid = storage::DataWriter::WriteTable(*exampleTable, tableName, outputFolder,fixedGridPartitioning);
    return 0;
}
