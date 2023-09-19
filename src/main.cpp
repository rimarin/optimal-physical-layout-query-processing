#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "../include/partitioning/FixedGrid.h"
#include "partitioning/FixedGrid.cpp"
#include "../include/storage/DataWriter.h"
#include "storage/DataWriter.cpp"


int main() {
    std::string noPartitionFolder = "test/noPartition/";
    auto dataWriter = storage::DataWriter();
    arrow::Result<std::shared_ptr<arrow::Table>> exampleTable = dataWriter.GenerateExampleTable();
    std::string tableName = "test";
    std::filesystem::path outputFolder = std::filesystem::current_path().parent_path() / noPartitionFolder;
    // arrow::Status statusNoPartition = dataWriter.WriteTable(*exampleTable, tableName, outputFolder, nullptr);

    std::string fixedGridFolder = "test/fixedGrid/";
    outputFolder = std::filesystem::current_path().parent_path() / fixedGridFolder;
    std::vector<std::string> partitioningColumns = {"Day", "Month"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> partitioningMethod = std::make_shared<partitioning::FixedGrid>(partitioningColumns, 2);
    arrow::Status statusFixedGrid = dataWriter.WriteTable(*exampleTable, tableName, outputFolder,
                                                          partitioningMethod);
    return 0;
}
