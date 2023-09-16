#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "storage/DataWriter.h"
#include "storage/DataWriter.cpp"


int main() {
    std::string testsFolder = "test/noPartition/";
    std::filesystem::path outputFolder = std::filesystem::current_path().parent_path() / testsFolder;
    auto dataWriter = storage::DataWriter(outputFolder);
    std::shared_ptr<arrow::Table> exampleTable = dataWriter.GenerateExampleTable();
    std::string tableName = "test";
    arrow::Status st = dataWriter.WriteTable(exampleTable, tableName);
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}
