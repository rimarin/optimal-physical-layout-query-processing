#include "../include/partitioning/KDTree.h"

namespace partitioning {

KDTree::KDTree(std::vector<std::string> partitionColumns) {
    columns = std::move(partitionColumns);
}

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> KDTree::partition(std::shared_ptr<arrow::Table> table){
    std::vector<std::shared_ptr<arrow::Table>> partitionedTables = {};
    partitionedTables.push_back(table);
    return partitionedTables;
}

}