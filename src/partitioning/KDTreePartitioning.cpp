#include "../include/partitioning/KDTreePartitioning.h"

namespace partitioning {

KDTreePartitioning::KDTreePartitioning(std::vector<std::string> partitionColumns) {
    columns = std::move(partitionColumns);
}

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> KDTreePartitioning::partition(std::shared_ptr<arrow::Table> table){
    // Extract the partitioning columns from the table

    // Convert the table from columnar to row fashion

    std::vector<arrow::Array> leaves;
    // Recursively
    if pointList is empty
    return nil;
    else
    {
        // Select axis based on depth so that axis cycles through all valid values
        var int axis := depth mod k;

        // Sort point list and choose median as pivot element
        select median by axis from pointList;

        // Create node and construct subtrees
        var tree_node node;
        node.location := median;
        node.left := kdtree(points in pointList before median, depth+1);
        node.right := kdtree(points in pointList after median, depth+1);
        return node;
    }

    std::vector<std::shared_ptr<arrow::Table>> partitionedTables = {};
    partitionedTables.push_back(table);
    return partitionedTables;
}

}