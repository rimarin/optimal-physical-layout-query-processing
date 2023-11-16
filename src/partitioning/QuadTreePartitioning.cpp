#include "../include/partitioning/QuadTreePartitioning.h"

namespace partitioning {

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> QuadTreePartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                                                              std::vector<std::string> partitionColumns,
                                                                                              int32_t partitionSize){
        std::cout << "[QuadTreePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[QuadTreePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
        auto columnArrowArrays = storage::DataReader::getColumns(table, partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();

        // Extract column vectors from the batch, convert them from arrow array to std vector of points
        std::vector<common::Point> partitioningColumnValues = {};
        for(const auto & column : columnData){
            common::Point columnValues;
            for (int64_t i = 0; i < column.size(); ++i) {
                columnValues.push_back(column[i]);
            }
            partitioningColumnValues.emplace_back(columnValues);
        }

        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        std::vector<common::Point> points = common::ColumnDataConverter::toRows(columnData);

        // Build a QuadTree on the vector of points
        std::shared_ptr<common::QuadTree> quadTree = std::make_shared<common::QuadTree>(points);

        // Retrieve the leaves, where the points have partitioned and stored
        std::vector<std::shared_ptr<common::QuadNode>> leaves = quadTree->getLeaves();

        // Build a hashmap to link each point to the partition induced by the QuadTree
        std::map<common::Point, int64_t> pointToPartitionId;
        for (int i = 0; i < leaves.size(); i++){
            auto partitionedPoints = leaves[i]->data;
            for (auto &point: partitionedPoints){
                pointToPartitionId[point] = i;
            }
        }

        // We need to return a vector of partition id for the passed columns
        // However, the partition id have to be aligned with the initial sorting of the points
        // Therefore, iterate over points (as passed in the first place) and assign to the return value
        // the mapped partition
        std::shared_ptr<arrow::Array> partitionIds;
        arrow::Int64Builder int64Builder;
        std::vector<int64_t> values = {};
        for(const auto & point : points){
            values.emplace_back(pointToPartitionId[point]);
        }
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[QuadTreePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::splitTableIntoPartitions(table, partitionIds);
    }

}