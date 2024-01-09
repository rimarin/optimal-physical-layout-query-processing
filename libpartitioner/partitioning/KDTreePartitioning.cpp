#include "partitioning/KDTreePartitioning.h"

namespace partitioning {

    arrow::Status KDTreePartitioning::partition(){

        // Convert columns to rows
        auto table = dataReader->readTable().ValueOrDie();
        auto columnArrowArrays = storage::DataReader::getColumnsOld(table, columns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();

        // Extract column vectors from the batch, convert them from arrow array to std vector of points
        std::vector<common::Point> partitioningColumnValues = {};
        for(const auto & column : columnData){
            common::Point columnValues;
            for (int64_t i = 0; i < column->size(); ++i) {
                columnValues.push_back(column->at(i));
            }
            partitioningColumnValues.emplace_back(columnValues);
        }

        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        std::vector<std::shared_ptr<common::Point>> points = common::ColumnDataConverter::toRows(columnData);

        // Build a kd-tree on the vector of points
        std::shared_ptr<structures::KDTree> kdTree = std::make_shared<structures::KDTree>(points, partitionSize);

        // Retrieve the leaves, where the points have partitioned and stored
        std::vector<std::shared_ptr<structures::KDNode>> leaves = kdTree->getLeaves();

        // Build a hashmap to link each point to the partition induced by the kd-tree
        std::map<std::shared_ptr<common::Point>, uint32_t> pointToPartitionId;
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
        arrow::UInt32Builder int32Builder;
        std::vector<uint32_t> values = {};
        values.reserve(points.size());
        for (const auto & point : points){
            values.emplace_back(pointToPartitionId[point]);
        }
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(values));
        std::cout << "[KDTreePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int32Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::writeOutPartitions(table, partitionIds, folder);
    }

}