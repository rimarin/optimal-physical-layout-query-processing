#include "partitioning/QuadTreePartitioning.h"

namespace partitioning {

    arrow::Status QuadTreePartitioning::partition(storage::DataReader &dataReader,
                                                  const std::vector<std::string> &partitionColumns,
                                                  const size_t partitionSize,
                                                  const std::filesystem::path &outputFolder){
        std::cout << "[QuadTreePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[QuadTreePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;

        auto numRows = dataReader.getNumRows();

        if (partitionSize >= numRows) {
            std::cout << "[QuadTreePartitioning] Partition size greater than the available rows" << std::endl;
            std::cout << "[QuadTreePartitioning] Therefore put all data in one partition" << std::endl;
            std::filesystem::path source = dataReader.getReaderPath();
            std::filesystem::path destination = outputFolder / "0.parquet";
            std::filesystem::copy(source, destination, std::filesystem::copy_options::overwrite_existing);
            return arrow::Status::OK();
        }

        auto table = dataReader.readTable().ValueOrDie();
        auto columnArrowArrays = storage::DataReader::getColumnsOld(table, partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();

        // Extract column vectors from the batch, convert them from arrow array to std vector of points
        std::vector<common::Point> partitioningColumnValues = {};
        for(const auto & column : columnData){
            common::Point columnValues;
            for (double i : *column) {
                columnValues.push_back(i);
            }
            partitioningColumnValues.emplace_back(columnValues);
        }

        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        std::vector<std::shared_ptr<common::Point>> points = common::ColumnDataConverter::toRows(columnData);

        // Build a QuadTree on the vector of points
        std::shared_ptr<structures::QuadTree> quadTree = std::make_shared<structures::QuadTree>(points, partitionSize);

        // Retrieve the leaves, where the points have partitioned and stored
        std::vector<std::shared_ptr<structures::QuadNode>> leaves = quadTree->getLeaves();

        // Build a hashmap to link each point to the partition induced by the QuadTree
        std::map<std::shared_ptr<common::Point>, int64_t> pointToPartitionId;
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
        for(const auto & point : points){
            values.emplace_back(pointToPartitionId[point]);
        }
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(values));
        std::cout << "[QuadTreePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int32Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::writeOutPartitions(table, partitionIds, outputFolder);
    }

}