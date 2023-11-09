#include "../include/partitioning/STRTreePartitioning.h"

namespace partitioning {

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> STRTreePartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                                                             std::vector<std::string> partitionColumns,
                                                                                             int32_t partitionSize){
        std::cout << "[STRTreePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[STRTreePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
        auto columnArrowArrays = storage::DataReader::getColumns(table, partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();
        std::shared_ptr<arrow::Array> partitionIds;
        arrow::Int64Builder int64Builder;
        auto x = columnData[0];
        auto y = columnData[1];
        std::vector<int64_t> values = {};
        // Implementation based on:
        // STR: A Simple and Efficient Algorithm for R-Tree Packing, https://dl.acm.org/doi/10.5555/870314
        // Preprocess the data file so that the T rectangles are
        // ordered in [r/b] consecutive groups of b rectangles,
        // where each group of b is intended to be placed in
        // the same leaf level node
        for (int64_t i = 0; i < x.size(); ++i) {
            values.emplace_back(0);
        }
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[STRTreePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::splitTableIntoPartitions(table, partitionIds);
    }

}