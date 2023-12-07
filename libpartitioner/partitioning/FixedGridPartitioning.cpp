#include "../include/partitioning/FixedGridPartitioning.h"

namespace partitioning {

    arrow::Status FixedGridPartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                   std::vector<std::string> partitionColumns,
                                                   int32_t partitionSize,
                                                   std::filesystem::path &outputFolder){
        // Note: for FixedGrid the partition size is not applicable. In fact, we can only
        // define the cell size/width in the grid and that will determine the number of elements per partition.
        // Here the partition size is used as cell width.
        std::cout << "[FixedGridPartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[FixedGridPartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
        std::cout << "[FixedGridPartitioning] Regardless of number of passed columns, the FixedGrid technique will "
                     "consider the first 2 columns and overlap the 2-dimensional space with a grid of fixed-sized cells" << std::endl;
        // std::vector<std::shared_ptr<arrow::Array>> columnArrowArrays;
        auto columnArrowArrays = storage::DataReader::getColumns(table, partitionColumns).ValueOrDie();
        /*
        if (partitionColumns.size() >= 2){
            columnArrowArrays = storage::DataReader::getColumns(table, partitionColumns).ValueOrDie();
        }
        else{
            auto cols = table->columns();
            columnArrowArrays.emplace_back(cols[0]->chunk(0));
            columnArrowArrays.emplace_back(cols[1]->chunk(0));
        }*/
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();
        std::shared_ptr<arrow::Array> partitionIds;
        arrow::Int64Builder int64Builder;
        auto x = columnData[0];
        auto y = columnData[1];
        std::cout << "[FixedGridPartitioning] Using cell width " << partitionSize << std::endl;
        std::cout << "[FixedGridPartitioning] First value x is: " << x[0] << std::endl;
        std::cout << "[FixedGridPartitioning] First value y is: " << y[0] << std::endl;
        std::cout << "[FixedGridPartitioning] Computing cell index" << std::endl;
        std::vector<int64_t> values = {};
        for (int64_t i = 0; i < x.size(); ++i) {
            long grid_x = x[i] / partitionSize;
            long grid_y = y[i] / partitionSize;
            long grid_idx = grid_y * partitionSize + grid_x;
            values.emplace_back(grid_idx);
        }
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[FixedGridPartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::writeOutPartitions(table, partitionIds, outputFolder);
    }

}