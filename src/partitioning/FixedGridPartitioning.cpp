
#include "../include/partitioning/FixedGridPartitioning.h"

namespace partitioning {

    FixedGridPartitioning::FixedGridPartitioning(std::vector<std::string> partitionColumns) {
        columns = std::move(partitionColumns);
        std::cout << "[FixedGridPartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : columns) displayColumns + " " += column;
        std::cout << "[FixedGridPartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
    }

    arrow::Result<arrow::Datum> FixedGridPartitioning::columnsToPartitionId(std::vector<std::shared_ptr<arrow::Array>>
    &columnArrowArrays, int32_t &partitionSize) {
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();
        std::shared_ptr<arrow::Array> fixedGridCellIds;
        arrow::Int64Builder int64Builder;
        auto x = columnData[0];
        auto y = columnData[1];
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
        ARROW_ASSIGN_OR_RAISE(fixedGridCellIds, int64Builder.Finish());
        return fixedGridCellIds;
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> FixedGridPartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                                                               int32_t partitionSize){
        std::cout << "[FixedGridPartitioning] Applying partitioning technique" << std::endl;
        auto columnData = storage::DataReader::getColumns(table, columns).ValueOrDie();
        std::shared_ptr<arrow::Array> partitionIds = columnsToPartitionId(columnData, partitionSize).ValueOrDie().make_array();
        return partitioning::MultiDimensionalPartitioning::splitTableIntoPartitions(table, partitionIds);
    }

}