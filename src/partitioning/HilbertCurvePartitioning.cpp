#include "../include/partitioning/HilbertCurvePartitioning.h"

namespace partitioning {

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> HilbertCurvePartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                                                               std::vector<std::string> partitionColumns,
                                                                                               int32_t partitionSize){
        std::cout << "[HilbertCurvePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[HilbertCurvePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
        auto columnArrowArrays = storage::DataReader::getColumns(table, partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();

        auto hilbertCurve = common::HilbertCurve();
        auto test1 = hilbertCurve.test();
        std::cout << test1;

        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();
        std::shared_ptr<arrow::Array> partitionIds;
        arrow::Int64Builder int64Builder;
        auto x = columnData[0];
        auto y = columnData[1];
        std::vector<int64_t> values = {};
        for (int64_t i = 0; i < x.size(); ++i) {
            values.emplace_back(0);
        }
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[FixedGridPartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::splitTableIntoPartitions(table, partitionIds);
    }

}