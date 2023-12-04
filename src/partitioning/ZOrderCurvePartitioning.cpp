#include "../include/partitioning/ZOrderCurvePartitioning.h"

namespace partitioning {

    arrow::Status ZOrderCurvePartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                     std::vector<std::string> partitionColumns,
                                                     int32_t partitionSize,
                                                     std::filesystem::path &outputFolder){
        std::cout << "[ZOrderCurvePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[ZOrderCurvePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
        auto columnArrowArrays = storage::DataReader::getColumns(table, partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toInt64(columnArrowArrays).ValueOrDie();
        int numDims = columnData.size();
        std::shared_ptr<arrow::Array> partitionIds;
        arrow::Int64Builder int64Builder;
        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        std::vector<common::Point> rows = common::ColumnDataConverter::toRows(columnData);

        std::map<common::Point, int64_t> rowToZValue;
        std::vector<int64_t> zOrderValues = {};
        auto zOrderCurve = common::ZOrderCurve();
        for (auto &row: rows){
            double pointArr[numDims];
            std::copy(row.begin(), row.end(), pointArr);
            uint64_t zOrderValue = zOrderCurve.encode(pointArr, numDims);
            rowToZValue[row] = zOrderValue;
            zOrderValues.emplace_back(zOrderValue);
            auto decoded = zOrderCurve.decode(zOrderValue, numDims);
            int q = 5;
        }
        // Sort z-order curve values
        std::sort(std::begin(zOrderValues), std::end(zOrderValues));

        // Iterate over sorted values and group them according to partition size
        std::map<int64_t, int64_t> zOrderValueToPartitionId;
        for (int i = 0; i < zOrderValues.size(); ++i) {
            zOrderValueToPartitionId[zOrderValues[i]] = i / partitionSize;
        }

        // Iterate over rows, get zOrderValue from rowToZValue, use it to get partitionId
        // from zOrderValueToPartitionId. Append the obtained partitionId to the result values
        std::vector<int64_t> values = {};
        for (auto &row : rows) {
            auto zOrderValueForRow = rowToZValue[row];
            auto partitionId = zOrderValueToPartitionId[zOrderValueForRow];
            values.emplace_back(partitionId);
        }

        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[ZOrderCurvePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::writeOutPartitions(table, partitionIds, outputFolder);
    }

}