#include "partitioning/ZOrderCurvePartitioning.h"

namespace partitioning {

    arrow::Status ZOrderCurvePartitioning::partition(storage::DataReader &dataReader,
                                                     const std::vector<std::string> &partitionColumns,
                                                     const size_t partitionSize,
                                                     const std::filesystem::path &outputFolder){
        std::cout << "[ZOrderCurvePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[ZOrderCurvePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
        auto table = dataReader.readTable().ValueOrDie();
        auto columnArrowArrays = storage::DataReader::getColumnsOld(table, partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toInt64(columnArrowArrays).ValueOrDie();
        int numDims = columnData.size();
        std::shared_ptr<arrow::Array> partitionIds;
        arrow::UInt32Builder int32Builder;
        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        std::vector<std::shared_ptr<common::Point>> rows = common::ColumnDataConverter::toRows(columnData);

        std::map<std::shared_ptr<common::Point>, int64_t> rowToZValue;
        std::vector<int64_t> zOrderValues = {};
        auto zOrderCurve = common::ZOrderCurve();
        for (auto &row: rows){
            double pointArr[numDims];
            std::copy(row->begin(), row->end(), pointArr);
            uint64_t zOrderValue = zOrderCurve.encode(pointArr, numDims);
            rowToZValue[row] = zOrderValue;
            zOrderValues.emplace_back(zOrderValue);
            auto decoded = zOrderCurve.decode(zOrderValue, numDims);
            int q = 5;
        }
        // Sort z-order curve values
        std::sort(std::begin(zOrderValues), std::end(zOrderValues));

        // Iterate over sorted values and group them according to partition size
        std::map<int64_t, uint32_t> zOrderValueToPartitionId;
        for (int i = 0; i < zOrderValues.size(); ++i) {
            zOrderValueToPartitionId[zOrderValues[i]] = i / partitionSize;
        }

        // Iterate over rows, get zOrderValue from rowToZValue, use it to get partitionId
        // from zOrderValueToPartitionId. Append the obtained partitionId to the result values
        std::vector<uint32_t> values = {};
        for (auto &row : rows) {
            auto zOrderValueForRow = rowToZValue[row];
            auto partitionId = zOrderValueToPartitionId[zOrderValueForRow];
            values.emplace_back(partitionId);
        }

        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(values));
        std::cout << "[ZOrderCurvePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int32Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::writeOutPartitions(table, partitionIds, outputFolder);
    }

}