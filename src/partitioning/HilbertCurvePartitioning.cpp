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
        auto columnData = converter.toInt64(columnArrowArrays).ValueOrDie();
        std::shared_ptr<arrow::Array> partitionIds;
        arrow::Int64Builder int64Builder;
        auto hilbertCurve = common::HilbertCurve();
        int numBits = 8;
        int numDims = columnData.size();
        int columnSize = columnData[0].size();
        std::map<common::Point, int64_t> rowToHilbertValue;
        std::vector<int64_t> hilbertValues = {};
        // Convert columnar format to rows and compute Hilbert value for each for them
        // Build a hashmap to link each row to the generated Hilbert value
        // In addition, append Hilbert values to one vector
        for (int i = 0; i < columnSize; ++i) {
            std::vector<int64_t> rowVector;
            for (int j = 0; j < numDims; ++j) {
                rowVector.emplace_back(columnData[j][i]);
            }
            int64_t* row = rowVector.data();
            hilbertCurve.axesToTranspose(row, numBits, numDims);
            unsigned int hilbertValue = hilbertCurve.interleaveBits(row, numBits, numDims);
            hilbertValues.emplace_back(hilbertValue);
            rowToHilbertValue[rowVector] = hilbertValue;
        }
        // TODO: sort hilbert curve values

        // TODO: iterate over sorted values and group them according to partition size
        //  create map<hilbertValue, partitionId> hilbertValueToPartitionId

        // TODO: iterate over rows, get HilbertValue from rowToHilbertValue, use it to get partitionId
        //  from hilbertValueToPartitionId. Append the obtained partitionId to the result values

        std::vector<int64_t> values = {};
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[FixedGridPartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::splitTableIntoPartitions(table, partitionIds);
    }

}