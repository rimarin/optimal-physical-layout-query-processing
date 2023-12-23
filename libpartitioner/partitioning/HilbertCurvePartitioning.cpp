#include "partitioning/HilbertCurvePartitioning.h"

namespace partitioning {

    arrow::Status HilbertCurvePartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                      std::vector<std::string> partitionColumns,
                                                      int32_t partitionSize,
                                                      std::filesystem::path &outputFolder){
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
        std::map<IntRow, int64_t> rowToHilbertValue;
        std::vector<IntRow> rows;
        std::vector<int64_t> hilbertValues = {};

        // Convert columnar format to rows and compute Hilbert value for each for them
        // Build a hashmap to link each row to the generated Hilbert value
        // In addition, append Hilbert values to one vector
        for (int i = 0; i < columnSize; ++i) {
            IntRow rowVector;
            for (int j = 0; j < numDims; ++j) {
                rowVector.emplace_back(columnData[j][i]);
            }
            rows.emplace_back(rowVector);
            auto coordinatesVector = IntRow(rowVector);
            int64_t* coordinates = coordinatesVector.data();
            hilbertCurve.axesToTranspose(coordinates, numBits, numDims);
            unsigned int hilbertValue = hilbertCurve.interleaveBits(coordinates, numBits, numDims);
            hilbertValues.emplace_back(hilbertValue);
            rowToHilbertValue[rowVector] = hilbertValue;
        }

        // Sort hilbert curve values
        std::sort(std::begin(hilbertValues), std::end(hilbertValues));

        // Iterate over sorted values and group them according to partition size
        std::map<int64_t, int64_t> hilbertValueToPartitionId;
        for (int i = 0; i < hilbertValues.size(); ++i) {
            hilbertValueToPartitionId[hilbertValues[i]] = i / partitionSize;
        }

        // Iterate over rows, get HilbertValue from rowToHilbertValue, use it to get partitionId
        // from hilbertValueToPartitionId. Append the obtained partitionId to the result values
        std::vector<int64_t> values = {};
        for (auto &row : rows) {
            auto hilbertValueForRow = rowToHilbertValue[row];
            auto partitionId = hilbertValueToPartitionId[hilbertValueForRow];
            values.emplace_back(partitionId);
        }
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[HilbertCurvePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::writeOutPartitions(table, partitionIds, outputFolder);
    }

}