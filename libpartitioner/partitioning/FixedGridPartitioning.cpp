#include <algorithm>
#include <numeric>

#include "partitioning/FixedGridPartitioning.h"

namespace partitioning {

    arrow::Status FixedGridPartitioning::partition(storage::DataReader &dataReader,
                                                      const std::vector<std::string> &partitionColumns,
                                                      const size_t partitionSize,
                                                      const std::filesystem::path &outputFolder) {
        std::cout << "[FixedGridPartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns = std::accumulate(partitionColumns.begin(), partitionColumns.end(), std::string(" "));
        std::cout << "[FixedGridPartitioning] Partition has to be done on columns: " << displayColumns << std::endl;

        auto columnArrowArrays = dataReader.getColumns(partitionColumns).ValueOrDie();
        auto numColumns = partitionColumns.size();
        auto numRows = columnArrowArrays[0]->length();

        if (partitionSize > numRows){
            std::cout << "[FixedGridPartitioning] Partition size greater than the available rows" << std::endl;
            std::cout << "[FixedGridPartitioning] Therefore put all data in one partition" << std::endl;
            std::vector<int64_t> cellIndexes(numRows, 0);
            arrow::Int64Builder int64Builder;
            ARROW_RETURN_NOT_OK(int64Builder.AppendValues(cellIndexes));
            std::cout << "[FixedGridPartitioning] Mapped columns to partition ids" << std::endl;
            std::shared_ptr<arrow::Array> partitionIds;
            ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
            auto table = dataReader.readTable();
            return partitioning::MultiDimensionalPartitioning::writeOutPartitions(*table,
                                                                                  partitionIds,
                                                                                  outputFolder);
        }

        // Number of cells is = (domain(x) / cellSize) * (domain(y) / cellSize) ... * (domain(n) / cellSize)
        // e.g. 20x20 grid, 100x100 coordinates -> (100 / 20) * (100 / 20) = 5 * 5 = 25 squares
        std::cout << "[FixedGridPartitioning] Analyzing span of column values to determine cell width" << std::endl;
        uint64_t columnDomainAverage = 0;
        std::unordered_map<uint8_t, uint64_t> columnToDomain;
        for (int j = 0; j < numColumns; ++j) {
            auto columnStats = dataReader.getColumnStats(partitionColumns[j]).ValueOrDie();
            uint64_t columnDomain = (columnStats.second - columnStats.first) * 1.2;
            columnToDomain[j] = columnDomain;
            columnDomainAverage += columnDomain;
        }
        columnDomainAverage /= numColumns;
        // uint64_t cellWidth = columnDomainAverage / 5 * partitionSize;
        uint64_t cellWidth = columnDomainAverage / 10;
        std::cout << "[FixedGridPartitioning] Computed cell width is: " << cellWidth << std::endl;

        std::vector<int64_t> cellIndexes = {};
        for (int i = 0; i < numRows; ++i){
            int64_t cellIndex = 0;
            for (int j = 0; j < numColumns; ++j){
                auto dimensionNumCells = std::floor(columnToDomain[j] / cellWidth);
                auto arrow_int32_array = std::static_pointer_cast<arrow::Int32Array>(columnArrowArrays[j]->chunk(0));
                auto value = arrow_int32_array->Value(i);
                auto cellDimensionIndex = std::floor(value / cellWidth);
                if (j > 0){
                    cellIndex += cellDimensionIndex * dimensionNumCells;
                }
                else{
                    cellIndex += cellDimensionIndex;
                }
            }
            cellIndexes.emplace_back(cellIndex);
        }

        std::vector<size_t> idx(cellIndexes.size());
        iota(idx.begin(), idx.end(), 0);

        std::stable_sort(idx.begin(), idx.end(),
                    [&cellIndexes](size_t i1, size_t i2) {return cellIndexes[i1] < cellIndexes[i2];});

        std::map<uint64_t, uint64_t> cellIndexToPartition;
        std::sort(std::begin(cellIndexes), std::end(cellIndexes));
        for (int i = 0; i < cellIndexes.size(); ++i) {
            cellIndexToPartition[idx[i]] = i / partitionSize;
        }

        std::vector<int64_t> values = {};
        values.reserve(numRows);
        for (int i = 0; i < numRows; ++i){
            values.emplace_back(cellIndexToPartition[i]);
        }

        arrow::Int64Builder int64Builder;
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[FixedGridPartitioning] Mapped columns to partition ids" << std::endl;
        std::shared_ptr<arrow::Array> partitionIds;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        auto table = dataReader.readTable();
        auto batch_reader = dataReader.getTableBatchReader();

        return partitioning::MultiDimensionalPartitioning::writeOutPartitions(*table,
                                                                              partitionIds,
                                                                              outputFolder);
    }

}