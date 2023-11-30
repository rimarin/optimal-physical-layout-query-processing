#include "../include/partitioning/GridFilePartitioning.h"

namespace partitioning {

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> GridFilePartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                                                              std::vector<std::string> partitionColumns,
                                                                                              int32_t partitionSize){
        /*
         * In the literature there are different implementations of adaptive grid.
         * Here we use a simplified version of the algorithm described in Flood:
         * "Learning Multi-Dimensional Indexes", https://dl.acm.org/doi/10.1145/3318464.3380579
         * A certain dimension is chosen and the data space is divided into equally spaced slices.
         * Each slice is then sorted by the other dimension. Flood adapts the slice size using a model
         * of the underlying data (Recursive Model Index - RMI) and at the same time takes into account
         * the workload characteristics.
         * Unlike Flood, this approach is not workload-aware, we only provide trivial data-awareness
         * through the sort dimension selection. For that, we'll pick the most skewed dimension.
         * The skewness is measured with the notion of nonparametric skew, defined as: (\mu -\nu )/ \sigma
         * where \mu is the mean, \nu is the median, and \sigma is the standard deviation.
         * In many ways it is similar to STRTree, with a couple of differences:
         * - we do not have partially-filled cells, except possibly the last one
         * - the choice of sort dimension (which has a relevant impact) does not take the first
         *   column among the partitioning dimensions, but the most skewed dimension
        */
        std::cout << "[GridFilePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[GridFilePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
        auto columnArrowArrays = storage::DataReader::getColumns(table, partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();

        std::vector<common::Point> points = common::ColumnDataConverter::toRows(columnData);

        std::vector<std::tuple<int, double>> columnIndexToSkewPairs;
        for(int i = 0; i <columnData.size(); ++i) {
            auto skew = common::ColumnDataConverter::getColumnSkew(columnData[i]);
            columnIndexToSkewPairs.emplace_back(i, skew);
        }
        std::sort(columnIndexToSkewPairs.begin(), columnIndexToSkewPairs.end(),
                  [](const std::tuple<int, double>& a, const std::tuple<int, double>& b) {
                      return std::get<1>(a) < std::get<1>(b);
                  });
        for (auto &indexAndSkew: columnIndexToSkewPairs) {
            columnIndexes.emplace_back(std::get<0>(indexAndSkew));
        }

        k = columnData.size();
        r = points.size();
        n = partitionSize;
        P = r / n;
        S = ceil(sqrt(P));
        slices = {};

        int coordIdx = 0;
        packSlicesRecursive(points, coordIdx);

        std::map<common::Point, int64_t> pointToPartitionId;
        for (int i = 0; i < slices.size(); ++i) {
            for (const auto &point: slices[i]){
                pointToPartitionId[point] = i;
            }
        }
        std::vector<int64_t> values = {};
        for (const auto &point: points){
            values.emplace_back(pointToPartitionId[point]);
        }
        arrow::Int64Builder int64Builder;
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[GridFilePartitioning] Mapped columns to partition ids" << std::endl;
        std::shared_ptr<arrow::Array> partitionIds;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::splitTableIntoPartitions(table, partitionIds);
    }

    void GridFilePartitioning::packSlicesRecursive(std::vector<common::Point> points, int coord) {
        if (points.size() <= n){
            slices.emplace_back(points);
            return;
        }
        int coordToUse = columnIndexes[coord % k];
        std::sort(points.begin(), points.end(),
                  [&coordToUse](const std::vector<double>& a, const std::vector<double>& b) {
                      return a[coordToUse] < b[coordToUse];
                  });
        for (int i = 0; i < S; ++i) {
            auto begin = points.begin() + (i * (points.size() / S) );
            auto end = points.begin() + ((i + 1) * (points.size() / S) );
            if (end >= points.end()){
                end = points.end();
            }
            auto slice = std::vector<common::Point>(begin, end);
            packSlicesRecursive(slice, coordToUse+1);
        }
    }

}