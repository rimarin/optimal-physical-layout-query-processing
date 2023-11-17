#include "../include/partitioning/STRTreePartitioning.h"

namespace partitioning {

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> STRTreePartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                                                             std::vector<std::string> partitionColumns,int32_t partitionSize) {
        /* Implementation based on:
           STR: A Simple and Efficient Algorithm for R-Tree Packing, https://dl.acm.org/doi/10.5555/870314
           An R-tree is a hierarchical data structure derived from the B-tree, which stores a collection of
           rectangles which can change overtime. Each R-Tree has r nodes, each can hold n rectangles/points.
           There are different packing algorithms, here we focus on the Sort-Tile-Recursive (STR).
           Consider a k-dimension data set of r hyper-rectangles. A hyper-rectangle is defined by k
           intervals of the form [Ai, Bi] and is the locus of points whose i-th coordinate falls inside the
           i-th interval, for all 1 < i < k.
           STR is best described recursively with k = 2 providing the base case. (The case k = 1 is
           already handled well by regular B-trees.) Accordingly, we first consider a set of rectangles in
           the plane. The basic idea is to "tile" the data space using sqrt(r/n) vertical slices so that each slice
           contains enough rectangles to pack roughly sqrt(r/n) nodes. Once again we assume coordinates
           are for the center points of the rectangles. Determine the number of leaf level pages P = [r/n]
           and let S = sqrt(P). Sort the rectangles by x-coordinate and partition them into S vertical
           slices. A slice consists of a run of S * n consecutive rectangles from the sorted list. Note that
           the last slice may contain fewer than S * n rectangles. Now sort the rectangles of each slice
           by y-coordinate and pack them into nodes by grouping them into runs of length n (the first n
           rectangles into the first node, the next n into the second node, and so on).
        */
        std::cout << "[STRTreePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column: partitionColumns) displayColumns + " " += column;
        std::cout << "[STRTreePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
        auto columnArrowArrays = storage::DataReader::getColumns(table, partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();
        std::shared_ptr<arrow::Array> partitionIds;
        arrow::Int64Builder int64Builder;

        std::vector<common::Point> points = common::ColumnDataConverter::toRows(columnData);

        k = columnData.size();
        r = points.size();
        n = partitionSize;
        P = r / n;
        S = ceil(sqrt(P));

        int coord = 0;
        sortTileRecursive({points}, coord);

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
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(values));
        std::cout << "[STRTreePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int64Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::splitTableIntoPartitions(table, partitionIds);
    }

    void STRTreePartitioning::sortTileRecursive(std::vector<common::Point> points, int coord) {
        if (points.size() <= n || coord == k){
            slices.emplace_back(points);
            return;
        }
        std::sort(points.begin(), points.end(),
                  [&coord](const std::vector<double>& a, const std::vector<double>& b) {
                      return a[coord] < b[coord];
                  });
        for (int i = 0; i < S; ++i) {
            auto begin = points.begin() + (i * (points.size() / S) );
            auto end = points.begin() + ((i + 1) * (points.size() / S) );
            if (end >= points.end()){
                end = points.end();
            }
            auto slice = std::vector(begin, end);
            sortTileRecursive(slice, coord+1);
        }
    }

}