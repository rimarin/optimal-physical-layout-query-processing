#include "partitioning/STRTreePartitioning.h"

namespace partitioning {

    arrow::Status STRTreePartitioning::partition() {
        /* Implementation based on:
         * STR: A Simple and Efficient Algorithm for R-Tree Packing, https://dl.acm.org/doi/10.5555/870314
         * An R-tree is a hierarchical data structure derived from the B-tree, which stores a collection of
         * rectangles which can change overtime. Each R-Tree has r nodes, each can hold n rectangles/points.
         * There are different packing algorithms, here we focus on the Sort-Tile-Recursive (STR).
         * Consider a k-dimension data set of r hyper-rectangles. A hyper-rectangle is defined by k
         * intervals of the form [Ai, Bi] and is the locus of points whose i-th coordinate falls inside the
         * i-th interval, for all 1 < i < k.
         * STR is best described recursively with k = 2 providing the base case. (The case k = 1 is
         * already handled well by regular B-trees.) Accordingly, we first consider a set of rectangles in
         * the plane. The basic idea is to "tile" the data space using sqrt(r/n) vertical slices so that each slice
         * contains enough rectangles to pack roughly sqrt(r/n) nodes. Once again we assume coordinates
         * are for the center points of the rectangles. Determine the number of leaf level pages P = [r/n]
         * and let S = sqrt(P). Sort the rectangles by x-coordinate and partition them into S vertical
         * slices. A slice consists of a run of S * n consecutive rectangles from the sorted list. Note that
         * the last slice may contain fewer than S * n rectangles. Now sort the rectangles of each slice
         * by y-coordinate and pack them into nodes by grouping them into runs of length n (the first n
         * rectangles into the first node, the next n into the second node, and so on).
        */

        // Convert columns to rows
        auto table = dataReader->readTable().ValueOrDie();
        auto columnArrowArrays = storage::DataReader::getColumnsOld(table, columns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toDouble(columnArrowArrays).ValueOrDie();
        std::vector<std::shared_ptr<common::Point>> points = common::ColumnDataConverter::toRows(columnData);

        k = columnData.size();
        r = points.size();
        n = partitionSize;
        P = r / n;
        S = ceil(sqrt(P));
        slices = {};

        int coord = 0;
        sortTileRecursive(points, coord);

        std::map<std::shared_ptr<common::Point>, int64_t> pointToPartitionId;
        for (int i = 0; i < slices.size(); ++i) {
            for (const auto &point: slices[i]){
                pointToPartitionId[point] = i;
            }
        }
        std::vector<uint32_t> values = {};
        for (const auto &point: points){
            values.emplace_back(pointToPartitionId[point]);
        }
        arrow::UInt32Builder int32Builder;
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(values));
        std::cout << "[STRTreePartitioning] Mapped columns to partition ids" << std::endl;
        std::shared_ptr<arrow::Array> partitionIds;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int32Builder.Finish());
        return partitioning::MultiDimensionalPartitioning::writeOutPartitions(table, partitionIds, folder);
    }

    void STRTreePartitioning::sortTileRecursive(std::vector<std::shared_ptr<common::Point>> points, int coord) {
        if (points.size() <= n){
            slices.emplace_back(points);
            return;
        }
        int coordToUse = coord % k;
        std::sort(points.begin(), points.end(),
                  [&coordToUse](const std::shared_ptr<std::vector<double>>& a, const std::shared_ptr<std::vector<double>>& b) {
                      return a->at(coordToUse) < b->at(coordToUse);
                  });
        for (int i = 0; i < S; ++i) {
            auto begin = points.begin() + (i * (points.size() / S) );
            auto end = points.begin() + ((i + 1) * (points.size() / S) );
            if (end >= points.end()){
                end = points.end();
            }
            auto slice = std::vector<std::shared_ptr<common::Point>>(begin, end);
            sortTileRecursive(slice, coordToUse+1);
        }
    }

}