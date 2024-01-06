#ifndef COMMON_QUAD_TREE_H
#define COMMON_QUAD_TREE_H

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include <set>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include "Point.h"

namespace common {

    struct QuadNode {
        std::pair<double, double> splitValues;
        std::vector<std::shared_ptr<Point>> data;
        std::shared_ptr<QuadNode> northWest;
        std::shared_ptr<QuadNode> northEast;
        std::shared_ptr<QuadNode> southWest;
        std::shared_ptr<QuadNode> southEast;

        // Leaf node constructor, has associated points
        QuadNode(std::vector<std::shared_ptr<Point>> &values) : northWest(nullptr), northEast(nullptr), southWest(nullptr),
        southEast(nullptr), splitValues(0, 0), data(values) {};
        // Empty node constructor, only define a split value
        QuadNode(std::pair<double, double> &splitNums) : northWest(nullptr), northEast(nullptr), southWest(nullptr),
        southEast(nullptr), splitValues(splitNums), data({}) {};
    };

    class QuadTree {
    public:
        QuadTree(std::vector<std::shared_ptr<Point>> &rows, size_t partitionSize);
        std::shared_ptr<QuadNode> buildTree(std::vector<std::shared_ptr<Point>> &points, uint32_t depth);
        virtual ~QuadTree() = default;
        std::shared_ptr<QuadNode> getRoot();
        std::vector<std::shared_ptr<QuadNode>> getLeaves();
    private:
        std::shared_ptr<QuadNode> root;
        std::vector<std::shared_ptr<QuadNode>> leaves;
        uint32_t leafSize;
        std::vector<std::shared_ptr<Point>> points;
    };

}

#endif //COMMON_QUAD_TREE_H
