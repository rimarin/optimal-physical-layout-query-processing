#ifndef PARTITIONING_QUADTREE_H
#define PARTITIONING_QUADTREE_H

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <set>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

namespace common {

    using Point = std::vector<double>;

    struct QuadNode {
        std::pair<double, double> splitValues;
        std::vector<Point> data;
        std::shared_ptr<QuadNode> northWest;
        std::shared_ptr<QuadNode> northEast;
        std::shared_ptr<QuadNode> southWest;
        std::shared_ptr<QuadNode> southEast;

        // Leaf node constructor, has associated points
        QuadNode(std::vector<Point> &values) : northWest(nullptr), northEast(nullptr), southWest(nullptr),
        southEast(nullptr), splitValues(0, 0), data(values) {};
        // Empty node constructor, only define a split value
        QuadNode(std::pair<double, double> &splitNums) : northWest(nullptr), northEast(nullptr), southWest(nullptr),
        southEast(nullptr), splitValues(splitNums), data({}) {};
    };

    class QuadTree {
    public:
        QuadTree(std::vector<Point> &points);
        std::shared_ptr<QuadNode> buildTree(std::vector<Point> points, int depth);
        virtual ~QuadTree() = default;
        std::shared_ptr<QuadNode> getRoot();
        std::vector<std::shared_ptr<QuadNode>> getLeaves();
    private:
        std::shared_ptr<QuadNode> root;
        std::vector<std::shared_ptr<QuadNode>> leaves;
    };

}

#endif //PARTITIONING_QUADTREE_H
