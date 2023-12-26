#ifndef COMMON_KD_TREE_H
#define COMMON_KD_TREE_H

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

#include "Point.h"

namespace common {

    struct KDNode {
        double splitValue;
        std::vector<Point> data;
        std::shared_ptr<KDNode> left;
        std::shared_ptr<KDNode> right;

        // Leaf node constructor, has associated points
        KDNode(std::vector<Point> &values) : left(nullptr), right(nullptr), splitValue(0), data(values) {};
        // Empty node constructor, only define a split value
        KDNode(double &splitNum) : left(nullptr), right(nullptr), splitValue(splitNum), data({}) {};
    };

    class KDTree {
    public:
        KDTree(std::vector<Point> &points, int32_t partitionSize);
        std::shared_ptr<KDNode> buildTree(std::vector<Point> points, int depth);
        virtual ~KDTree() = default;
        std::shared_ptr<KDNode> getRoot();
        std::vector<std::shared_ptr<KDNode>> getLeaves();
    private:
        int32_t leafSize;
        std::shared_ptr<KDNode> root;
        std::vector<std::shared_ptr<KDNode>> leaves;
    };

}

#endif //COMMON_KD_TREE_H
