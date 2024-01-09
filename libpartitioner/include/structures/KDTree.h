#ifndef STRUCTURES_KD_TREE_H
#define STRUCTURES_KD_TREE_H

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>
#include <set>

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/dataset/api.h"
#include "arrow/io/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"

#include "common/Point.h"

namespace structures {

    struct KDNode {
        double splitValue;
        std::vector<std::shared_ptr<common::Point>> data;
        std::shared_ptr<KDNode> left;
        std::shared_ptr<KDNode> right;

        // Leaf node constructor, has associated points
        KDNode(std::vector<std::shared_ptr<common::Point>> values) : left(nullptr), right(nullptr), splitValue(0), data(std::move(values)) {};
        // Empty node constructor, only define a split value
        KDNode(double &splitNum) : left(nullptr), right(nullptr), splitValue(splitNum), data({}) {};
    };

    class KDTree {
    public:
        KDTree(std::vector<std::shared_ptr<common::Point>> &rows, size_t partitionSize);
        std::shared_ptr<KDNode> buildTree(std::vector<std::shared_ptr<common::Point>>::iterator start,
                                          std::vector<std::shared_ptr<common::Point>>::iterator end,
                                          uint32_t depth);
        virtual ~KDTree() = default;
        std::shared_ptr<KDNode> getRoot();
        std::vector<std::shared_ptr<KDNode>> getLeaves();
    private:
        uint32_t leafSize;
        uint32_t pointDimensions;
        std::shared_ptr<KDNode> root;
        std::vector<std::shared_ptr<KDNode>> leaves;
        std::vector<std::shared_ptr<common::Point>> points;
    };

}

#endif //STRUCTURES_KD_TREE_H
