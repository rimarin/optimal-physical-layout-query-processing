#ifndef PARTITIONING_KDTREE_H
#define PARTITIONING_KDTREE_H

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

    struct KDNode {
        double splitValue;
        std::vector<Point> data;
        std::shared_ptr<KDNode> left;
        std::shared_ptr<KDNode> right;

        KDNode(std::vector<Point> &values) : left(nullptr), right(nullptr), splitValue(0), data(values) {};
        KDNode(double &splitNum) : left(nullptr), right(nullptr), splitValue(splitNum), data({}) {};
    };

    class KDTree {
    public:
        KDTree(std::vector<Point> &points);
        std::shared_ptr<KDNode> buildTree(std::vector<Point> points, int depth);
        virtual ~KDTree() = default;
        std::shared_ptr<KDNode> getRoot();
        std::vector<std::shared_ptr<KDNode>> getLeaves(std::shared_ptr<KDNode> node);
    private:
        std::shared_ptr<KDNode> root;
        std::vector<std::shared_ptr<KDNode>> leaves;
    };

}

#endif //PARTITIONING_KDTREE_H
