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

namespace partitioning {

    class KDTree {
    public:
        KDTree(std::vector<arrow::Array> values);
        virtual ~KDTree() = default;
        std::vector<struct KDNode> getLeaves();
    private:
        KDTree buildTreeRecursive(std::vector<arrow::Array> values);
        std::vector<arrow::Array> pointList;
    };

    struct KDNode {
        arrow::Array *data;
        struct KDNode *left, *right;
    };
}

#endif //PARTITIONING_KDTREE_H
