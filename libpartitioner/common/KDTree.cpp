#include "common/KDTree.h"

namespace common {

    KDTree::KDTree(std::vector<std::shared_ptr<Point>> &rows, size_t partitionSize) {
        // Construct the tree from the given points and store the root
        leafSize = partitionSize;
        std::cout << "[KDTree] Start building a kd-tree for " << rows.size() << " points and partition size " << partitionSize << std::endl;
        points = rows;
        pointDimensions = points[0]->size();
        root = buildTree(points.begin(), points.end(), 0);
    }

    std::shared_ptr<KDNode> KDTree::buildTree(std::vector<std::shared_ptr<Point>>::iterator start,
                                              std::vector<std::shared_ptr<Point>>::iterator end,
                                              uint32_t depth){
        uint32_t pointsSize = std::distance(start, end);
        std::cout << "[KDTree] Reached depth " << depth << " with " << pointsSize << " elements" << std::endl;
        // Recursive implementation of kdTree construction from a set of multidimensional points
        if (pointsSize == 0) {
            return nullptr;
        }
        // When we reach the desired leaf size (which is the number of rows per parquet partition)
        // We should store the node and exit the recursion
        if (pointsSize <= leafSize){
            auto node = std::make_shared<KDNode>(std::vector<std::shared_ptr<common::Point>>(start, end));
            leaves.emplace_back(node);
            return node;
        }
        // At each level we choose the dimension to split, in a circular fashion
        uint32_t dimension = depth % pointDimensions;
        // Sort the points by the dimension
        std::sort(start, end,
                  [&](const std::shared_ptr<Point> &a, const std::shared_ptr<Point> & b) {
                      return a->at(dimension) < b->at(dimension);
                  });
        // Pick the median point for the split
        // Data partitioning kd-tree: pick median
        // Space partitioning kd-tree: (max-min) / 2
        uint32_t medianIdx = pointsSize / 2;
        // Create a split node (no data is passed, only the split value)
        auto node = std::make_shared<KDNode>(points[medianIdx]->at(dimension));
        // Assign the values bigger and smaller than the median point to the respective arrays
        // Recursive call to the right and left children, pass increased depth and values
        node->left = buildTree(start, start + medianIdx, depth + 1);
        node->right = buildTree(start + medianIdx, end, depth + 1);
        return node;
    }

    std::shared_ptr<KDNode> KDTree::getRoot() {
        return root;
    }

    std::vector<std::shared_ptr<KDNode>> KDTree::getLeaves() {
        return leaves;
    }

}