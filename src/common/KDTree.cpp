#include "include/common/KDTree.h"

namespace common {

    KDTree::KDTree(std::vector<Point> &points) {
        // Construct the tree from the given points and store the root
        root = buildTree(points, 0);
    }

    std::shared_ptr<KDNode> KDTree::buildTree(std::vector<Point> points, int depth){
        // Recursive implementation of kdTree construction from a set of multidimensional points
        if (points.empty()) {
            return nullptr;
        }
        // When we reach the desired leaf size (which is the number of rows per parquet partition)
        // We should store the node and exit the recursion
        if (points.size() == 2){
            auto node = std::make_shared<KDNode>(points);
            leaves.emplace_back(node);
            return node;
        }
        // At each level we choose the dimension to split, in a circular fashion
        int dimension = depth % points[0].size();
        // Sort the points by the dimension
        std::sort(points.begin(), points.end(),
                  [&](const Point & a, const Point & b) {
                      return a[dimension] < b[dimension];
                  });
        // Pick the median point for the split
        int medianIdx = points.size() / 2;
        // Create a split node (no data is passed, only the split value)
        auto node = std::make_shared<KDNode>(points[medianIdx][dimension]);
        // Assign the values bigger and smaller than the median point to the respective arrays
        std::vector<Point> leftPoints(medianIdx);
        std::copy(points.begin(), points.begin() + medianIdx, leftPoints.begin());
        std::vector<Point> rightPoints(points.size() - medianIdx);
        std::copy(points.begin() + medianIdx, points.end(), rightPoints.begin());
        // Recursive call to the right and left children, pass increased depth and values
        node->left = buildTree(leftPoints, depth + 1);
        node->right = buildTree(rightPoints, depth + 1);
        return node;
    }

    std::shared_ptr<KDNode> KDTree::getRoot() {
        return root;
    }

    std::vector<std::shared_ptr<KDNode>> KDTree::getLeaves() {
        return leaves;
    }

}