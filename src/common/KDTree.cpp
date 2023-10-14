#include "include/common/KDTree.h"

namespace common {

    KDTree::KDTree(std::vector<Point> &points) {
        root = buildTree(points, 0);
        auto right = root->right;
        auto left = root->left;
    }

    std::shared_ptr<KDNode> KDTree::buildTree(std::vector<Point> points, int depth){
        if (points.empty()) {
            return nullptr;
        }
        if (points.size() == 2){
            auto node = std::make_shared<KDNode>(points);
            leaves.emplace_back(node);
            return node;
        }
        int axis = depth % points.size();
        std::sort(points.begin(), points.end(),
                  [&](const Point & a, const Point & b) {
                      return a[axis] < b[axis];
                  });
        int medianIdx = points.size() / 2;
        auto node = std::make_shared<KDNode>(points[medianIdx][axis]);
        std::vector<Point> leftPts(medianIdx);
        std::copy(points.begin(), points.begin() + medianIdx, leftPts.begin());
        std::vector<Point> rightPts(points.size() - medianIdx);
        std::copy(points.begin() + medianIdx, points.end(), rightPts.begin());
        node->left = buildTree(leftPts, depth + 1);
        node->right = buildTree(rightPts, depth + 1);
        return node;
    }

    std::shared_ptr<KDNode> KDTree::getRoot() {
        return root;
    }

    std::vector<std::shared_ptr<KDNode>> KDTree::getLeaves(std::shared_ptr<KDNode> node) {
        return leaves;
    }

}