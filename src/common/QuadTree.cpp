#include "../include/common/QuadTree.h"

namespace common {

    QuadTree::QuadTree(std::vector<Point> &points) {
        // Construct the tree from the given points and store the root
        root = buildTree(points, 0);
    }

    std::shared_ptr<QuadNode> QuadTree::buildTree(std::vector<Point> points, int depth){
        // Recursive implementation of QuadTree construction from a set of multidimensional points
        if (points.empty()) {
            return nullptr;
        }
        // When we reach the desired leaf size (which is the number of rows per parquet partition)
        // We should store the node and exit the recursion
        // TODO: parametrize according to rows per partition or partition size
        if (points.size() <= 2){
            auto node = std::make_shared<QuadNode>(points);
            leaves.emplace_back(node);
            return node;
        }
        // Compute range x, y for the set of points
        std::vector<double> pointsX;
        std::vector<double> pointsY;
        for (auto &point: points){
            pointsX.emplace_back(point[0]);
            pointsY.emplace_back(point[1]);
        }
        // Compute the mean point of both dimensions for the split
        auto minmaxX = std::minmax_element(pointsX.begin(), pointsX.end());
        auto minmaxY = std::minmax_element(pointsY.begin(), pointsY.end());
        double meanDimX = (*minmaxX.first + *minmaxX.second) / 2;
        double meanDimY = (*minmaxY.first + *minmaxY.second) / 2;
        auto meanDims = std::pair<double, double>(meanDimX, meanDimY);
        // Create a split node (no data is passed, only the split value)
        auto node = std::make_shared<QuadNode>(meanDims);
        // Assign the values bigger and smaller than the median point to the respective arrays
        std::vector<Point> northWestPoints;
        std::vector<Point> northEastPoints;
        std::vector<Point> southWestPoints;
        std::vector<Point> southEastPoints;
        for (auto &point: points){
            auto x = point.at(0);
            auto y = point.at(1);
            if (x < meanDimX && y > meanDimY){
                northWestPoints.emplace_back(point);
            }
            else if (x > meanDimX && y > meanDimY){
                northEastPoints.emplace_back(point);
            }
            else if (x < meanDimX && y < meanDimY){
                southWestPoints.emplace_back(point);
            }
            else if (x > meanDimX && y < meanDimY){
                southEastPoints.emplace_back(point);
            }
        }
        // Recursive call to the right and left children, pass increased depth and values
        node->northWest = buildTree(northWestPoints, depth + 1);
        node->northEast = buildTree(northEastPoints, depth + 1);
        node->southWest = buildTree(southWestPoints, depth + 1);
        node->southEast = buildTree(southEastPoints, depth + 1);
        return node;
    }

    std::shared_ptr<QuadNode> QuadTree::getRoot() {
        return root;
    }

    std::vector<std::shared_ptr<QuadNode>> QuadTree::getLeaves() {
        return leaves;
    }

}