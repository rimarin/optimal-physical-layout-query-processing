#include "common/QuadTree.h"

namespace common {

    QuadTree::QuadTree(std::vector<std::shared_ptr<Point>> &rows, size_t partitionSize) {
        // Construct the tree from the given points and store the root
        leafSize = partitionSize;
        std::cout << "[QuadTree] Start building a Quad Tree for " << rows.size() << " points and partition size " << partitionSize << std::endl;
        points = rows;
        root = buildTree(points, 0);
    }

    std::shared_ptr<QuadNode> QuadTree::buildTree(std::vector<std::shared_ptr<Point>> &points, uint32_t depth){
        // Recursive implementation of QuadTree construction from a set of multidimensional points
        if (points.empty()) {
            return nullptr;
        }
        // When we reach the desired leaf size (which is the number of rows per parquet partition)
        // We should store the node and exit the recursion
        if (points.size() <= leafSize){
            auto node = std::make_shared<QuadNode>(points);
            leaves.emplace_back(node);
            return node;
        }
        // Compute range x, y for the set of points
        std::vector<double> pointsX;
        std::vector<double> pointsY;
        for (auto &point: points){
            pointsX.emplace_back(point->at(0));
            pointsY.emplace_back(point->at(1));
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
        std::vector<std::shared_ptr<Point>> northWestPoints;
        std::vector<std::shared_ptr<Point>> northEastPoints;
        std::vector<std::shared_ptr<Point>> southWestPoints;
        std::vector<std::shared_ptr<Point>> southEastPoints;
        for (auto &point: points){
            auto x = point->at(0);
            auto y = point->at(1);
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