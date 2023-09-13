#include "FixedGrid.h"

std::vector<Point> FixedGrid::mapPointsToCells(const std::vector<Point>& points) {
    std::map<std::pair<int, int>, std::vector<Point>> cellMap;

    for (const Point& point : points) {
        double x = point.first;
        double y = point.second;
        int gridX = static_cast<int>(x / cellSize);
        int gridY = static_cast<int>(y / cellSize);
        std::pair<int, int> cellCoordinates = std::make_pair(gridX, gridY);

        cellMap[cellCoordinates].push_back(point);
    }

    return cellMap;
}
