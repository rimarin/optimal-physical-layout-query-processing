#include "FixedGrid.h"

namespace common {

FixedGrid::FixedGrid(std::pair<std::string, std::string> partitionColumns, int size) {
    columns = partitionColumns;
    setCellSize(size);
}

void FixedGrid::setCellSize(int size){
    cellSize = size;
}

std::vector<std::tuple<>> FixedGrid::mapPointsToCells(const std::vector<Point>& points) {
    std::map<Point, std::vector<Point>> cellMap;

    for (const Point& point : points) {
        double x = point.first;
        double y = point.second;
        int gridX = static_cast<int>(x / cellSize);
        int gridY = static_cast<int>(y / cellSize);
        std::pair<int, int> cellCoordinates = std::make_pair(gridX, gridY);

        cellMap[cellCoordinates].push_back(point);
    }

    std::vector<std::tuple<anything>> cellsList = {};
    for (const auto &s : cellMap)
        cellsList.push_back(s.second);

    return cellsList;
}

}