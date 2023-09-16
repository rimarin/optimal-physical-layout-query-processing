#include "../../include/partitioning/FixedGrid.h"

namespace partitioning {

FixedGrid::FixedGrid(std::vector<std::string> partitionColumns, int size) {
    columns = partitionColumns;
    setCellSize(size);
}

void FixedGrid::setCellSize(int size){
    cellSize = size;
}

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> FixedGrid::partition(std::shared_ptr<arrow::Table> table){
    std::map<Point, std::vector<arrow::Table>> cellToTables;

    // TODO: iterate over rows, pick pair of points corresponding to columns[0] and columns[1]
    //  for each pair, compute grid cell and insert row into an hashmap
    //  extract hashmap values and insert them into a vector of vector
    auto first_column = table->GetColumnByName(columns[0]);
    auto list = std::static_pointer_cast<arrow::ListArray>(first_column->chunk(0));
    int l_offset1, l_offset2, l_gap;
    for (int cur_row = 0; cur_row < table->num_rows(); ++cur_row) {
        l_offset1 = list->value_offset(cur_row);
        l_offset2 = list->value_offset(cur_row + 1);
        l_gap = l_offset2 > l_offset1 ? l_offset2 - l_offset1 : 1;
        // real_offset = real_offset + l_gap - 1;
        // auto varr = std::static_pointer_cast<arrow::Int64Array>(list->values());
        // varr->Value(real_offset);
        // real_offset += 1;
    }

    // for (const Point& point : points) {
    //    double x = point.first;
    //    double y = point.second;
    //    int gridX = static_cast<int>(x / cellSize);
    //    int gridY = static_cast<int>(y / cellSize);
    //    Point cellCoordinates = std::make_pair(gridX, gridY);
    //      cellToTables[cellCoordinates].push_back(point);
    // }

    // std::vector<std::shared_ptr<arrow::Table>> tablesList = {};
    // for (const auto &cell : cellToTables)
    //    tablesList.push_back(cell.second);

    return arrow::Status::OK();
}

}