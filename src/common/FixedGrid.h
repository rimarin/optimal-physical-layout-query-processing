#ifndef COMMON_FIXEDGRID_H
#define COMMON_FIXEDGRID_H

#include <map>
#include <vector>
#include <tuple>
#include <string>
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

typedef std::pair<double, double> Point;

namespace common {

    class FixedGrid {
    public:
        FixedGrid(std::pair<std::string, std::string> partitionColumns, int size);
        void setCellSize(int cellSize);
        std::vector<std::tuple<>> mapPointsToCells(const std::vector<Point> &points);

    private:
        int cellSize;
        std::pair<std::string, std::string> columns;
    };
}

#endif //COMMON_FIXEDGRID_H
