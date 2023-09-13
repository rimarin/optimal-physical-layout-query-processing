#ifndef OPLQP_FIXEDGRID_H
#define OPLQP_FIXEDGRID_H

#include <map>
#include <vector>

typedef std::pair<double, double> Point;


class FixedGrid {
    public: FixedGrid();
    public: std::vector<Point> mapPointsToCells(const std::vector<Point>& points);
    private: int cellSize;
};


#endif //OPLQP_FIXEDGRID_H
