#ifndef PARTITIONING_FIXEDGRID_H
#define PARTITIONING_FIXEDGRID_H

#include <map>
#include <vector>
#include <tuple>
#include <string>
#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include "../../include/partitioning/Partitioning.h"

typedef std::pair<double, double> Point;

namespace partitioning {

    class FixedGrid : public MultiDimensionalPartitioning {
    public:
        FixedGrid(std::vector<std::string> partitionColumns, int size);
        virtual ~FixedGrid() = default;
        void setCellSize(int cellSize);
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table);
    private:
        int cellSize;
        std::vector<std::string> columns;
    };
}

#endif //PARTITIONING_FIXEDGRID_H
