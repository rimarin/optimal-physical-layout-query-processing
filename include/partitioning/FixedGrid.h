#ifndef PARTITIONING_FIXEDGRID_H
#define PARTITIONING_FIXEDGRID_H

#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <set>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/dataset_writer.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/partition.h>
#include <arrow/dataset/scanner.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

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
        static arrow::Status PointsToCell(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                   arrow::compute::ExecResult* out);
    };
}

#endif //PARTITIONING_FIXEDGRID_H
