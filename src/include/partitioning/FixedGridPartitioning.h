#ifndef PARTITIONING_FIXEDGRID_H
#define PARTITIONING_FIXEDGRID_H

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <set>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "Partitioning.h"
#include "../storage/DataWriter.h"

typedef std::pair<double, double> Point;

namespace partitioning {

    class FixedGridPartitioning : public MultiDimensionalPartitioning {
    public:
        FixedGridPartitioning(std::vector<std::string> partitionColumns, int size);
        virtual ~FixedGridPartitioning() = default;
        void setCellSize(int cellSize);
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table);
    private:
        int cellSize;
        std::vector<std::string> columns;
        static arrow::Status ColumnsToPartitionId(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                                  arrow::compute::ExecResult* out);
    };
}

#endif //PARTITIONING_FIXEDGRID_H
