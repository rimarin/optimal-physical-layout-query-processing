#ifndef PARTITIONING_QUADTREEPARTITIONING_H
#define PARTITIONING_QUADTREEPARTITIONING_H

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
#include "../common/QuadTree.h"

namespace partitioning {

    class QuadTreePartitioning : public MultiDimensionalPartitioning {
    public:
        QuadTreePartitioning(std::vector<std::string> partitionColumns);
        virtual ~QuadTreePartitioning() = default;
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table);
    private:
        std::vector<std::string> columns;
        static arrow::Status ColumnsToPartitionId(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                                                  arrow::compute::ExecResult* out);
    };
}

#endif //PARTITIONING_QUADTREEPARTITIONING_H
