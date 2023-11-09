#ifndef PARTITIONING_KDTREEPARTITIONING_H
#define PARTITIONING_KDTREEPARTITIONING_H

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <utility>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "Partitioning.h"
#include "../storage/DataWriter.h"
#include "../storage/DataReader.h"
#include "../common/KDTree.h"

namespace partitioning {

    class KDTreePartitioning : public MultiDimensionalPartitioning {
    public:
        explicit KDTreePartitioning(std::vector<std::string> partitionColumns);
        ~KDTreePartitioning() override = default;
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table,
                                                                            std::vector<std::string> partitionColumns,
                                                                            int32_t partitionSize);
    private:
        std::vector<std::string> columns;
        static arrow::Status ColumnsToPartitionId(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                                  arrow::compute::ExecResult* out);
    };
}

#endif //PARTITIONING_KDTREEPARTITIONING_H
