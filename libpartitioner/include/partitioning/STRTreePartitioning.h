#ifndef PARTITIONING_STR_TREE_H
#define PARTITIONING_STR_TREE_H

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

#include "common/ColumnDataConverter.h"
#include "common/Point.h"
#include "external/ExternalSort.h"
#include "external/ExternalMerge.h"
#include "partitioning/Partitioning.h"
#include "storage/DataWriter.h"
#include "storage/DataReader.h"

namespace partitioning {

    class STRTreePartitioning : public MultiDimensionalPartitioning {
    public:
        STRTreePartitioning(const std::shared_ptr<storage::DataReader> &reader,
                            const std::vector<std::string> &partitionColumns,
                            const size_t rowsPerPartition,
                            const std::filesystem::path &outputFolder) :
                MultiDimensionalPartitioning(reader, partitionColumns, rowsPerPartition, outputFolder) {
            // k: number of dimensions
            k = numColumns;
            // r: number of points
            r = numRows;
            // n: number of points in a node (partition)
            n = partitionSize;
            // P: number of leaf-level pages
            P = std::max(r / n, (size_t) 1);
            // S: number of vertical slices
            S = ceil(sqrt(P));
        };
        arrow::Status partition() override;
    private:
        PartitioningType type = TREE;
        size_t k;
        size_t n;
        size_t r;
        size_t P;
        size_t S;
    };
}

#endif //PARTITIONING_STR_TREE_H
