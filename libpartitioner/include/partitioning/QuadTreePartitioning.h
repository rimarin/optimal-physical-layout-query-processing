#ifndef PARTITIONING_QUAD_TREE_PARTITIONING_H
#define PARTITIONING_QUAD_TREE_PARTITIONING_H

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <set>
#include <utility>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "common/ColumnDataConverter.h"
#include "partitioning/Partitioning.h"
#include "partitioning/PartitioningType.h"
#include "structures/QuadTree.h"
#include "storage/DataReader.h"
#include "storage/DataWriter.h"


namespace partitioning {

    class QuadTreePartitioning : public MultiDimensionalPartitioning {
    public:
        QuadTreePartitioning(const std::shared_ptr<storage::DataReader> &reader,
                             const std::vector<std::string> &partitionColumns,
                             const size_t rowsPerPartition,
                             const std::filesystem::path &outputFolder) :
                MultiDimensionalPartitioning(reader, partitionColumns, rowsPerPartition, outputFolder) {
        };
        arrow::Status partition() override;
    private:
        partitioning::PartitioningType type = TREE;
    };
}

#endif //PARTITIONING_QUAD_TREE_PARTITIONING_H
