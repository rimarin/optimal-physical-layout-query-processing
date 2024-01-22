#ifndef PARTITIONING_KD_TREE_PARTITIONING_H
#define PARTITIONING_KD_TREE_PARTITIONING_H

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

#include "common/ColumnDataConverter.h"
#include "structures/KDTree.h"
#include "partitioning/Partitioning.h"
#include "partitioning/PartitioningType.h"
#include "storage/DataWriter.h"
#include "storage/DataReader.h"

namespace partitioning {

    class KDTreePartitioning : public MultiDimensionalPartitioning {
    public:
        KDTreePartitioning(const std::shared_ptr<storage::DataReader> &reader,
                           const std::vector<std::string> &partitionColumns,
                           const size_t rowsPerPartition,
                           const std::filesystem::path &outputFolder) :
                MultiDimensionalPartitioning(reader, partitionColumns, rowsPerPartition, outputFolder) {
        };
        arrow::Status partition() override;
    private:
        partitioning::PartitioningType type = TREE;
        arrow::Status partitionBranches(std::filesystem::path &datasetFile, uint32_t depth);
        arrow::Result<double> findMedian(std::filesystem::path &datasetFile, uint32_t columnIndex);
    };
}

#endif //PARTITIONING_KD_TREE_PARTITIONING_H
