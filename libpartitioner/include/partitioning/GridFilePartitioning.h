#ifndef PARTITIONING_GRID_FILE_H
#define PARTITIONING_GRID_FILE_H

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
#include "partitioning/Partitioning.h"
#include "partitioning/PartitioningType.h"
#include "storage/DataWriter.h"
#include "storage/DataReader.h"

namespace partitioning {

    class GridFilePartitioning : public MultiDimensionalPartitioning {
    public:
        GridFilePartitioning(const std::shared_ptr<storage::DataReader> &reader,
                             const std::vector<std::string> &partitionColumns,
                             const size_t rowsPerPartition,
                             const std::filesystem::path &outputFolder) :
                MultiDimensionalPartitioning(reader, partitionColumns, rowsPerPartition, outputFolder) {
            linearScales = {};
            cellCapacity = partitionSize;
            rowIndexToPartitionId = {};
        };
        arrow::Status partition() override;
    private:
        partitioning::PartitioningType type = GRID;
        void computeLinearScales(std::filesystem::path &partitionFile,
                                 const uint32_t depth,
                                 const std::vector<std::pair<double, double>> &dimensionRanges);
        std::vector<std::vector<double>> linearScales;
        size_t cellCapacity;
        std::vector<std::pair<uint32_t, uint32_t>> rowIndexToPartitionId;
        std::string getTimestamp(double value);
    };
}

#endif //PARTITIONING_GRID_FILE_H
