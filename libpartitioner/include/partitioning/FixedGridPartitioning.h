#ifndef PARTITIONING_FIXED_GRID_H
#define PARTITIONING_FIXED_GRID_H

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
#include "storage/DataReader.h"
#include "storage/DataWriter.h"


namespace partitioning {

    class FixedGridPartitioning : public MultiDimensionalPartitioning {
    public:
        FixedGridPartitioning(const std::shared_ptr<storage::DataReader> &reader,
                              const std::vector<std::string> &partitionColumns,
                              const size_t rowsPerPartition,
                              const std::filesystem::path &outputFolder) :
                              MultiDimensionalPartitioning(reader, partitionColumns, rowsPerPartition, outputFolder) {
            cellWidth = 0;
            cellCapacity = partitionSize;
        };
        arrow::Status partition() override;
        arrow::Status partitionBatch(const uint32_t &batchId,
                                     std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                     std::shared_ptr<storage::DataReader> &dataReader);
    private:
        partitioning::PartitioningType type = GRID;
        size_t cellCapacity;
        uint64_t cellWidth;
        std::unordered_map<uint8_t, uint64_t> columnToDomain;
        std::vector<uint32_t> partitionIds;
        std::set<uint32_t> uniquePartitionIds;
    };
}

#endif //PARTITIONING_FIXED_GRID_H
