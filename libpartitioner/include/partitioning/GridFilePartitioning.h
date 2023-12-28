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
#include "storage/DataWriter.h"
#include "storage/DataReader.h"

namespace partitioning {

    class GridFilePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(storage::DataReader &dataReader,
                                const std::vector<std::string> &partitionColumns,
                                const size_t partitionSize,
                                const std::filesystem::path &outputFolder) override;
    private:
        void computeLinearScales(std::vector<std::shared_ptr<common::Point>> &allRows, uint32_t initialCoord);
        std::vector<std::vector<double>> linearScales;
        size_t cellCapacity;
        std::vector<std::string> columns;
        size_t numColumns;
        std::filesystem::path folder;
        std::vector<std::pair<uint32_t, uint32_t>> rowIndexToPartitionId;
        bool addColumnPartitionId = true;
    };
}

#endif //PARTITIONING_GRID_FILE_H
