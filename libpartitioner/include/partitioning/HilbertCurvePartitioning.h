#ifndef PARTITIONING_HILBERT_CURVE_H
#define PARTITIONING_HILBERT_CURVE_H

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
#include "common/HilbertCurve.h"
#include "partitioning/Partitioning.h"
#include "storage/DataReader.h"
#include "storage/DataWriter.h"


namespace partitioning {

    using IntRow = std::vector<int64_t>;

    class HilbertCurvePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(storage::DataReader &dataReader,
                                const std::vector<std::string> &partitionColumns,
                                const size_t partitionSize,
                                const std::filesystem::path &outputFolder) override;
        arrow::Status partitionBatch(const uint32_t &batchId,
                                     std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                     storage::DataReader &dataReader);
    private:
        std::vector<std::string> columns;
        size_t numColumns;
        size_t partitionCapacity;
        std::filesystem::path folder;
        std::unordered_map<uint8_t, uint64_t> columnToDomain;
        std::vector<uint32_t> partitionIds;
        std::set<uint32_t> uniquePartitionIds;
        bool addColumnPartitionId = true;
        uint32_t expectedNumBatches;
    };
}

#endif //PARTITIONING_HILBERT_CURVE_H
