#ifndef PARTITIONING_NO_PARTITIONING_H
#define PARTITIONING_NO_PARTITIONING_H

#include "Partitioning.h"

namespace partitioning {

    class NoPartitioning : public MultiDimensionalPartitioning {
    public:
        NoPartitioning(const std::shared_ptr<storage::DataReader> &reader,
                           const std::vector<std::string> &partitionColumns,
                           const size_t rowsPerPartition,
                           const std::filesystem::path &outputFolder) :
                MultiDimensionalPartitioning(reader, partitionColumns, rowsPerPartition, outputFolder) {
        };
        arrow::Status partition() override;
    private:
        partitioning::PartitioningType type = OTHER;
    };
}

#endif //PARTITIONING_NO_PARTITIONING_H
