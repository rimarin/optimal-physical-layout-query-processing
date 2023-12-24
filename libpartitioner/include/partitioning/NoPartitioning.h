#ifndef PARTITIONING_NO_PARTITIONING_H
#define PARTITIONING_NO_PARTITIONING_H

#include "Partitioning.h"


namespace partitioning {

    class NoPartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(storage::DataReader &dataReader,
                                const std::vector<std::string> &partitionColumns,
                                const size_t partitionSize,
                                const std::filesystem::path &outputFolder) override;
    };
}

#endif //PARTITIONING_NO_PARTITIONING_H
