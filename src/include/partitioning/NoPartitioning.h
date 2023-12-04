#ifndef PARTITIONING_NO_PARTITIONING_H
#define PARTITIONING_NO_PARTITIONING_H

#include "Partitioning.h"


namespace partitioning {

    class NoPartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(std::shared_ptr<arrow::Table> table,
                                std::vector<std::string> partitionColumns,
                                int32_t partitionSize,
                                std::filesystem::path &outputFolder) override;
    };
}

#endif //PARTITIONING_NO_PARTITIONING_H
