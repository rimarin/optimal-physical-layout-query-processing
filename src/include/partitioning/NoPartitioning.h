#ifndef PARTITIONING_NO_PARTITIONING_H
#define PARTITIONING_NO_PARTITIONING_H

#include "Partitioning.h"

namespace partitioning {

    class NoPartitioning : public MultiDimensionalPartitioning {
    public:
        NoPartitioning();
        ~NoPartitioning() override = default;
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table) override;
    };
}

#endif //PARTITIONING_NO_PARTITIONING_H
