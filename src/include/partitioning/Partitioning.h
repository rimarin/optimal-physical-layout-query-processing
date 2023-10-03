#ifndef PARTITIONING_PARTITIONING_H
#define PARTITIONING_PARTITIONING_H

#include <arrow/api.h>


namespace partitioning {

    class MultiDimensionalPartitioning {
    public:
        MultiDimensionalPartitioning() = default;
        virtual ~MultiDimensionalPartitioning() = default;
        virtual arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table) = 0;
    };
}

#endif //PARTITIONING_PARTITIONING_H
