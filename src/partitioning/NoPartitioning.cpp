#include "../include/partitioning/NoPartitioning.h"

namespace partitioning {

    NoPartitioning::NoPartitioning() = default;

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>>
    NoPartitioning::partition(std::shared_ptr<arrow::Table> table) {
        return std::vector<std::shared_ptr<arrow::Table>> {table};
    }

}