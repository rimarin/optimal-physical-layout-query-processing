#include "../include/partitioning/NoPartitioning.h"

namespace partitioning {

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>>
    NoPartitioning::partition(std::shared_ptr<arrow::Table> table, std::vector<std::string> partitionColumns,
                              int32_t partitionSize) {
        return std::vector<std::shared_ptr<arrow::Table>> {table};
    }

}