#include "partitioning/NoPartitioning.h"

namespace partitioning {

    arrow::Status NoPartitioning::partition(std::shared_ptr<arrow::Table> table, std::vector<std::string> partitionColumns,
                              int32_t partitionSize, std::filesystem::path &outputFolder) {
        return arrow::Status::OK();
    }

}