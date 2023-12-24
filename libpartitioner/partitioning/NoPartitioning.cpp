#include "partitioning/NoPartitioning.h"

namespace partitioning {

    arrow::Status NoPartitioning::partition(storage::DataReader &dataReader,
                                            const std::vector<std::string> &partitionColumns,
                                            const size_t partitionSize, const std::filesystem::path &outputFolder) {
        return arrow::Status::OK();
    }

}