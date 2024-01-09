#include "partitioning/NoPartitioning.h"

namespace partitioning {

    arrow::Status NoPartitioning::partition() {
        // Dummy class that does not make any change
        return arrow::Status::OK();
    }

}