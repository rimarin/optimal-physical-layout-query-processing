#ifndef COMMON_POINT_H
#define COMMON_POINT_H

#include <vector>

namespace common {
    // Alias for the points/rows used in the partitioner
    // Originally we are dealing with column-oriented storage, so a conversion is always needed
    // Moreover, we need to unify data types
    // Double is quite extensive
    using Point = std::vector<double>;
}

#endif //COMMON_POINT_H
