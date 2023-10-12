#ifndef PARTITIONING_KDTREE_H
#define PARTITIONING_KDTREE_H

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <set>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "Partitioning.h"
#include "../storage/DataWriter.h"

namespace partitioning {

    class KDTree : public MultiDimensionalPartitioning {
    public:
        KDTree(std::vector<std::string> partitionColumns);
        virtual ~KDTree() = default;
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table);
    private:
        std::vector<std::string> columns;
    };
}

#endif //PARTITIONING_KDTREE_H
