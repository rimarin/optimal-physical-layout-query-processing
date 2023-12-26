#ifndef PARTITIONING_KD_TREE_PARTITIONING_H
#define PARTITIONING_KD_TREE_PARTITIONING_H

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <utility>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "common/ColumnDataConverter.h"
#include "common/KDTree.h"
#include "partitioning/Partitioning.h"
#include "storage/DataWriter.h"
#include "storage/DataReader.h"

namespace partitioning {

    class KDTreePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(storage::DataReader &dataReader,
                                const std::vector<std::string> &partitionColumns,
                                const size_t partitionSize,
                                const std::filesystem::path &outputFolder) override;
    };
}

#endif //PARTITIONING_KD_TREE_PARTITIONING_H
