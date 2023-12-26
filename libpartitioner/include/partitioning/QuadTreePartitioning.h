#ifndef PARTITIONING_QUAD_TREE_PARTITIONING_H
#define PARTITIONING_QUAD_TREE_PARTITIONING_H

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <set>
#include <utility>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "common/ColumnDataConverter.h"
#include "common/QuadTree.h"
#include "storage/DataReader.h"
#include "storage/DataWriter.h"
#include "partitioning/Partitioning.h"


namespace partitioning {

    class QuadTreePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(storage::DataReader &dataReader,
                                const std::vector<std::string> &partitionColumns,
                                const size_t partitionSize,
                                const std::filesystem::path &outputFolder) override;
    };
}

#endif //PARTITIONING_QUAD_TREE_PARTITIONING_H
