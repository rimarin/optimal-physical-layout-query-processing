#ifndef PARTITIONING_STR_TREE_H
#define PARTITIONING_STR_TREE_H

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

#include "common/ColumnDataConverter.h"
#include "common/Point.h"
#include "partitioning/Partitioning.h"
#include "storage/DataWriter.h"
#include "storage/DataReader.h"

namespace partitioning {

    class STRTreePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(storage::DataReader &dataReader,
                                const std::vector<std::string> &partitionColumns,
                                const size_t partitionSize,
                                const std::filesystem::path &outputFolder) override;
    private:
        void sortTileRecursive(std::vector<std::shared_ptr<common::Point>> points, int coord);
        std::vector<std::vector<std::shared_ptr<common::Point>>> slices = {};
        int k;
        int n;
        int r;
        int P;
        int S;
    };
}

#endif //PARTITIONING_STR_TREE_H
