#ifndef PARTITIONING_STRTREE_H
#define PARTITIONING_STRTREE_H

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
#include "../common/ColumnDataConverter.h"
#include "../common/Point.h"
#include "../storage/DataWriter.h"
#include "../storage/DataReader.h"

namespace partitioning {

    class STRTreePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(std::shared_ptr<arrow::Table> table,
                                std::vector<std::string> partitionColumns,
                                int32_t partitionSize,
                                std::filesystem::path &outputFolder);
    private:
        void sortTileRecursive(std::vector<common::Point> points, int coord);
        std::vector<std::vector<common::Point>> slices = {};
        int k;
        int n;
        int r;
        int P;
        int S;
    };
}

#endif //PARTITIONING_STRTREE_H
