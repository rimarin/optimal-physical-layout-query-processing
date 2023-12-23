#ifndef PARTITIONING_GRIDFILE_H
#define PARTITIONING_GRIDFILE_H

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
#include "partitioning/Partitioning.h"
#include "storage/DataWriter.h"
#include "storage/DataReader.h"

namespace partitioning {

    class GridFilePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(std::shared_ptr<arrow::Table> table,
                                std::vector<std::string> partitionColumns,
                                int32_t partitionSize,
                                std::filesystem::path &outputFolder) override;
    private:
        void packSlicesRecursive(std::vector<common::Point> points, int coord);
        std::vector<std::vector<common::Point>> slices = {};
        std::vector<int> columnIndexes;
        int k;
        int n;
        int r;
        int P;
        int S;
    };
}

#endif //PARTITIONING_GRIDFILE_H
