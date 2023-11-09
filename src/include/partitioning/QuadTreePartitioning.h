#ifndef PARTITIONING_QUADTREEPARTITIONING_H
#define PARTITIONING_QUADTREEPARTITIONING_H

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

#include "Partitioning.h"
#include "../storage/DataReader.h"
#include "../storage/DataWriter.h"
#include "../common/ColumnDataConverter.h"
#include "../common/QuadTree.h"


namespace partitioning {

    class QuadTreePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table,
                                                                            std::vector<std::string> partitionColumns,
                                                                            int32_t partitionSize) override;
    };
}

#endif //PARTITIONING_QUADTREEPARTITIONING_H
