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
#include "../storage/DataWriter.h"
#include "../storage/DataReader.h"


namespace partitioning {

    class STRTreePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table,
                                                                            std::vector<std::string> partitionColumns,
                                                                            int32_t partitionSize);
    };
}

#endif //PARTITIONING_STRTREE_H
