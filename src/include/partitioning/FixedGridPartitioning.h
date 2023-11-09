#ifndef PARTITIONING_FIXEDGRID_H
#define PARTITIONING_FIXEDGRID_H

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

#include "../common/ColumnDataConverter.h"
#include "../storage/DataWriter.h"
#include "../storage/DataReader.h"
#include "Partitioning.h"


namespace partitioning {

    class FixedGridPartitioning : public MultiDimensionalPartitioning {
    public:
        FixedGridPartitioning(std::vector<std::string> partitionColumns);
        virtual ~FixedGridPartitioning() = default;
        arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table,
                                                                            int32_t partitionSize);
        arrow::Result<arrow::Datum> columnsToPartitionId(std::vector<std::shared_ptr<arrow::Array>> &columnArrowArrays, int32_t &partitionSize);
    private:
        std::vector<std::string> columns;
    };
}

#endif //PARTITIONING_FIXEDGRID_H
