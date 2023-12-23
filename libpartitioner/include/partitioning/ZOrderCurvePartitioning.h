#ifndef PARTITIONING_ZORDERCURVE_H
#define PARTITIONING_ZORDERCURVE_H

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
#include "common/ZOrderCurve.h"
#include "partitioning/Partitioning.h"
#include "storage/DataReader.h"
#include "storage/DataWriter.h"


namespace partitioning {

    class ZOrderCurvePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(std::shared_ptr<arrow::Table> table,
                                std::vector<std::string> partitionColumns,
                                int32_t partitionSize,
                                std::filesystem::path &outputFolder) override;
    };
}

#endif //PARTITIONING_ZORDERCURVE_H
