#ifndef PARTITIONING_HILBERTCURVE_H
#define PARTITIONING_HILBERTCURVE_H

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
#include "common/HilbertCurve.h"
#include "partitioning/Partitioning.h"
#include "storage/DataReader.h"
#include "storage/DataWriter.h"


namespace partitioning {

    using IntRow = std::vector<int64_t>;

    class HilbertCurvePartitioning : public MultiDimensionalPartitioning {
    public:
        arrow::Status partition(std::shared_ptr<arrow::Table> table,
                                std::vector<std::string> partitionColumns,
                                int32_t partitionSize,
                                std::filesystem::path &outputFolder) override;
    };
}

#endif //PARTITIONING_HILBERTCURVE_H
