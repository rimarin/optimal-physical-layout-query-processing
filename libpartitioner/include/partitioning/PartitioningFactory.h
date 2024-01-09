#ifndef PARTITIONING_FACTORY_H
#define PARTITIONING_FACTORY_H

#include <string>

#include "partitioning/FixedGridPartitioning.h"
#include "partitioning/GridFilePartitioning.h"
#include "partitioning/HilbertCurvePartitioning.h"
#include "partitioning/KDTreePartitioning.h"
#include "partitioning/NoPartitioning.h"
#include "partitioning/QuadTreePartitioning.h"
#include "partitioning/STRTreePartitioning.h"
#include "partitioning/ZOrderCurvePartitioning.h"

namespace partitioning{

    enum PartitioningScheme{
        NO_PARTITION = 1,
        FIXED_GRID = 2,
        GRID_FILE = 3,
        KD_TREE = 4,
        STR_TREE = 5,
        QUAD_TREE = 6,
        HILBERT_CURVE = 7,
        Z_ORDER_CURVE = 8
    };

    std::map<std::string, PartitioningScheme> mapNameToScheme = {
            {"no-partition", NO_PARTITION},
            {"fixed-grid", FIXED_GRID},
            {"grid-file", GRID_FILE},
            {"kd-tree", KD_TREE},
            {"str-tree", STR_TREE},
            {"quad-tree", QUAD_TREE},
            {"hilbert-curve", HILBERT_CURVE},
            {"z-order-curve", Z_ORDER_CURVE},
    };

    const std::map<PartitioningScheme, std::string> mapSchemeToName = {
            {NO_PARTITION, "no-partition"},
            {FIXED_GRID, "fixed-grid"},
            {GRID_FILE, "grid-file"},
            {KD_TREE, "kd-tree"},
            {STR_TREE, "str-tree"},
            {QUAD_TREE, "quad-tree"},
            {HILBERT_CURVE, "hilbert-curve"},
            {Z_ORDER_CURVE, "z-order-curve"},
    };

    class PartitioningFactory{
        public:
            static std::shared_ptr<MultiDimensionalPartitioning> create(
                    PartitioningScheme const& partitioningScheme,
                    const std::shared_ptr<storage::DataReader> &reader,
                    const std::vector<std::string> &partitionColumns,
                    const size_t rowsPerPartition,
                    const std::filesystem::path &outputFolder) {
                switch (partitioningScheme) {
                    case NO_PARTITION:
                        return std::make_shared<NoPartitioning>(reader, partitionColumns, rowsPerPartition, outputFolder);
                    case FIXED_GRID:
                        return std::make_shared<FixedGridPartitioning>(reader, partitionColumns, rowsPerPartition, outputFolder);
                    case GRID_FILE:
                        return std::make_shared<GridFilePartitioning>(reader, partitionColumns, rowsPerPartition, outputFolder);
                    case KD_TREE:
                        return std::make_shared<KDTreePartitioning>(reader, partitionColumns, rowsPerPartition, outputFolder);
                    case STR_TREE:
                        return std::make_shared<STRTreePartitioning>(reader, partitionColumns, rowsPerPartition, outputFolder);
                    case QUAD_TREE:
                        return std::make_shared<QuadTreePartitioning>(reader, partitionColumns, rowsPerPartition, outputFolder);
                    case HILBERT_CURVE:
                        return std::make_shared<HilbertCurvePartitioning>(reader, partitionColumns, rowsPerPartition, outputFolder);
                    case Z_ORDER_CURVE:
                        return std::make_shared<ZOrderCurvePartitioning>(reader, partitionColumns, rowsPerPartition, outputFolder);
                    default:
                        return nullptr;
                }
            }
    };
}

#endif //PARTITIONING_FACTORY_H

