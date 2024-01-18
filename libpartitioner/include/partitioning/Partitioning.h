#ifndef PARTITIONING_PARTITIONING_H
#define PARTITIONING_PARTITIONING_H

#include <iostream>
#include <filesystem>
#include <set>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/type_fwd.h>
#include <parquet/arrow/writer.h>

#include "partitioning/PartitioningType.h"
#include "storage/DataReader.h"

namespace partitioning {

    class MultiDimensionalPartitioning {
    public:
        MultiDimensionalPartitioning(const std::shared_ptr<storage::DataReader> &reader,
                                     const std::vector<std::string> &partitionColumns,
                                     size_t rowsPerPartition,
                                     const std::filesystem::path &outputFolder);
        virtual ~MultiDimensionalPartitioning() = default;
        virtual arrow::Status partition() = 0;
        static arrow::Status writeOutPartitions(std::shared_ptr<arrow::Table> &table,
                                                std::shared_ptr<arrow::Array> &partitionIds,
                                                const std::filesystem::path &outputFolder);
        void setColumns(const std::vector<std::string> &partitionColumns);
        void setDataReader(const std::shared_ptr<storage::DataReader> &reader);
        void setPartitionSize(size_t rowsPerPartition);
    private:
        bool checkNoNecessaryPartition();
        partitioning::PartitioningType type = OTHER;
    protected:
        std::shared_ptr<storage::DataReader> dataReader;
        std::vector<std::string> columns;
        size_t partitionSize;
        size_t numColumns;
        size_t numRows;
        std::filesystem::path folder;
        uint32_t expectedNumBatches;
        bool addColumnPartitionId = true;
        std::string fileExtension = common::Settings::fileExtension;
        bool finished = false;
        const uint32_t minNumberOfColumns = 2;
        const uint32_t minPartitionSize = 1;
    };
}

#endif //PARTITIONING_PARTITIONING_H
