#ifndef PARTITIONING_PARTITIONING_H
#define PARTITIONING_PARTITIONING_H

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/compute/api.h>
#include <arrow/util/type_fwd.h>
#include <iostream>
#include <filesystem>
#include <parquet/arrow/writer.h>
#include <regex>
#include <set>

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
        bool isFinished();
        // DuckDB config
        static inline const std::string memoryLimit = "450GB";
        static inline const std::string tempDirectory = "/tmp";
    private:
        bool canSkipPartitioning();
        partitioning::PartitioningType type = OTHER;
    protected:
        arrow::Status copyOriginalToDestination();
        bool isFileCompleted(const std::filesystem::path &partitionFile);
        void deleteIntermediateFiles();
        std::set<std::filesystem::path> getCompletedFiles();
        void moveCompletedFiles();
        void deleteSubfolders();
        std::shared_ptr<storage::DataReader> dataReader;
        std::shared_ptr<::arrow::RecordBatchReader> batchReader;
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
