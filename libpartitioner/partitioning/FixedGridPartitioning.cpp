#include <algorithm>
#include <numeric>

#include "partitioning/FixedGridPartitioning.h"

namespace partitioning {

    arrow::Status FixedGridPartitioning::partition() {

        // Number of cells is = (domain(x) / cellSize) * (domain(y) / cellSize) ... * (domain(n) / cellSize)
        // e.g. 20x20 grid, 100x100 coordinates -> (100 / 20) * (100 / 20) = 5 * 5 = 25 squares
        std::cout << "[FixedGridPartitioning] Analyzing span of column values to determine cell width" << std::endl;
        uint32_t columnDomainAverage = 0;
        for (int j = 0; j < numColumns; ++j) {
            std::pair<double_t, double_t> columnStats = dataReader->getColumnStats(columns[j]).ValueOrDie();
            auto columnDomain = (columnStats.second - columnStats.first) * 1.1;
            columnToDomain[j] = columnDomain;
            columnDomainAverage += columnDomain;
        }
        columnDomainAverage /= numColumns;
        std::cout << "[FixedGridPartitioning] Average of the columns domain is: " << columnDomainAverage << std::endl;
        cellWidth = columnDomainAverage / 10;
        std::cout << "[FixedGridPartitioning] Computed cell width is: " << cellWidth << std::endl;

        auto batch_reader = dataReader->getTableBatchReader().ValueOrDie();
        uint32_t batchId = 0;
        uint32_t totalNumRows = 0;
        while (true) {
            std::shared_ptr<arrow::RecordBatch> record_batch;
            ARROW_RETURN_NOT_OK(batch_reader->ReadNext(&record_batch));
            if (record_batch == nullptr) {
                break;
            }
            totalNumRows += record_batch->num_rows();
            ARROW_RETURN_NOT_OK(partitionBatch(batchId, record_batch, dataReader));
            std::cout << "[FixedGridPartitioning] Batch " << batchId << " completed" << std::endl;
            std::cout << "[FixedGridPartitioning] Imported " << totalNumRows << " out of " << numRows << " rows" << std::endl;
            batchId += 1;
        }
        std::cout << "[FixedGridPartitioning] Partitioning of " << batchId << " batches completed" << std::endl;
        ARROW_RETURN_NOT_OK(storage::DataWriter::mergeBatches(folder, uniquePartitionIds));
        return arrow::Status::OK();
    }

    arrow::Status FixedGridPartitioning::partitionBatch(const uint32_t &batchId,
                                                        std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                                        std::shared_ptr<storage::DataReader> &dataReader) {
        partitionIds = {};
        std::vector<std::shared_ptr<arrow::Array>> batchColumns;
        batchColumns.reserve(columns.size());
        for (const auto &columnName: columns){
            batchColumns.emplace_back(recordBatch->column(dataReader->getColumnIndex(columnName).ValueOrDie()));
        }

        size_t batchNumRows = batchColumns[0]->length();

        std::vector<uint32_t> cellIndexes = {};
        for (int i = 0; i < batchNumRows; ++i){
            uint32_t cellIndex = 0;
            for (int j = 0; j < numColumns; ++j){
                auto dimensionNumCells = (uint32_t) std::floor(columnToDomain[j] / cellWidth);
                auto arrow_int32_array = std::static_pointer_cast<arrow::Int32Array>(batchColumns[j]);
                auto value = arrow_int32_array->Value(i);
                auto cellDimensionIndex = (uint32_t) std::floor(value / cellWidth);
                if (j > 0){
                    cellIndex += cellDimensionIndex * dimensionNumCells;
                }
                else{
                    cellIndex += cellDimensionIndex;
                }
            }
            cellIndexes.emplace_back(cellIndex);
        }

        batchColumns.clear();

        std::vector<size_t> idx(cellIndexes.size());
        iota(idx.begin(), idx.end(), 0);

        std::stable_sort(idx.begin(), idx.end(),
                         [&cellIndexes](size_t i1, size_t i2) {return cellIndexes[i1] < cellIndexes[i2];});

        std::map<uint32_t, uint32_t> cellIndexToPartition;
        std::sort(std::begin(cellIndexes), std::end(cellIndexes));
        auto batchCapacity = cellCapacity / expectedNumBatches;
        if (batchCapacity > 0){
            std::cout << "[FixedGridPartitioning] In order to fit " << cellCapacity << " elements per cell, "
                                                  "each batch has a capacity of " << batchCapacity;
        }
        else{
            std::cout << "[FixedGridPartitioning] Impossible to follow the partition size constraint: too many batches "
                         "for this cell capacity / partition size";
            batchCapacity = 1000;
        }
        for (int i = 0; i < idx.size(); ++i) {
            cellIndexToPartition[idx[i]] = i / batchCapacity;
        }
        cellIndexes.clear();
        for (int i = 0; i < batchNumRows; ++i){
            partitionIds.emplace_back(cellIndexToPartition[i]);
        }
        cellIndexToPartition.clear();
        arrow::UInt32Builder int32Builder;
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(partitionIds));
        std::cout << "[FixedGridPartitioning] Mapped columns to partition ids" << std::endl;
        std::shared_ptr<arrow::Array> partitionIdsArrow;
        ARROW_ASSIGN_OR_RAISE(partitionIdsArrow, int32Builder.Finish());
        std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
        ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "partition_id", partitionIdsArrow));
        uniquePartitionIds.merge(std::set(partitionIds.begin(), partitionIds.end()));
        auto numPartitions = uniquePartitionIds.size();
        std::cout << "[FixedGridPartitioning] Computed " << numPartitions << " unique partition ids" << std::endl;

        uint64_t partitionedTablesNumRows = 0;
        uint32_t completedPartitions = 0;
        for (const auto &partitionId: uniquePartitionIds){
            auto writeTable = arrow::Table::FromRecordBatches({updatedRecordBatch}).ValueOrDie();
            std::shared_ptr<arrow::dataset::Dataset> dataset = std::make_shared<arrow::dataset::InMemoryDataset>(writeTable);
            auto options = std::make_shared<arrow::dataset::ScanOptions>();
            options->filter = arrow::compute::equal(
                    arrow::compute::field_ref("partition_id"),
                    arrow::compute::literal(partitionId));
            auto builder = arrow::dataset::ScannerBuilder(dataset, options);
            auto scanner = builder.Finish();
            std::shared_ptr<arrow::Table> partitionedTable = scanner.ValueOrDie()->ToTable().ValueOrDie();
            partitionedTablesNumRows += partitionedTable->num_rows();
            std::filesystem::path subPartitionsFolder = folder / std::to_string(partitionId);
            if (!std::filesystem::exists(subPartitionsFolder)) {
                std::filesystem::create_directory(subPartitionsFolder);
            }
            if (!addColumnPartitionId){
                ARROW_ASSIGN_OR_RAISE(partitionedTable, partitionedTable->RemoveColumn(0));
            }
            std::filesystem::path outfile = subPartitionsFolder / ("b" + std::to_string(batchId) + ".parquet");
            ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTable(partitionedTable, outfile));
            completedPartitions += 1;
            std::cout << "[FixedGridPartitioning] Generate partitioned table with " << partitionedTable->num_rows() << " rows" << std::endl;
            int progress = (int) (float(completedPartitions) / float(numPartitions) * 100);
            std::cout << "[FixedGridPartitioning] Progress: " << progress << " %" << std::endl;
        }
        return arrow::Status::OK();
    }

}