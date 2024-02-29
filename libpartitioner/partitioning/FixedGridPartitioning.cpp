#include "partitioning/FixedGridPartitioning.h"

namespace partitioning {

    arrow::Status FixedGridPartitioning::partition() {
        /* The data space is superimposed with an n-dimension grid with fixed cell size.
         * This way, all the points falling into one cell of the uniform grid are mapped to the correspondent cell id.
         * In order to force the partition size constraint, we are linearizing the obtained cells.
         * Number of cells is = (domain(x) / cellSize) * (domain(y) / cellSize) ... * (domain(n) / cellSize)
         * e.g. 20x20 grid, 100x100 coordinates -> (100 / 20) * (100 / 20) = 5 * 5 = 25 squares
         * Idea:
         * 1. Read in batches
         * 2. Compute index of cell
         * 3. Write out sorted batched by cell index
         * 4. Sort-merge the sorted batches
         */

        std::cout << "[FixedGridPartitioning] Analyzing span of column values to determine cell width" << std::endl;
        double_t maxColumnDomain = 0;
        double_t minColumnDomain = std::numeric_limits<double>::max();
        for (int j = 0; j < numColumns; ++j) {
            std::pair<double_t, double_t> columnStats = dataReader->getColumnStats(columns[j]).ValueOrDie();
            double_t domainStats = std::max(columnStats.second - columnStats.first, 1.0);
            double_t columnDomain = domainStats * 1.1;
            columnToDomain[j] = columnDomain;
            maxColumnDomain = std::max(maxColumnDomain, columnDomain);
            minColumnDomain = std::min(minColumnDomain, columnDomain);
        }
        std::cout << "[FixedGridPartitioning] Widest column domain is: " << maxColumnDomain << std::endl;
        // Cell width is 1/10 of the biggest domain.
        // Make sure to cover all dimensions, by setting minColumnDomain as lower bound
        // cellWidth = std::max(maxColumnDomain / 10, minColumnDomain);
        cellWidth = maxColumnDomain / 10;
        std::cout << "[FixedGridPartitioning] Computed cell width is: " << cellWidth << std::endl;

        // Read the table in batches
        uint32_t batchId = 0;
        uint32_t totalNumRows = 0;
        while (true) {
            // Try to load a new batch, when possible
            std::shared_ptr<arrow::RecordBatch> record_batch;
            ARROW_RETURN_NOT_OK(batchReader->ReadNext(&record_batch));
            if (record_batch == nullptr) {
                break;
            }
            totalNumRows += record_batch->num_rows();
            ARROW_RETURN_NOT_OK(partitionBatch(batchId, record_batch, dataReader));
            std::cout << "[FixedGridPartitioning] Batch " << batchId << " out of " << expectedNumBatches << " completed" << std::endl;
            std::cout << "[FixedGridPartitioning] Imported " << totalNumRows << " out of " << numRows << " rows" << std::endl;
            batchId += 1;
        }
        // The sum of rows from the batches should match the number of rows expected from parquet metadata
        assert(totalNumRows == numRows);

        // Merge the files to create globally sorted partitions
        ARROW_RETURN_NOT_OK(external::ExternalMerge::mergeFilesFromSortedBatches(folder, "cell_idx", partitionSize));
        std::cout << "[FixedGridPartitioning] Partitioning of " << batchId << " batches completed" << std::endl;

        deleteSubfolders();

        return arrow::Status::OK();
    }

    arrow::Status FixedGridPartitioning::partitionBatch(const uint32_t &batchId,
                                                        std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                                        std::shared_ptr<storage::DataReader> &dataReader) {
        auto converter = common::ColumnDataConverter();
        partitionIds = {};
        std::vector<std::shared_ptr<arrow::Array>> batchColumns;
        batchColumns.reserve(columns.size());
        for (const auto &columnName: columns){
            batchColumns.emplace_back(recordBatch->column(dataReader->getColumnIndex(columnName).ValueOrDie()));
        }
        auto columnData = converter.toDouble(batchColumns).ValueOrDie();
        size_t batchNumRows = columnData[0]->size();
        size_t batchNumCols = columnData.size();
        batchColumns.clear();

        std::vector<uint64_t> cellIndexes = {};
        for (size_t i = 0; i < batchNumRows; i++){
            uint64_t cellIndex = 0;
            uint64_t multiplier = 1;
            for (size_t j = 0; j < batchNumCols; j++){
                try{
                    auto dimensionDomain = columnToDomain[j];
                    auto dimensionNumCells = (uint64_t) std::ceil(dimensionDomain / cellWidth);
                    auto column = columnData[j];
                    double_t columnValue = column->at(i);
                    auto cellDimensionIndex = (uint64_t) std::floor(columnValue / cellWidth);
                    // assert(cellDimensionIndex < columnToDomain[j]);
                    cellIndex += cellDimensionIndex * multiplier;
                    multiplier *= dimensionNumCells;
                }
                catch (std::exception& e) {
                    std::cout << "Could not compute cell index " << e.what() << std::endl;
                    cellIndexes.emplace_back(0);
                    continue;
                }
            }
            cellIndexes.emplace_back(cellIndex);
        }

        // Add to the record batch the new column with the cell index values
        arrow::UInt64Builder uint64Builder;
        ARROW_RETURN_NOT_OK(uint64Builder.AppendValues(cellIndexes));
        std::shared_ptr<arrow::Array> cellIndexValuesArrow;
        ARROW_ASSIGN_OR_RAISE(cellIndexValuesArrow, uint64Builder.Finish());
        std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
        ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "cell_idx", cellIndexValuesArrow));
        std::cout << "[FixedGridPartitioning] Added column with cell index values " << std::endl;

        // Write out a sorted batch
        std::filesystem::path sortedBatchPath = folder / ("s" + std::to_string(batchId) + fileExtension);
        ARROW_RETURN_NOT_OK(external::ExternalSort::writeSortedBatch(updatedRecordBatch, "cell_idx", sortedBatchPath));
        return arrow::Status::OK();
    }

}