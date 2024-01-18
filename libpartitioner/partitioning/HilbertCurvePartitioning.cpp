#include "partitioning/HilbertCurvePartitioning.h"

namespace partitioning {

    arrow::Status HilbertCurvePartitioning::partition(){
        // TODO: Adjust algorithm:
        //  1. Read in batches
        //  2. Write out sorted batched by Hilbert value
        //  3. Sort-merge the sorted batches
        // Read the table in batches
        auto batch_reader = dataReader->getTableBatchReader().ValueOrDie();
        uint32_t batchId = 0;
        uint32_t totalNumRows = 0;
        while (true) {
            // Try to load a new batch, when possible
            std::shared_ptr<arrow::RecordBatch> record_batch;
            ARROW_RETURN_NOT_OK(batch_reader->ReadNext(&record_batch));
            if (record_batch == nullptr) {
                break;
            }
            totalNumRows += record_batch->num_rows();
            // Partitioning current batch
            ARROW_RETURN_NOT_OK(partitionBatch(batchId, record_batch, dataReader));
            std::cout << "[HilbertCurvePartitioning] Batch " << batchId << " completed" << std::endl;
            std::cout << "[HilbertCurvePartitioning] Imported " << totalNumRows << " out of " << numRows << " rows" << std::endl;
            batchId += 1;
        }
        ARROW_RETURN_NOT_OK(external::ExternalMerge::mergeFiles(folder, "hilbert_curve", partitionSize));
        std::cout << "[HilbertCurvePartitioning] Partitioning of " << batchId << " batches completed" << std::endl;
        return arrow::Status::OK();
    }

    arrow::Status HilbertCurvePartitioning::partitionBatch(const uint32_t &batchId,
                                                           std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                                           std::shared_ptr<storage::DataReader> &dataReader) {
        std::vector<std::shared_ptr<arrow::Array>> batchColumns;
        batchColumns.reserve(columns.size());
        for (const auto &columnName: columns){
            batchColumns.emplace_back(recordBatch->column(dataReader->getColumnIndex(columnName).ValueOrDie()));
        }
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toInt64(batchColumns).ValueOrDie();
        std::shared_ptr<arrow::Array> partitionIdsArrow;
        arrow::UInt32Builder int32Builder;
        auto hilbertCurve = structures::HilbertCurve();
        int numBits = 8;
        auto batchNumRows = recordBatch->num_rows();
        assert(columnData.size() == numColumns);
        assert(batchNumRows == columnData[0]->size());
        std::map<IntRow, int64_t> rowToHilbertValue;
        std::vector<IntRow> rows;
        std::vector<uint64_t> hilbertValues = {};
        // Convert columnar format to rows and compute Hilbert value for each for them
        // Append Hilbert values to one vector
        for (int i = 0; i < batchNumRows; ++i) {
            IntRow rowVector;
            for (int j = 0; j < numColumns; ++j) {
                rowVector.emplace_back(columnData[j]->at(i));
            }
            rows.emplace_back(rowVector);
            auto coordinatesVector = IntRow(rowVector);
            int64_t* coordinates = coordinatesVector.data();
            hilbertCurve.axesToTranspose(coordinates, numBits, (int) numColumns);
            unsigned int hilbertValue = hilbertCurve.interleaveBits(coordinates, numBits, (int) numColumns);
            hilbertValues.emplace_back(hilbertValue);
        }

        // Add to the record batch the new column with the Hilbert values
        arrow::UInt64Builder uint64Builder;
        ARROW_RETURN_NOT_OK(uint64Builder.AppendValues(hilbertValues));
        std::shared_ptr<arrow::Array> hilbertValuesArrow;
        ARROW_ASSIGN_OR_RAISE(hilbertValuesArrow, uint64Builder.Finish());
        std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
        ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "hilbert_curve", hilbertValuesArrow));
        std::cout << "[HilbertCurvePartitioning] Added column with hilbert curve values " << std::endl;

        std::filesystem::path sortedBatchPath = folder / ("s" + std::to_string(batchId) + fileExtension);
        ARROW_RETURN_NOT_OK(external::ExternalSort::writeSorted(updatedRecordBatch, "hilbert_curve", sortedBatchPath));
        return arrow::Status::OK();
    }

}