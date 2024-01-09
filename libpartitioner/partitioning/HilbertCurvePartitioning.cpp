#include "partitioning/HilbertCurvePartitioning.h"

namespace partitioning {

    arrow::Status HilbertCurvePartitioning::partition(){

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
        std::cout << "[HilbertCurvePartitioning] Partitioning of " << batchId << " batches completed" << std::endl;
        ARROW_RETURN_NOT_OK(storage::DataWriter::mergeBatches(folder, uniquePartitionIds));
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
        int numDims = columnData.size();
        int columnSize = columnData[0]->size();
        std::map<IntRow, int64_t> rowToHilbertValue;
        std::vector<IntRow> rows;
        std::vector<uint64_t> hilbertValues = {};
        // Convert columnar format to rows and compute Hilbert value for each for them
        // Build a hashmap to link each row to the generated Hilbert value
        // In addition, append Hilbert values to one vector
        for (int i = 0; i < columnSize; ++i) {
            IntRow rowVector;
            for (int j = 0; j < numDims; ++j) {
                rowVector.emplace_back(columnData[j]->at(i));
            }
            rows.emplace_back(rowVector);
            auto coordinatesVector = IntRow(rowVector);
            int64_t* coordinates = coordinatesVector.data();
            hilbertCurve.axesToTranspose(coordinates, numBits, numDims);
            unsigned int hilbertValue = hilbertCurve.interleaveBits(coordinates, numBits, numDims);
            hilbertValues.emplace_back(hilbertValue);
        }

        arrow::UInt64Builder uint64Builder;
        ARROW_RETURN_NOT_OK(uint64Builder.AppendValues(hilbertValues));
        std::shared_ptr<arrow::Array> hilbertValuesArrow;
        ARROW_ASSIGN_OR_RAISE(hilbertValuesArrow, uint64Builder.Finish());
        std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
        ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "hilbert_curve", hilbertValuesArrow));
        std::cout << "[HilbertCurvePartitioning] Added column with hilbert curve values " << std::endl;

        std::filesystem::path sortedBatchPath = folder / ("s" + std::to_string(batchId) + ".parquet");
        ARROW_RETURN_NOT_OK(external::ExternalSort::writeSorted(updatedRecordBatch->Slice(2, 2), "hilbert_curve", sortedBatchPath));
        std::filesystem::path sortedBatchPath2 = folder / ("s" + std::to_string(batchId+1) + ".parquet");
        ARROW_RETURN_NOT_OK(external::ExternalSort::writeSorted(updatedRecordBatch->Slice(4, 2), "hilbert_curve", sortedBatchPath2));
        std::filesystem::path sortedBatchPath3 = folder / ("s" + std::to_string(batchId+2) + ".parquet");
        ARROW_RETURN_NOT_OK(external::ExternalSort::writeSorted(updatedRecordBatch->Slice(0, 2), "hilbert_curve", sortedBatchPath3));
        std::filesystem::path sortedBatchPath4 = folder / ("s" + std::to_string(batchId+3) + ".parquet");
        ARROW_RETURN_NOT_OK(external::ExternalSort::writeSorted(updatedRecordBatch->Slice(6, 2), "hilbert_curve", sortedBatchPath4));

        return arrow::Status::OK();
    }

}