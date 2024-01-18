#include "partitioning/ZOrderCurvePartitioning.h"

namespace partitioning {

    arrow::Status ZOrderCurvePartitioning::partition(){
        // Idea:
        //  1. Read in batches
        //  2. Write out sorted batched by Hilbert value
        //  3. Sort-merge the sorted batches

        // Read the table in batches
        auto batch_reader = dataReader->getTableBatchReader().ValueOrDie();

        uint32_t batchId = 0;
        uint32_t totalNumRows = 0;

        while (true) {

            // Try to read a record batch
            std::shared_ptr<arrow::RecordBatch> record_batch;
            ARROW_RETURN_NOT_OK(batch_reader->ReadNext(&record_batch));
            if (record_batch == nullptr) {
                break;
            }

            totalNumRows += record_batch->num_rows();

            // Work on current batch
            ARROW_RETURN_NOT_OK(partitionBatch(batchId, record_batch, dataReader));
            std::cout << "[ZOrderCurvePartitioning] Batch " << batchId << " completed" << std::endl;
            std::cout << "[ZOrderCurvePartitioning] Imported " << totalNumRows << " out of " << numRows << " rows" << std::endl;
            batchId += 1;
        }
        assert(totalNumRows == numRows);
        // Merge the files to create globally sorted partitions
        ARROW_RETURN_NOT_OK(external::ExternalMerge::mergeFiles(folder, "z_order_curve", partitionSize));
        std::cout << "[ZOrderCurvePartitioning] Partitioning of " << batchId << " batches completed" << std::endl;
        return arrow::Status::OK();
    }

    arrow::Status ZOrderCurvePartitioning::partitionBatch(const uint32_t &batchId,
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
        auto batchNumRows = recordBatch->num_rows();
        assert(columnData.size() == numColumns);
        assert(batchNumRows == columnData[0]->size());

        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        std::vector<std::shared_ptr<common::Point>> rows = common::ColumnDataConverter::toRows(columnData);
        std::vector<int64_t> zOrderValues = {};
        auto zOrderCurve = structures::ZOrderCurve();
        for (auto &row: rows){
            double pointArr[numColumns];
            std::copy(row->begin(), row->end(), pointArr);
            uint64_t zOrderValue = zOrderCurve.encode(pointArr, (int) numColumns);
            zOrderValues.emplace_back(zOrderValue);
        }


        // Add to the record batch the new column with the Z Order values
        arrow::Int64Builder int64Builder;
        ARROW_RETURN_NOT_OK(int64Builder.AppendValues(zOrderValues));
        std::shared_ptr<arrow::Array> hilbertValuesArrow;
        ARROW_ASSIGN_OR_RAISE(hilbertValuesArrow, int64Builder.Finish());
        std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
        ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "z_order_curve", hilbertValuesArrow));
        std::cout << "[ZOrderCurvePartitioning] Added column with Z Order curve values " << std::endl;

        // Write out a sorted batch
        std::filesystem::path sortedBatchPath = folder / ("s" + std::to_string(batchId) + fileExtension);
        ARROW_RETURN_NOT_OK(external::ExternalSort::writeSorted(updatedRecordBatch, "z_order_curve", sortedBatchPath));
        return arrow::Status::OK();
    }

}