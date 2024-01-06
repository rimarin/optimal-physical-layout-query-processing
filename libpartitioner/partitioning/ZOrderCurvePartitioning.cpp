#include "partitioning/ZOrderCurvePartitioning.h"

namespace partitioning {

    arrow::Status ZOrderCurvePartitioning::partition(storage::DataReader &dataReader,
                                                     const std::vector<std::string> &partitionColumns,
                                                     const size_t partitionSize,
                                                     const std::filesystem::path &outputFolder){
        columns = partitionColumns;
        folder = outputFolder;
        numColumns = columns.size();
        partitionIds = {};
        uniquePartitionIds = {};
        expectedNumBatches = dataReader.getExpectedNumBatches();
        partitionCapacity = partitionSize;

        std::cout << "[ZOrderCurvePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[ZOrderCurvePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;

        auto numRows = dataReader.getNumRows();

        if (partitionSize >= numRows) {
            std::cout << "[ZOrderCurvePartitioning] Partition size greater than the available rows" << std::endl;
            std::cout << "[ZOrderCurvePartitioning] Therefore put all data in one partition" << std::endl;
            std::filesystem::path source = dataReader.getReaderPath();
            std::filesystem::path destination = outputFolder / "0.parquet";
            std::filesystem::copy(source, destination, std::filesystem::copy_options::overwrite_existing);
            return arrow::Status::OK();
        }

        auto batch_reader = dataReader.getTableBatchReader().ValueOrDie();
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
            std::cout << "[ZOrderCurvePartitioning] Batch " << batchId << " completed" << std::endl;
            std::cout << "[ZOrderCurvePartitioning] Imported " << totalNumRows << " out of " << numRows << " rows" << std::endl;
            batchId += 1;
        }
        std::cout << "[ZOrderCurvePartitioning] Partitioning of " << batchId << " batches completed" << std::endl;
        ARROW_RETURN_NOT_OK(storage::DataWriter::mergeBatches(folder, uniquePartitionIds));
        return arrow::Status::OK();
    }

    arrow::Status ZOrderCurvePartitioning::partitionBatch(const uint32_t &batchId,
                                                           std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                                           storage::DataReader &dataReader) {
        std::vector<std::shared_ptr<arrow::Array>> batchColumns;
        batchColumns.reserve(columns.size());
        for (const auto &columnName: columns){
            batchColumns.emplace_back(recordBatch->column(dataReader.getColumnIndex(columnName).ValueOrDie()));
        }
        auto converter = common::ColumnDataConverter();
        auto columnData = converter.toInt64(batchColumns).ValueOrDie();
        std::shared_ptr<arrow::Array> partitionIdsArrow;
        arrow::UInt32Builder int32Builder;
        int numBits = 8;
        int numDims = columnData.size();
        int columnSize = columnData[0]->size();

        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        std::vector<std::shared_ptr<common::Point>> rows = common::ColumnDataConverter::toRows(columnData);
        std::map<std::shared_ptr<common::Point>, int64_t> rowToZValue;
        std::vector<int64_t> zOrderValues = {};
        auto zOrderCurve = common::ZOrderCurve();
        for (auto &row: rows){
            double pointArr[numDims];
            std::copy(row->begin(), row->end(), pointArr);
            uint64_t zOrderValue = zOrderCurve.encode(pointArr, numDims);
            rowToZValue[row] = zOrderValue;
            zOrderValues.emplace_back(zOrderValue);
        }

        // Sort z-order curve values
        std::sort(std::begin(zOrderValues), std::end(zOrderValues));

        // Iterate over sorted values and group them according to partition size
        std::map<int64_t, uint32_t> zOrderValueToPartitionId;
        auto batchCapacity = partitionCapacity / expectedNumBatches;
        if (batchCapacity <= 0){
            batchCapacity = 1000;
        }
        for (int i = 0; i < zOrderValues.size(); ++i) {
            zOrderValueToPartitionId[zOrderValues[i]] = i / batchCapacity;
        }

        // Iterate over rows, get zOrderValue from rowToZValue, use it to get partitionId
        // from zOrderValueToPartitionId. Append the obtained partitionId to the result values
        std::vector<uint32_t> partitionIdsRaw = {};
        for (auto &row : rows) {
            auto zOrderValueForRow = rowToZValue[row];
            auto partitionId = zOrderValueToPartitionId[zOrderValueForRow];
            partitionIdsRaw.emplace_back(partitionId);
        }

        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(partitionIdsRaw));
        std::cout << "[ZOrderCurvePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIdsArrow, int32Builder.Finish());
        std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
        ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "partition_id", partitionIdsArrow));
        uniquePartitionIds.merge(std::set(partitionIdsRaw.begin(), partitionIdsRaw.end()));
        auto numPartitions = uniquePartitionIds.size();
        std::cout << "[ZOrderCurvePartitioning] Computed " << numPartitions << " unique partition ids" << std::endl;
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
            std::cout << "[ZOrderCurvePartitioning] Generate partitioned table with " << partitionedTable->num_rows() << " rows" << std::endl;
            int progress = (int) (float(completedPartitions) / float(numPartitions) * 100);
            std::cout << "[ZOrderCurvePartitioning] Progress: " << progress << " %" << std::endl;
        }
        return arrow::Status::OK();
    }

}