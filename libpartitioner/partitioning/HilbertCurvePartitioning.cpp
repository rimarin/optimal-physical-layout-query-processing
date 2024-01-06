#include "partitioning/HilbertCurvePartitioning.h"

namespace partitioning {

    arrow::Status HilbertCurvePartitioning::partition(storage::DataReader &dataReader,
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

        std::cout << "[HilbertCurvePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[HilbertCurvePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;

        auto numRows = dataReader.getNumRows();

        if (partitionSize >= numRows) {
            std::cout << "[HilbertCurvePartitioning] Partition size greater than the available rows" << std::endl;
            std::cout << "[HilbertCurvePartitioning] Therefore put all data in one partition" << std::endl;
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
        auto hilbertCurve = common::HilbertCurve();
        int numBits = 8;
        int numDims = columnData.size();
        int columnSize = columnData[0]->size();
        std::map<IntRow, int64_t> rowToHilbertValue;
        std::vector<IntRow> rows;
        std::vector<int64_t> hilbertValues = {};
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
            rowToHilbertValue[rowVector] = hilbertValue;
        }

        // Sort hilbert curve values
        std::sort(std::begin(hilbertValues), std::end(hilbertValues));

        // Iterate over sorted values and group them according to partition size
        std::map<int64_t, uint32_t> hilbertValueToPartitionId;
        auto batchCapacity = partitionCapacity / expectedNumBatches;
        if (batchCapacity <= 0){
            batchCapacity = 1000;
        }
        for (int i = 0; i < hilbertValues.size(); ++i) {
            hilbertValueToPartitionId[hilbertValues[i]] = i / batchCapacity;
        }

        // Iterate over rows, get HilbertValue from rowToHilbertValue, use it to get partitionId
        // from hilbertValueToPartitionId. Append the obtained partitionId to the result values
        std::vector<uint32_t> partitionIdsRaw = {};
        for (auto &row : rows) {
            auto hilbertValueForRow = rowToHilbertValue[row];
            auto partitionId = hilbertValueToPartitionId[hilbertValueForRow];
            partitionIdsRaw.emplace_back(partitionId);
        }
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues(partitionIdsRaw));
        std::cout << "[HilbertCurvePartitioning] Mapped columns to partition ids" << std::endl;
        ARROW_ASSIGN_OR_RAISE(partitionIdsArrow, int32Builder.Finish());
        std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
        ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "partition_id", partitionIdsArrow));
        uniquePartitionIds.merge(std::set(partitionIdsRaw.begin(), partitionIdsRaw.end()));
        auto numPartitions = uniquePartitionIds.size();
        std::cout << "[HilbertCurvePartitioning] Computed " << numPartitions << " unique partition ids" << std::endl;
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
            std::cout << "[HilbertCurvePartitioning] Generate partitioned table with " << partitionedTable->num_rows() << " rows" << std::endl;
            int progress = (int) (float(completedPartitions) / float(numPartitions) * 100);
            std::cout << "[HilbertCurvePartitioning] Progress: " << progress << " %" << std::endl;
        }
        return arrow::Status::OK();
    }

}