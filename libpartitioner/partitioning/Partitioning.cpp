#include "partitioning/Partitioning.h"

namespace partitioning {

    // Initialize data members and display initial debug information
    MultiDimensionalPartitioning::MultiDimensionalPartitioning(const std::shared_ptr<storage::DataReader> &reader,
                                                               const std::vector<std::string> &partitionColumns,
                                                               const size_t rowsPerPartition,
                                                               const std::filesystem::path &outputFolder) {
        dataReader = reader;
        columns = partitionColumns;
        folder = outputFolder;
        numColumns = partitionColumns.size();
        assert(numColumns >= 2);
        numRows = dataReader->getNumRows();
        expectedNumBatches = dataReader->getExpectedNumBatches();
        partitionSize = rowsPerPartition;
        assert(partitionSize > 0);

        std::cout << "[Partitioning] Initializing partitioning technique" << std::endl;
        std::cout << "[Partitioning] Partition has to be done on columns: <";
        for (const auto &column: columns){
            std::cout << column << ", ";
        }
        std::cout << ">" << std::endl;

        finished = checkNoNecessaryPartition();
    };

    // If the partition size is greater than the available rows, we do not need to partition
    // Therefore, we directly copy the original file to the partitioned folder
    bool MultiDimensionalPartitioning::checkNoNecessaryPartition() {
        if (partitionSize >= dataReader->getNumRows()) {
            std::cout << "[Partitioning] Partition size greater than the available rows" << std::endl;
            std::cout << "[Partitioning] Therefore put all data in one partition" << std::endl;
            std::filesystem::path source = dataReader->getReaderPath();
            std::filesystem::path destination = folder / "0.parquet";
            std::filesystem::copy(source, destination, std::filesystem::copy_options::overwrite_existing);
            return true;
        }
        return false;
    }

    // Splitting method to divide a table into sub-tables according to the partition ids
    // Also useful: https://stackoverflow.com/questions/73118363/how-can-i-partition-an-arrow-table-by-value-in-one-pass
    // Can slice like in https://github.com/apache/arrow/blob/353139680311e809d2413ea46e17e1656069ac5e/cpp/src/arrow/dataset/partition.cc#L90C20-L90C20
    arrow::Status MultiDimensionalPartitioning::writeOutPartitions(std::shared_ptr<arrow::Table> &table,
                                            std::shared_ptr<arrow::Array> &partitionIds,
                                            const std::filesystem::path &outputFolder) {
        // Extract the distinct values of partitions ids.
        // Split the table into a set of different tables, one for each partition:
        // For each distinct partition_id we filter the table by that partition_id (the newly created column)

        // Extract the partition ids
        // Create a new table with the current schema + a new column with the partition ids
        auto numRows = table->num_rows();
        std::shared_ptr<arrow::Table> combined = table->CombineChunks().ValueOrDie();
        std::vector<std::shared_ptr<arrow::Array>> columnArrays;
        std::vector<std::shared_ptr<arrow::Field>> columnFields;
        for (int i = 0; i < table->num_columns(); ++i) {
            auto combinedColumnChunks = combined->columns().at(i)->chunks();
            for (const auto &chunk: combinedColumnChunks) {
                columnArrays.push_back(chunk);
            }
            columnFields.push_back(table->schema()->field(i));
        }
        columnFields.push_back(arrow::field("partition_id", arrow::uint32()));
        columnArrays.push_back(partitionIds);
        std::shared_ptr<arrow::Schema> newSchema = arrow::schema(columnFields);
        std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(newSchema, table->num_rows(),
                                                                             std::move(columnArrays));
        auto newTablePreview = batch->ToString();
        // Retrieve the distinct partition ids
        auto arrow_array = std::static_pointer_cast<arrow::UInt32Array>(partitionIds);
        std::set<uint32_t> uniquePartitionIds;
        for (uint32_t i = 0; i < partitionIds->length(); ++i) {
            uniquePartitionIds.insert(arrow_array->Value(i));
        }
        auto numPartitions = uniquePartitionIds.size();
        std::cout << "[Partitioning] Computed " << numPartitions << " unique partition ids" << std::endl;
        assert(("Number of partitions is too high, component might freeze or crash", numPartitions < 100000));
        // For each distinct partition_id we filter the table by that partition_id (the newly created column)
        // Construct new table with the partitioning column from the record batches and
        // Wrap the Table in a Dataset, so we can use a Scanner
        int partitionedTablesNumRows = 0;
        uint32_t completedPartitions = 0;
        for (const auto &partitionId: uniquePartitionIds) {
            auto writeTable = arrow::Table::FromRecordBatches({batch}).ValueOrDie();
            std::shared_ptr<arrow::dataset::Dataset> dataset = std::make_shared<arrow::dataset::InMemoryDataset>(
                    writeTable);
            // Build ScannerOptions for a Scanner to do a basic filter operation
            auto options = std::make_shared<arrow::dataset::ScanOptions>();
            options->filter = arrow::compute::equal(
                    arrow::compute::field_ref("partition_id"),
                    arrow::compute::literal(partitionId));
            auto builder = arrow::dataset::ScannerBuilder(dataset, options);
            auto scanner = builder.Finish();
            std::shared_ptr<arrow::Table> partitionedTable = scanner.ValueOrDie()->ToTable().ValueOrDie();
            partitionedTablesNumRows += partitionedTable->num_rows();
            auto outfile = arrow::io::FileOutputStream::Open(
                    outputFolder.string() + "/" + std::to_string(partitionId) + ".parquet");
            std::unique_ptr<parquet::arrow::FileWriter> writer;
            std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder()
                    .max_row_group_length(100000)
                    ->created_by("Optimal Layout Partitioner")
                    ->version(parquet::ParquetVersion::PARQUET_2_6)
                    ->data_page_version(parquet::ParquetDataPageVersion::V2)
                    ->compression(arrow::Compression::SNAPPY)
                    ->build();
            // Options to store Arrow schema for easier reads back into Arrow
            std::shared_ptr<parquet::ArrowWriterProperties> arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();
            PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*partitionedTable, arrow::default_memory_pool(), *outfile,
                                                            table->num_rows(), props, arrow_props));
            completedPartitions += 1;
            std::cout << "[Partitioning] Generate partitioned table with " << partitionedTable->num_rows() << " rows"
                      << std::endl;
            int progress = float(completedPartitions) / float(numPartitions) * 100;
            std::cout << "[Partitioning] Progress: " << progress << " %" << std::endl;
        }
        if (numRows != partitionedTablesNumRows) {
            throw std::runtime_error(
                    "Numbers of rows of the original table and the sum of the rows of the partitioned table should match");
        }
        std::cout << "[Partitioning] Split table into " << numPartitions << " partitions" << std::endl;
        return arrow::Status::OK();
    }

    void MultiDimensionalPartitioning::setColumns(const std::vector<std::string> &partitionColumns) {
        columns = partitionColumns;
    }

    void MultiDimensionalPartitioning::setPartitionSize(const size_t rowsPerPartition) {
        partitionSize = rowsPerPartition;
    }

    void MultiDimensionalPartitioning::setDataReader(const std::shared_ptr<storage::DataReader> &reader) {
        dataReader = reader;
    }
}