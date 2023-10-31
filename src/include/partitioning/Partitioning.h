#ifndef PARTITIONING_PARTITIONING_H
#define PARTITIONING_PARTITIONING_H

#include <iostream>
#include <set>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/compute/api.h>


namespace partitioning {

    class MultiDimensionalPartitioning {
    public:
        MultiDimensionalPartitioning() = default;
        virtual ~MultiDimensionalPartitioning() = default;
        virtual arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> partition(std::shared_ptr<arrow::Table> table,
                                                                                    int partitionSize) = 0;
        inline static std::vector<std::shared_ptr<arrow::Table>> splitPartitions(std::shared_ptr<arrow::Table> &table,
                                                                                  std::shared_ptr<arrow::Array> &partitionIds);
    };

    std::vector<std::shared_ptr<arrow::Table>>
    MultiDimensionalPartitioning::splitPartitions(std::shared_ptr<arrow::Table> &table,
                                                  std::shared_ptr<arrow::Array> &partitionIds) {
        // Extract the distinct values of partitions ids.
        // Split the table into a set of different tables, one for each partition:
        // For each distinct partition_id we filter the table by that partition_id (the newly created column)

        // Extract the partition ids
        // Create a new table with the current schema + a new column with the partition ids
        std::string strTable = table->ToString();
        std::shared_ptr<arrow::Table> combined = table->CombineChunks().ValueOrDie();
        std::vector<std::shared_ptr<arrow::Array>> columnArrays;
        std::vector<std::shared_ptr<arrow::Field>> columnFields;
        for (int i=0; i < table->num_columns(); ++i) {
            columnArrays.push_back(combined->columns().at(i)->chunk(0));
            columnFields.push_back(table->schema()->field(i));
        }
        columnFields.push_back(arrow::field("partition_id", arrow::int64()));
        columnArrays.push_back(partitionIds);
        std::shared_ptr<arrow::Schema> newSchema = arrow::schema(columnFields);
        std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(newSchema, table->num_rows(), std::move(columnArrays));
        auto newTablePreview = batch->ToString();
        // Retrieve the distinct partition ids
        auto arrow_array = std::static_pointer_cast<arrow::Int64Array>(partitionIds);
        std::set<int64_t> uniquePartitionIds;
        for (int64_t i = 0; i < partitionIds->length(); ++i)
        {
            uniquePartitionIds.insert(arrow_array->Value(i));
        }
        auto numPartitions = uniquePartitionIds.size();
        std::cout << "[Partitioning] Computed " << numPartitions << " unique partition ids" << std::endl;
        assert(("Number of partitions is too high, component might freeze or crash", numPartitions < 20000));

        // For each distinct partition_id we filter the table by that partition_id (the newly created column)
        std::vector<std::shared_ptr<arrow::Table>> partitionedTables = {};
        // Construct new table with the partitioning column from the record batches and
        // Wrap the Table in a Dataset, so we can use a Scanner
        auto writeTable = arrow::Table::FromRecordBatches({batch}).ValueOrDie();
        std::shared_ptr<arrow::dataset::Dataset> dataset = std::make_shared<arrow::dataset::InMemoryDataset>(writeTable);
        for (const auto &partitionId: uniquePartitionIds){
            // Build ScannerOptions for a Scanner to do a basic filter operation
            auto options = std::make_shared<arrow::dataset::ScanOptions>();
            options->filter = arrow::compute::equal(
                    arrow::compute::field_ref("partition_id"),
                    arrow::compute::literal(partitionId)); // Change for your use case
            auto builder = arrow::dataset::ScannerBuilder(dataset, options);
            auto scanner = builder.Finish();
            std::shared_ptr<arrow::Table> partitionedTable = scanner.ValueOrDie()->ToTable().ValueOrDie();
            partitionedTables.push_back(partitionedTable);
        }
        std::cout << "[Partitioning] Split table into " << partitionedTables.size() << " partitions" << std::endl;
        return partitionedTables;
    }
}

#endif //PARTITIONING_PARTITIONING_H
