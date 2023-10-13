#include "../include/partitioning/FixedGridPartitioning.h"

namespace partitioning {

FixedGridPartitioning::FixedGridPartitioning(std::vector<std::string> partitionColumns, int size) {
    columns = std::move(partitionColumns);
    setCellSize(size);
}

void FixedGridPartitioning::setCellSize(int size){
    cellSize = size;
}

arrow::Status FixedGridPartitioning::ColumnsToPartitionId(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                                          arrow::compute::ExecResult* out) {
    auto* x = batch[0].array.GetValues<int32_t>(1);
    auto* y = batch[1].array.GetValues<int32_t>(1);
    const auto* cellSize = batch[2].array.GetValues<int64_t>(1);
    auto* out_values = out->array_span_mutable()->GetValues<int64_t>(1);

    // Calculate the cell index
    for (int64_t i = 0; i < batch[0].array.length; ++i) {
        auto grid_x = *x++ / *cellSize;
        auto grid_y = *y++ / *cellSize;
        out_values[i] = grid_y * *cellSize + grid_x;
    }
    return arrow::Status::OK();
}

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> FixedGridPartitioning::partition(std::shared_ptr<arrow::Table> table){
    // One idea was adding a custom group_by aggregation function and extract the matching values for each group
    // using the method hash_list(). However, this is not a viable approach since "Grouped Aggregations are not
    // invocable via CallFunction." https://arrow.apache.org/docs/cpp/compute.html

    // The solution approach is the following:
    // 1. We create a new custom method and add it to the compute registry of Arrow.
    //    This custom function takes as parameters two column values + the cell size and returns the corresponding
    //    partition id (that is the cell index) in the FixedGrid.
    // 2. Invoke the custom method passing the columns to generate a vector of corresponding partition ids
    // 3. We extract the distinct values of partitions ids.
    // 4. We split the table into a set of different tables, one for each partition:
    //    For each distinct partition_id we filter the table by that partition_id (the newly created column)

    // 1. Add a new custom Compute function to the registry
    // More details here: https://github.com/apache/arrow/blob/fa4310635c784f03fe825ecc818efa3eca361ec0/cpp/examples/arrow/udf_example.cc
    const std::string computeFunctionName = "partition_fixed_grid";
    const arrow::compute::FunctionDoc computeFunctionDoc{
            "Given two values, computes the index of the corresponding cell in a fixed grid 2-dim space",
            "returns the cell index for the values x, y",
            {"x", "y", "cellSize"},
            "PartitionFixedGridOptions"};
    auto func = std::make_shared<arrow::compute::ScalarFunction>(computeFunctionName,
                                                                               arrow::compute::Arity::Ternary(),
                                                                               computeFunctionDoc);
    std::vector<arrow::compute::InputType> inputTypes;
    std::vector<arrow::Datum> columnData;
    for (const auto &column: columns){
        // Infer the data types of the columns
        inputTypes.emplace_back(table->schema()->GetFieldByName(column)->type());
        // Extract column data by getting the chunks and casting them to an arrow array
        std::shared_ptr<arrow::ChunkedArray> chunkedColumn = table->GetColumnByName(column);
        columnData.emplace_back(chunkedColumn->chunk(0));
    }
    inputTypes.emplace_back(arrow::int64());
    arrow::compute::ScalarKernel kernel(inputTypes,
                                        arrow::int64(), ColumnsToPartitionId);
    kernel.mem_allocation = arrow::compute::MemAllocation::PREALLOCATE;
    kernel.null_handling = arrow::compute::NullHandling::INTERSECTION;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
    auto registry = arrow::compute::GetFunctionRegistry();
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

    // Repeat the cellSize values into an array, needed because CallFunction wants same-size columnArrays as args
    std::vector<int64_t> cellSizeValues;
    cellSizeValues.insert(cellSizeValues.end(), table->GetColumnByName(columns.at(0))->length(), cellSize);
    arrow::NumericBuilder<arrow::Int64Type> array_builder;
    ARROW_RETURN_NOT_OK(array_builder.AppendValues(cellSizeValues));
    std::shared_ptr<arrow::Array> cellSizeArray;
    ARROW_RETURN_NOT_OK(array_builder.Finish(&cellSizeArray));
    array_builder.Reset();
    columnData.emplace_back(cellSizeArray);

    // 2. Invoke the custom method
    arrow::Result<arrow::Datum> fixedGridCellIds = arrow::compute::CallFunction(computeFunctionName, columnData);
    auto hallo = fixedGridCellIds->ToString();
    std::shared_ptr<arrow::Array> partitionIds = std::move(fixedGridCellIds)->make_array();
    auto test = partitionIds->ToString();

    // 3. Extract the partition ids
    // Create a new table with the current schema + a new column with the partition ids
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> combined, table->CombineChunks());
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

    // 4. For each distinct partition_id we filter the table by that partition_id (the newly created column)
    std::vector<std::shared_ptr<arrow::Table>> partitionedTables = {};
    // Construct new table with the partitioning column from the record batches and
    // wrap the Table in a Dataset, so we can use a Scanner
    ARROW_ASSIGN_OR_RAISE(auto writeTable, arrow::Table::FromRecordBatches({batch}));
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

    return partitionedTables;
}

}