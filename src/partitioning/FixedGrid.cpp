

#include "../../include/partitioning/FixedGrid.h"

namespace partitioning {

FixedGrid::FixedGrid(std::vector<std::string> partitionColumns, int size) {
    columns = std::move(partitionColumns);
    setCellSize(size);
}

void FixedGrid::setCellSize(int size){
    cellSize = size;
}

arrow::Status FixedGrid::PointsToCell(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
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

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> FixedGrid::partition(std::shared_ptr<arrow::Table> table){
    std::map<Point, std::vector<arrow::Table>> cellToTables;

    // One idea was adding a custom group_by aggregation function and extract the matching values for each group
    // using the method hash_list(). However, this is not a viable approach since "Grouped Aggregations are not
    // invocable via CallFunction." https://arrow.apache.org/docs/cpp/compute.html

    // The solution approach is the following:
    // 1. We create a new custom method and add it to the compute registry of Arrow.
    //    This custom function takes two column values as a parameter and returns the corresponding partition in
    //    the FixedGrid.
    // 2. Invoke the custom method passing the columns to generate a vector of corresponding partition ids
    // 3. We extract the distinct values of partitions ids.
    // 4. For each distinct partition we filter the table by partition_id (the newly created column)
    // 5. The filtered data of each partition_id is exported to a different parquet file

    // 1. Add a new custom Compute function to the registry
    // More details here: https://github.com/apache/arrow/blob/fa4310635c784f03fe825ecc818efa3eca361ec0/cpp/examples/arrow/udf_example.cc
    const std::string computeFunctionName = "partition_fixed_grid";
    const arrow::compute::FunctionDoc computeFunctionDoc{
            "Given two values, computes the index of the corresponding cell in a fixed grid 2-dim space",
            "returns the cell index for the values x, y",
            {"x", "y"},
            "PartitionFixedGridOptions"};
    auto func = std::make_shared<arrow::compute::ScalarFunction>(computeFunctionName,
                                                                               arrow::compute::Arity::Ternary(),
                                                                               computeFunctionDoc);
    auto typeColumn1 = table->schema()->GetFieldByName(columns[0])->type();
    auto typeColumn2 = table->schema()->GetFieldByName(columns[1])->type();

    arrow::compute::ScalarKernel kernel({typeColumn1, typeColumn2, arrow::int64()},
                                        arrow::int64(), PointsToCell);

    kernel.mem_allocation = arrow::compute::MemAllocation::PREALLOCATE;
    kernel.null_handling = arrow::compute::NullHandling::INTERSECTION;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
    auto registry = arrow::compute::GetFunctionRegistry();
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

    std::shared_ptr<arrow::ChunkedArray> column1 = table->GetColumnByName(columns.at(0));
    std::shared_ptr<arrow::ChunkedArray> column2 = table->GetColumnByName(columns.at(1));

    std::shared_ptr<arrow::Int64Array> column1Data = std::static_pointer_cast<arrow::Int64Array>(column1->chunk(0));
    std::shared_ptr<arrow::Int64Array> column2Data = std::static_pointer_cast<arrow::Int64Array>(column2->chunk(0));

    // 2. Repeat the cellSize values into an array, needed because CallFunction wants same-size columnArrays as args
    std::vector<int64_t> cellSizeValues;
    cellSizeValues.insert(cellSizeValues.end(), column1->length(), cellSize);
    arrow::NumericBuilder<arrow::Int64Type> array_builder;
    ARROW_RETURN_NOT_OK(array_builder.AppendValues(cellSizeValues));
    std::shared_ptr<arrow::Array> cellSizeArray;
    ARROW_RETURN_NOT_OK(array_builder.Finish(&cellSizeArray));
    array_builder.Reset();

    //
    arrow::Result<arrow::Datum> fixedGridCellIds =  arrow::compute::CallFunction(computeFunctionName,
                                                                                 {column1Data, column2Data, cellSizeArray});
    auto hallo = fixedGridCellIds->ToString();
    std::shared_ptr<arrow::Array> partitionIds = std::move(fixedGridCellIds)->make_array();
    auto test = partitionIds->ToString();

    // create new table with current schema + new column
    // use same columns + new generate partitions ids as new column
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

    auto arrow_array = std::static_pointer_cast<arrow::Int64Array>(partitionIds);
    std::set<arrow::Int64Type> uniquePartitionIds;
    for (int64_t i = 0; i < partitionIds->length(); ++i)
    {
        uniquePartitionIds.insert(arrow_array->Value(i));
    }

    return arrow::Status::OK();
}

}