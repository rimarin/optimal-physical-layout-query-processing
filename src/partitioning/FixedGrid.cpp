#include <utility>

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
    auto* x = batch[0].array.GetValues<int64_t>(1);
    auto* y = batch[1].array.GetValues<int64_t>(1);
    const auto* cellSize = batch[2].array.GetValues<int64_t>(1);
    auto* out_values = out->array_span_mutable()->GetValues<int64_t>(1);

    // Calculate the cell index
    for (int64_t i = 0; i < batch.length; ++i) {
        *out_values++ = static_cast<int64_t>(*y / static_cast<int64_t>(*cellSize)) * *cellSize +
                static_cast<int64_t>(*x / static_cast<int64_t>(*cellSize));
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
    // 2. We apply a projection to the table. The projection calls the custom method to generate a new column with
    //    the partition id.
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
                                                                               arrow::compute::Arity::Binary(),
                                                                               computeFunctionDoc);
    arrow::compute::ScalarKernel kernel({arrow::int64(), arrow::int64(), arrow::int64()},
                            arrow::int64(), PointsToCell);

    kernel.mem_allocation = arrow::compute::MemAllocation::PREALLOCATE;
    kernel.null_handling = arrow::compute::NullHandling::INTERSECTION;
    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
    auto registry = arrow::compute::GetFunctionRegistry();
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

    std::shared_ptr<arrow::ChunkedArray> column1 = table->GetColumnByName(columns.at(0));
    std::shared_ptr<arrow::ChunkedArray> column2 = table->GetColumnByName(columns.at(1));

    // 2. Project the computed partition id to a new column

    arrow::NumericBuilder<arrow::Int64Type> int64_builder;
    std::vector<int64_t> int64_values = {1, 2, 3, 4, 5, 6, 7, 8};
    ARROW_RETURN_NOT_OK(int64_builder.AppendValues({cellSize}));
    std::shared_ptr<arrow::Array> gridSize;
    ARROW_RETURN_NOT_OK(int64_builder.Finish(&gridSize));
    int64_builder.Reset();

    arrow::Datum fixedGridCellIds;
    ARROW_ASSIGN_OR_RAISE(fixedGridCellIds,
                          arrow::compute::CallFunction(computeFunctionName, {column1, column2, gridSize}));
    std::shared_ptr<arrow::Array> partitionIds = std::move(fixedGridCellIds).make_array();

    // Wrap the Table in a Dataset, so we can use a Scanner
    // more info here: https://arrow.apache.org/docs/cpp/dataset.html#projecting-columns
    auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(std::move(table));

    ARROW_ASSIGN_OR_RAISE(auto scanBuilder, dataset->NewScan());

    ARROW_RETURN_NOT_OK(scanBuilder->Project(
            {
            arrow::Expression(*arrow::compute::CallFunction(computeFunctionName,
                                                            {column1, column2, gridSize}))
            },
            {"partition"}));

    std::vector<std::string> names;
    std::vector<arrow::compute::Expression> exprs;
    // Read all the original columns.
    for (const auto& field : dataset->schema()->fields()) {
        names.push_back(field->name());
        exprs.push_back(arrow::compute::field_ref(field->name()));
    }
    // Also derive a new column.
    // TODO: handle column computeFunctionName collision
    names.emplace_back("partition");
    exprs.emplace_back(*arrow::compute::CallFunction(computeFunctionName, {column1, column2}));
    ARROW_RETURN_NOT_OK(scanBuilder->Project(exprs, names));
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanBuilder->Finish());
    auto tbl = scanner->ToTable();

    return arrow::Status::OK();
}

}