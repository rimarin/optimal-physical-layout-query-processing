#include "../../include/partitioning/FixedGrid.h"

namespace partitioning {

FixedGrid::FixedGrid(std::vector<std::string> partitionColumns, int size) {
    columns = partitionColumns;
    setCellSize(size);
}

void FixedGrid::setCellSize(int size){
    cellSize = size;
}

arrow::Status PointsToCell(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                             arrow::compute::ExecResult* out) {
    const auto* x = batch[0].array.GetValues<int64_t>(1);
    const auto* y = batch[1].array.GetValues<int64_t>(1);
    auto* out_values = out->array_span_mutable()->GetValues<int64_t>(1);
    for (int64_t i = 0; i < batch.length; ++i) {
        // TODO: map the x, y coords to the index of the cell, the code should look something like this:
        //    int gridX = static_cast<int>(x / cellSize);
        //    int gridY = static_cast<int>(y / cellSize);
        //    Point cellCoordinates = std::make_pair(gridX, gridY);
        *out_values++ = *x++ + *y++;
    }
    return arrow::Status::OK();
}

arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> FixedGrid::partition(std::shared_ptr<arrow::Table> table){
    std::map<Point, std::vector<arrow::Table>> cellToTables;

    // Add a new custom Compute function to the registry
    // More details here: https://github.com/apache/arrow/blob/fa4310635c784f03fe825ecc818efa3eca361ec0/cpp/examples/arrow/udf_example.cc#L78
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

    // Wrap the Table in a Dataset, so we can use a Scanner
    // more info here: https://arrow.apache.org/docs/cpp/dataset.html#projecting-columns
    std::shared_ptr<arrow::dataset::Dataset> dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);
    // Build ScannerOptions for a Scanner and call the above defined function
    auto options = std::make_shared<arrow::dataset::ScanOptions>();
    options->projection = arrow::compute::CallFunction("partition_fixed_grid",
           {arrow::compute::field_ref(columns[0]),
            arrow::compute::field_ref(columns[1])});

    arrow::Int32Type partitionNumber;
    ARROW_ASSIGN_OR_RAISE(partitionNumber,
                          arrow::compute::CallFunction("partition_fixed_grid", {numbers_array, increment}));
    std::shared_ptr<arrow::Array> incremented_array = std::move(incremented_datum).make_array();

    ARROW_ASSIGN_OR_RAISE(auto scan_builder, table->NewScan());
    std::vector<std::string> names;
    std::vector<arrow::compute::Expression> exprs;
    // Read all the original columns.
    for (const auto& field : dataset->schema()->fields()) {
        names.push_back(field->name());
        exprs.push_back(cp::field_ref(field->name()));
    }
    // Also derive a new column.
    // TODO: handle column computeFunctionName collision
    names.emplace_back("partition");
    exprs.push_back(cp::greater(cp::field_ref("b"), cp::literal(1)));
    ARROW_RETURN_NOT_OK(scan_builder->Project(exprs, names));
    ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
    return scanner->ToTable();

    return arrow::Status::OK();
}

}