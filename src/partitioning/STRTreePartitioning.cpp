#include "../include/partitioning/STRTreePartitioning.h"

namespace partitioning {

    STRTreePartitioning::STRTreePartitioning(std::vector<std::string> partitionColumns, int size) {
        columns = std::move(partitionColumns);
        partitionSize = size;
    }

    arrow::Status STRTreePartitioning::ColumnsToPartitionId(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                                              arrow::compute::ExecResult* out) {
        // Implementation based on:
        // STR: A Simple and Efficient Algorithm for R-Tree Packing, https://dl.acm.org/doi/10.5555/870314
        // Preprocess the data file so that the T rectangles are
        // ordered in [r/b] consecutive groups of b rectangles,
        // where each group of b is intended to be placed in
        // the same leaf level node

        auto* out_values = out->array_span_mutable()->GetValues<int64_t>(1);
        for (int64_t i = 0; i < batch[0].array.length; ++i) {
            out_values[i] = 0;
        }
        std::cout << "[STRTreePartitioning] Mapped columns to partition ids" << std::endl;
        return arrow::Status::OK();
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> STRTreePartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                                                             int partitionSize){
        std::cout << "[STRTreePartitioning] Applying partitioning technique" << std::endl;
        // Add a new custom Compute function to the registry
        const std::string computeFunctionName = "partition_strtree";
        const arrow::compute::FunctionDoc computeFunctionDoc{
                "Given two values, computes the index of the corresponding cell in a fixed grid 2-dim space",
                "returns the cell index for the values x, y",
                {"x", "y", "partitionSize"},
                "PartitionSTRTreeOptions"};
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

        // Repeat the partitionSize values into an array, needed because CallFunction wants same-size columnArrays as args
        std::vector<int64_t> partitionSizeValues;
        partitionSizeValues.insert(partitionSizeValues.end(), table->GetColumnByName(columns.at(0))->length(), partitionSize);
        arrow::NumericBuilder<arrow::Int64Type> array_builder;
        ARROW_RETURN_NOT_OK(array_builder.AppendValues(partitionSizeValues));
        std::shared_ptr<arrow::Array> cellSizeArray;
        ARROW_RETURN_NOT_OK(array_builder.Finish(&cellSizeArray));
        array_builder.Reset();
        columnData.emplace_back(cellSizeArray);

        // Invoke the custom method
        arrow::Result<arrow::Datum> fixedGridCellIds = arrow::compute::CallFunction(computeFunctionName, columnData);
        std::shared_ptr<arrow::Array> partitionIds = std::move(fixedGridCellIds)->make_array();
        auto partitionIdsString = partitionIds->ToString();
        // Call the splitting method to divide the tables into sub-tables according to the partition ids
        return partitioning::MultiDimensionalPartitioning::splitTableIntoPartitions(table, partitionIds);
    }

}