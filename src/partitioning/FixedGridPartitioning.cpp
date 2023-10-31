
#include "../include/partitioning/FixedGridPartitioning.h"


namespace partitioning {

    FixedGridPartitioning::FixedGridPartitioning(std::vector<std::string> partitionColumns) {
        columns = std::move(partitionColumns);
        std::cout << "[FixedGridPartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : columns) displayColumns + " " += column;
        std::cout << "[FixedGridPartitioning] Partition has to be done on columns: " << displayColumns << std::endl;
    }

    arrow::Status FixedGridPartitioning::ColumnsToPartitionId(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                                                              arrow::compute::ExecResult* out) {
        auto* x = batch[0].array.GetValues<int32_t>(1);
        auto* y = batch[1].array.GetValues<int32_t>(1);
        std::cout << "[FixedGridPartitioning] First value x is: " << *x << std::endl;
        std::cout << "[FixedGridPartitioning] First value y is: " << *y << std::endl;
        const auto* cellSize = batch[2].array.GetValues<int64_t>(1);
        auto* out_values = out->array_span_mutable()->GetValues<int64_t>(1);

        std::cout << "[FixedGridPartitioning] Computing cell index" << std::endl;
        for (int64_t i = 0; i < batch[0].array.length; ++i) {
            auto grid_x = *x++ / *cellSize;
            auto grid_y = *y++ / *cellSize;
            out_values[i] = grid_y * *cellSize + grid_x;
        }
        std::cout << "[FixedGridPartitioning] Mapped columns to partition ids" << std::endl;
        return arrow::Status::OK();
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> FixedGridPartitioning::partition(std::shared_ptr<arrow::Table> table,
                                                                                               int partitionSize){
        std::cout << "[FixedGridPartitioning] Applying partitioning technique" << std::endl;
        // One idea was adding a custom group_by aggregation function and extract the matching values for each group
        // using the method hash_list(). However, this is not a viable approach since "Grouped Aggregations are not
        // invocable via CallFunction." https://arrow.apache.org/docs/cpp/compute.html

        // The solution approach is the following:
        // 1. We create a new custom method and add it to the compute registry of Arrow.
        //    This custom function takes as parameters two column values + the cell size and returns the corresponding
        //    partition id (that is the cell index) in the FixedGrid.
        // 2. Invoke the custom method passing the columns to generate a vector of corresponding partition ids
        // 3. Pass the table and the partition ids to a helper function to split the table according to the ids

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
            auto columnType = table->schema()->GetFieldByName(column)->type();
            assert(("FixedGrid supports only int32 column type at the moment", columnType == arrow::int32()));
            std::cout << "[FixedGridPartitioning] Reading column <" << column << "> of type " << columnType->ToString() << std::endl;
            inputTypes.emplace_back(columnType);
            // Extract column data by getting the chunks and casting them to an arrow array
            std::shared_ptr<arrow::ChunkedArray> chunkedColumn = table->GetColumnByName(column);
            auto chunk = chunkedColumn->chunk(0);
            auto arrow_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
            std::vector<int32_t> int_vector;
            for (int64_t i = 0; i < chunk->length(); ++i)
            {
                int_vector.push_back(arrow_array->Value(i));
            }
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
        cellSizeValues.insert(cellSizeValues.end(), table->GetColumnByName(columns.at(0))->length(), partitionSize);
        arrow::NumericBuilder<arrow::Int64Type> array_builder;
        ARROW_RETURN_NOT_OK(array_builder.AppendValues(cellSizeValues));
        std::shared_ptr<arrow::Array> cellSizeArray;
        ARROW_RETURN_NOT_OK(array_builder.Finish(&cellSizeArray));
        array_builder.Reset();
        columnData.emplace_back(cellSizeArray);

        // Invoke the custom method
        arrow::Result<arrow::Datum> fixedGridCellIds = arrow::compute::CallFunction(computeFunctionName, columnData);
        std::shared_ptr<arrow::Array> partitionIds = std::move(fixedGridCellIds)->make_array();
        auto partitionIdsString = partitionIds->ToString();
        // Call the splitting method to divide the tables into sub-tables according to the partition ids
        return partitioning::MultiDimensionalPartitioning::splitPartitions(table, partitionIds);
    }

}