#include <utility>

#include "../include/partitioning/QuadTreePartitioning.h"

namespace partitioning {

    QuadTreePartitioning::QuadTreePartitioning(std::vector<std::string> partitionColumns) {
        columns = std::move(partitionColumns);
    }

    arrow::Status QuadTreePartitioning::ColumnsToPartitionId(arrow::compute::KernelContext *ctx, const arrow::compute::ExecSpan &batch,
                                                           arrow::compute::ExecResult *out) {
        // Extract column vectors from the batch, convert them from arrow array to std vector of points
        std::vector<common::Point> partitioningColumnValues = {};
        for(const auto & batchValue : batch.values){
            auto columnArray = batchValue.array.ToArray();
            auto arrowDoubleArray = std::static_pointer_cast<arrow::DoubleArray>(columnArray);
            common::Point columnValues;
            for (int64_t i = 0; i < arrowDoubleArray->length(); ++i) {
                columnValues.push_back(arrowDoubleArray->Value(i));
            }
            partitioningColumnValues.emplace_back(columnValues);
        }
        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        std::vector<common::Point> points;
        auto numColumns = partitioningColumnValues.size();
        auto numRows = partitioningColumnValues[0].size();
        for (int i = 0; i < numRows; i++){
            common::Point point = {};
            for (int j = 0; j < numColumns; j++) {
                point.emplace_back(partitioningColumnValues[j][i]);
            }
            points.emplace_back(point);
        }

        // Build a QuadTree on the vector of points
        std::shared_ptr<common::QuadTree> quadTree = std::make_shared<common::QuadTree>(points);
        // Retrieve the leaves, where the points have partitioned and stored
        std::vector<std::shared_ptr<common::QuadNode>> leaves = quadTree->getLeaves();

        // Build a hashmap to link each point to the partition induced by the QuadTree
        std::map<common::Point, int64_t> pointToPartitionId;
        for (int i = 0; i < leaves.size(); i++){
            auto partitionedPoints = leaves[i]->data;
            for (auto &point: partitionedPoints){
                pointToPartitionId[point] = i;
            }
        }

        // We need to return a vector of partition id for the passed columns
        // However, the partition id have to be aligned with the initial sorting of the points
        // Therefore, iterate over points (as passed in the first place) and assign to the return value
        // the mapped partition
        auto* out_values = out->array_span_mutable()->GetValues<int64_t>(1);
        for (int i = 0; i < numRows; i++){
            out_values[i] = pointToPartitionId[points[i]];
        }
        std::cout << "[QuadTreePartitioning] Mapped columns to partition ids" << std::endl;
        return arrow::Status::OK();
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::Table>>> QuadTreePartitioning::partition(std::shared_ptr<arrow::Table> table){
        std::cout << "[QuadTreePartitioning] Applying partitioning technique" << std::endl;
        const std::string computeFunctionName = "partition_quadtree";
        const arrow::compute::FunctionDoc computeFunctionDoc{
                "Given n columns, builds a QuadTree to partition the data and return the partition id",
                "returns partition id for the columns",
                {"col1", "col2", "..."},
                "PartitionQuadTreeOptions"};
        auto func = std::make_shared<arrow::compute::ScalarFunction>(computeFunctionName,
                                                                     arrow::compute::Arity::VarArgs(2),
                                                                     computeFunctionDoc);
        std::vector<arrow::compute::InputType> inputTypes = {};
        // Extract column data by getting the chunks and casting them to an arrow array
        std::vector<arrow::Datum> columnData;
        for (const auto &column: columns){
            // Infer the data types of the columns
            inputTypes.emplace_back(table->schema()->GetFieldByName(column)->type());
            auto debug = table->schema()->GetFieldByName(column)->type();
            auto debug2 = table->ToString();
            // Extract column data by getting the chunks and casting them to an arrow array
            std::shared_ptr<arrow::ChunkedArray> chunkedColumn = table->GetColumnByName(column);
            columnData.emplace_back(chunkedColumn->chunk(0));
        }
        auto signature = arrow::compute::KernelSignature::Make(inputTypes, arrow::int64(), true);
        arrow::compute::ScalarKernel kernel(signature, ColumnsToPartitionId);
        kernel.mem_allocation = arrow::compute::MemAllocation::PREALLOCATE;
        kernel.null_handling = arrow::compute::NullHandling::INTERSECTION;
        ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));
        auto registry = arrow::compute::GetFunctionRegistry();
        ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

        // Invoke the custom method
        arrow::Result<arrow::Datum> QuadTreePartitionIds = arrow::compute::CallFunction(computeFunctionName, columnData);
        auto hallo = QuadTreePartitionIds->ToString();
        std::shared_ptr<arrow::Array> partitionIds = std::move(QuadTreePartitionIds)->make_array();
        auto test = partitionIds->ToString();
        return partitioning::MultiDimensionalPartitioning::splitPartitions(table, partitionIds);
    }

}