#include "partitioning/GridFilePartitioning.h"

namespace partitioning {

    arrow::Status GridFilePartitioning::partition(storage::DataReader &dataReader,
                                                  const std::vector<std::string> &partitionColumns,
                                                  const size_t partitionSize,
                                                  const std::filesystem::path &outputFolder){
        /*
         * In the literature there are different implementations of adaptive grid.
         * Here we use a bulk-load implementation of the original paper:
         * "The Grid File: An Adaptable, Symmetric Multikey File Structure",
         * https://www.cs.ucr.edu/~tsotras/cs236/W15/grid-file.pdf
         * A grid directory consists of two parts: first, a dynamic k-dimensional array called the grid array;
         * its elements (pointers to data buckets) are in one-to-one correspondence with the grid blocks of the
         * partition; and second, k one-dimensional arrays called linear scales, each defines a partition of a domain S;
         * As shown in Fig 9 and Fig 15, we'll try to fit elements in each bucket according to their capacity c,
         * that here is equal to the partition size. This is achieved by splitting the linear scales in half
         * until the desired bucket size is reached. This is done in a circular fashion on all partitioning dimensions.
        */
        std::cout << "[GridFilePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[GridFilePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;

        auto numRows = dataReader.getNumRows();

        if (partitionSize > numRows) {
            std::cout << "[GridFilePartitioning] Partition size greater than the available rows" << std::endl;
            std::cout << "[GridFilePartitioning] Therefore put all data in one partition" << std::endl;
            std::filesystem::path source = dataReader.getReaderPath();
            std::filesystem::path destination = outputFolder / "0.parquet";
            std::filesystem::copy(source, destination);
            return arrow::Status::OK();
        }

        std::vector<std::shared_ptr<arrow::ChunkedArray>> columns = dataReader.getColumns(partitionColumns).ValueOrDie();

        std::cout << "[GridFilePartitioning] Loaded columns of size: " << sizeof(columns) << " bytes" << std::endl;

        uint32_t coordIdx = 0;
        dataReader.getNumRows();
        // assignBucketsRecursive(columns, coordIdx);

        // 1. Read only partitioning columns
        // 2. Create the linear scales
        // 3. Read the dataset by batches
        // 4. For each batch, assign the right bucket by using the created scales
        //      generate combinations from the scales and filter
        // 5. Export the batch into different partition files
        // 6. Merge the batches into partition files

        arrow::UInt32Builder int32Builder;
        ARROW_RETURN_NOT_OK(int32Builder.AppendValues({}));
        std::cout << "[GridFilePartitioning] Mapped columns to partition ids" << std::endl;
        std::shared_ptr<arrow::Array> partitionIds;
        ARROW_ASSIGN_OR_RAISE(partitionIds, int32Builder.Finish());
        // return partitioning::MultiDimensionalPartitioning::writeOutPartitions(table, partitionIds, outputFolder);
    }

    void GridFilePartitioning::assignBucketsRecursive(std::vector<std::shared_ptr<arrow::ChunkedArray>> columns, uint32_t coord) {
        if (columns[0]->length() <= n){
            // TODO: write out partition fragment
            return;
        }

    }

}