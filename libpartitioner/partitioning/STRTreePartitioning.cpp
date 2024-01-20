#include "partitioning/STRTreePartitioning.h"

namespace partitioning {

    arrow::Status STRTreePartitioning::partition() {
        /* Implementation based on:
         * STR: A Simple and Efficient Algorithm for R-Tree Packing, https://dl.acm.org/doi/10.5555/870314
         * An R-tree is a hierarchical data structure derived from the B-tree, which stores a collection of
         * rectangles which can change overtime. Each R-Tree has r nodes, each can hold n rectangles/points.
         * There are different packing algorithms, here we focus on the Sort-Tile-Recursive (STR).
         * Consider a k-dimension data set of r hyper-rectangles. A hyper-rectangle is defined by k
         * intervals of the form [Ai, Bi] and is the locus of points whose i-th coordinate falls inside the
         * i-th interval, for all 1 < i < k.
         * STR is best described recursively with k = 2 providing the base case. (The case k = 1 is
         * already handled well by regular B-trees.) Accordingly, we first consider a set of rectangles in
         * the plane. The basic idea is to "tile" the data space using sqrt(r/n) vertical slices so that each slice
         * contains enough rectangles to pack roughly sqrt(r/n) nodes. Once again we assume coordinates
         * are for the center points of the rectangles. Determine the number of leaf level pages P = [r/n]
         * and let S = sqrt(P). Sort the rectangles by x-coordinate and partition them into S vertical
         * slices. A slice consists of a run of S * n consecutive rectangles from the sorted list. Note that
         * the last slice may contain fewer than S * n rectangles. Now sort the rectangles of each slice
         * by y-coordinate and pack them into nodes by grouping them into runs of length n (the first n
         * rectangles into the first node, the next n into the second node, and so on).
        */

        // Idea:
        //  1. Read in batches
        //  2. Write out sorted batched by x value
        //  3. Sort-merge the sorted batches into S slices
        //     Done by calling mergeFiles with a partition size which is totalRows / S
        //  4. For each slice, read it in batches and sort by y value, sort-merge, ecc...
        //  5. Repeat until we get a partition size <= desired partition size

        // Read the table in batches
        uint32_t batchId = 0;
        uint32_t totalNumRows = 0;

        // Copy original files to destination and work there
        // This way all the batch readers will point to the right folder from the start
        ARROW_RETURN_NOT_OK(copyOriginalToDestination());
        // Determine the base path of the dataset folder
        std::filesystem::path datasetFolderPath = dataReader->getReaderPath().parent_path();
        assert(is_directory(datasetFolderPath) == true);

        size_t sliceSize = numRows;
        uint32_t columnIndex = 0;
        std::string columnName;
        while (sliceSize >= partitionSize){
            // Determine column index
            columnIndex = columnIndex % k;
            columnName = columns.at(columnIndex);
            // Load and sort batches
            while (true) {
                // Try to load a new batch, when possible
                std::shared_ptr<arrow::RecordBatch> recordBatch;
                ARROW_RETURN_NOT_OK(batchReader->ReadNext(&recordBatch));
                if (recordBatch == nullptr) {
                    break;
                }
                // Write out a sorted batch
                std::filesystem::path sortedBatchPath = folder / ("s" + std::to_string(batchId) + fileExtension);
                ARROW_RETURN_NOT_OK(external::ExternalSort::writeSorted(recordBatch, columnName, sortedBatchPath));
                std::cout << "[STRTreePartitioning] Batch " << batchId << " completed" << std::endl;
                std::cout << "[STRTreePartitioning] Imported " << totalNumRows << " out of " << numRows << " rows" << std::endl;
                batchId += 1;
                totalNumRows += recordBatch->num_rows();
            }
            assert(totalNumRows == sliceSize);

            // Update the slice size and the column index
            sliceSize = std::ceil(totalNumRows / S);
            columnIndex += 1;
            // Merge the files to create sorted partitions
            ARROW_RETURN_NOT_OK(external::ExternalMerge::mergeFiles(folder, columnName, sliceSize));
            std::cout << "[STRTreePartitioning] Merged batches with sliceSize " << sliceSize << "and column name "
                      << columnName << std::endl;
        }
        std::cout << "[STRTreePartitioning] Completed" << std::endl;
        return arrow::Status::OK();
    }

}