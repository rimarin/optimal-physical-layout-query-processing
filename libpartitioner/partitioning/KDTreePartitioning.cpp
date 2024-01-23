#include "partitioning/KDTreePartitioning.h"

namespace partitioning {

    arrow::Status KDTreePartitioning::partition(){
        /* Idea:
         * 1. (First pass) Read in batches
         * 2. Find the median from the batches
         * 3. (Second pass) Assign left or right of median for each batch
         * 4. Write to disk left and right
         * 5. Repeat for both left and right on the new dimension
         */

        // Copy original files to destination and work there
        // This way all the batch readers will point to the right folder from the start
        ARROW_RETURN_NOT_OK(copyOriginalToDestination());

        // Initialize the reader, slice size and column index
        auto datasetFile = folder / ("0" + fileExtension);
        size_t sliceSize = numRows;
        uint32_t columnIndex = 0;

        // Call the recursive partitioning method
        std::ignore = partitionBranches(datasetFile, columnIndex);

        // Finalize the files
        deleteIntermediateFiles();
        moveCompletedFiles();
        deleteSubfolders();

        // Finished
        std::cout << "[KDTreePartitioning] Completed" << std::endl;
        return arrow::Status::OK();
    }

    arrow::Status KDTreePartitioning::partitionBranches(std::filesystem::path &datasetFile,
                                                        uint32_t depth){
        // Base case: created a node of size = partition size
        std::ignore = dataReader->load(datasetFile);
        auto nodeSize = dataReader->getNumRows();
        if (nodeSize <= partitionSize){
            // Rename the processed slice file
            auto basePath = datasetFile.parent_path();
            auto renamedDatasetFile = basePath / ("completed" + datasetFile.filename().string());
            std::filesystem::rename(datasetFile, renamedDatasetFile);
            return arrow::Status::OK();
        }

        // Compute the median of current file to partition
        uint32_t columnIndex = depth % numColumns;
        double median = findMedian(datasetFile, columnIndex).ValueOrDie();

        // Read the table in batches
        uint32_t batchId = 0;
        std::string columnName = columns.at(columnIndex);

        // Update readers for current file
        std::ignore = dataReader->load(datasetFile);
        auto currentBatchReader = dataReader->getBatchReader().ValueOrDie();

        // Extract partition id from the file name
        std::string filename = datasetFile.filename();
        size_t lastIndex = filename.find_last_of('.');
        std::string partitionId = filename.substr(0, lastIndex);

        // Generate new sub folder (with this partition id) for this processing iteration
        auto baseFolder = datasetFile.parent_path();
        auto subFolder = baseFolder / partitionId;
        if (!std::filesystem::exists(subFolder)) {
            std::filesystem::create_directory(subFolder);
        }

        std::vector<arrow::Expression> filterExpressions;
        // Load and sort batches
        while (true) {

            // Try to load a new batch, when possible
            std::shared_ptr<arrow::RecordBatch> recordBatch;
            ARROW_RETURN_NOT_OK(currentBatchReader->ReadNext(&recordBatch));
            if (recordBatch == nullptr) {
                break;
            }

            auto batchRows = recordBatch->num_rows();
            std::cout << "[KDTreePartitioning] Batch has " << batchRows << " rows" << std::endl;
            // If it makes sense to divide into >= and < than median
            if (batchRows > partitionSize){
                // Define filters for median split
                auto batchTable = arrow::Table::FromRecordBatches({recordBatch}).ValueOrDie();
                std::shared_ptr<arrow::dataset::Dataset> batchDataset = std::make_shared<arrow::dataset::InMemoryDataset>(batchTable);
                auto options = std::make_shared<arrow::dataset::ScanOptions>();
                filterExpressions = {
                        // Greater or equal than median
                        arrow::compute::greater_equal(
                                arrow::compute::field_ref(columnName),
                                arrow::compute::literal(median)
                        ),
                        // Less than median
                        arrow::compute::less(
                                arrow::compute::field_ref(columnName),
                                arrow::compute::literal(median)
                        )
                };
                // Filter a batch by the median values
                // TODO: maybe date cast could necessary, see GridFile
                for (int i = 0; i < filterExpressions.size(); ++i) {
                    options->filter = filterExpressions.at(i);
                    auto builder = arrow::dataset::ScannerBuilder(batchDataset, options);
                    auto scanner = builder.Finish().ValueOrDie();
                    std::shared_ptr<arrow::Table> filteredBatchTable = scanner->ToTable().ValueOrDie();
                    std::cout << "[KDTreePartitioning] Filtered batch table has " << filteredBatchTable->num_rows() << " rows" << std::endl;
                    if (filteredBatchTable->num_rows() > 0){
                        std::filesystem::path filteredFragmentPath = subFolder / filterExpressions.at(i).ToString();
                        if (!std::filesystem::exists(filteredFragmentPath)) {
                            std::filesystem::create_directory(filteredFragmentPath);
                        }
                        std::filesystem::path filteredBatchPath = filteredFragmentPath / (std::to_string(batchId) + fileExtension);
                        // Write out filtered batch
                        ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTableToDisk(filteredBatchTable, filteredBatchPath));
                        std::cout << "[KDTreePartitioning] Exported fragment " << std::to_string(i) << " for batch " << batchId << std::endl;
                    }
                }
                // Otherwise, the record batch size can already fit in partition
            } else{
                auto batchTable = arrow::Table::FromRecordBatches({recordBatch}).ValueOrDie();
                std::filesystem::path filteredBatchPath = subFolder / "(All)" / (std::to_string(batchId) + fileExtension);
                ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTableToDisk(batchTable, filteredBatchPath));
                std::cout << "[KDTreePartitioning] Exported entire fragment for batch " << batchId << std::endl;
            }
            batchId += 1;
        }

        // Merge the fragments of the same group but from different batches
        for (int i = 0; i < filterExpressions.size(); ++i) {
            std::filesystem::path fragmentPartsPath = subFolder / filterExpressions.at(i).ToString();
            if (std::filesystem::exists(fragmentPartsPath)) {
                std::string rootPath;
                ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(fragmentPartsPath, &rootPath));
                ARROW_RETURN_NOT_OK(storage::DataWriter::mergeBatchesInFolder(fs, rootPath));
                std::filesystem::remove_all(fragmentPartsPath);
            }
        }

        // Determine partitions to process next from the current folder
        std::vector<std::filesystem::path> partitionPaths;
        for (auto &file : std::filesystem::directory_iterator(subFolder)) {
            if (file.path().extension() == common::Settings::fileExtension) {
                partitionPaths.emplace_back(file);
            }
        }

        // Recursively split the files in the folder
        for (auto &partitionPath: partitionPaths){
            // Only for files still to be processed
            bool isValidFile = partitionPath.extension() == common::Settings::fileExtension;
            bool isAlreadyCompleted = isFileCompleted(partitionPath);
            if (isValidFile && !isAlreadyCompleted) {
                std::filesystem::path partitionFile = partitionPath;
                std::ignore = partitionBranches(partitionFile, depth + 1);
            }
        }
        return arrow::Status::OK();
    }

    arrow::Result<double> KDTreePartitioning::findMedian(std::filesystem::path &datasetFile, uint32_t columnIndex) {
        // Initialize readers for current file
        std::ignore = dataReader->load(datasetFile);
        auto currentBatchReader = dataReader->getBatchReader().ValueOrDie();
        std::vector<double> medians;
        std::string sortColumn = columns.at(columnIndex);

        // Load and sort batches
        while (true) {

            // Try to load a new batch, when possible
            std::shared_ptr<arrow::RecordBatch> recordBatch;
            ARROW_RETURN_NOT_OK(currentBatchReader->ReadNext(&recordBatch));
            if (recordBatch == nullptr) {
                break;
            }

            ARROW_ASSIGN_OR_RAISE(auto sort_indices,
                                  arrow::compute::SortIndices(recordBatch->GetColumnByName(sortColumn),
                                                              arrow::compute::SortOptions({arrow::compute::SortKey{sortColumn}})));
            ARROW_ASSIGN_OR_RAISE(arrow::Datum sorted, arrow::compute::Take(recordBatch, sort_indices));
            auto batchRows = recordBatch->num_rows();
            uint32_t medianRows;
            if (batchRows % 2 == 0){
                medianRows = 2;
            } else{
                medianRows = 1;
            }
            auto medianBatch = sorted.record_batch()->Slice((recordBatch->num_rows() / 2) - 1, medianRows);
            auto batchColumnIndex = dataReader->getColumnIndex(sortColumn).ValueOrDie();
            std::vector<std::shared_ptr<arrow::Array>> medianColumns = { medianBatch->column(batchColumnIndex)};
            auto converter = common::ColumnDataConverter();
            auto columnData = converter.toDouble(medianColumns).ValueOrDie();
            double medianValue;
            if (batchRows % 2 == 0){
                medianValue = (columnData[0]->at(0) + columnData[0]->at(1)) / 2;
            } else {
                medianValue = columnData[0]->at(0);
            }
            medians.emplace_back(medianValue);
        }

        std::sort(medians.begin(), medians.end(),
                  [&](const double &a, const double &b) {
                      return a < b;
                  });
        auto medianIdx = std::max((int) medians.size() / 2, 0);
        return medians.at(medianIdx);
    }


}