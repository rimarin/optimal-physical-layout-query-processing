#include "partitioning/QuadTreePartitioning.h"

namespace partitioning {

    arrow::Status QuadTreePartitioning::partition(){
        /*
         * Idea:
         * 1. Read metadata for each indexed column
         * 2. Use min-max values to compute the mean of each column
         * 3. Read batch by batch and assign data to quadrant
         * 4. Merge part from batches into 4 quadrants
         * 5. Repeat recursively from step 3 until we reach partition size
         */

        // Copy original files to destination and work there
        // This way all the batch readers will point to the right folder from the start
        ARROW_RETURN_NOT_OK(copyOriginalToDestination());

        // Initialize the reader, slice size and column index
        auto datasetFile = folder / ("0" + fileExtension);

        // Call the recursive quadrant slicing method
        std::ignore = partitionQuadrants(datasetFile, 0);

        // Finalize the files
        deleteIntermediateFiles();
        moveCompletedFiles();
        deleteSubfolders();

        return arrow::Status::OK();
    }

    arrow::Status QuadTreePartitioning::partitionQuadrants(std::filesystem::path &datasetFile,
                                                           uint32_t depth){

        // Base case: created a quadrant of size = partition size
        std::ignore = dataReader->load(datasetFile);
        auto quadrantSize = dataReader->getNumRows();
        if (quadrantSize <= partitionSize){
            // Rename the processed quadrant file
            auto basePath = datasetFile.parent_path();
            auto renamedDatasetFile = basePath / ("completed" + datasetFile.filename().string());
            std::filesystem::rename(datasetFile, renamedDatasetFile);
            return arrow::Status::OK();
        }

        // Determine the columns to use for the split
        // We follow this policy: we always use 2 dimensions (with more than that,
        // this would be a QuadTree anymore).
        // When the indexing columns are >= 2, we rotate the pairs of columns to use
        assert(numColumns >= 2);
        uint32_t columnIndexX;
        uint32_t columnIndexY;
        if (numColumns == 2){
            columnIndexX = 0;
            columnIndexY = 1;
        } else{
            columnIndexX = depth % numColumns;
            columnIndexY = (depth + 1) % numColumns;
        }

        // Read the metadata from the indexing columns
        std::string columnX = columns.at(columnIndexX);
        std::pair<double_t, double_t> columnStatsX = dataReader->getColumnStats(columnX).ValueOrDie();
        std::string columnY = columns.at(columnIndexY);
        std::pair<double_t, double_t> columnStatsY = dataReader->getColumnStats(columnY).ValueOrDie();
        // Compute the mean from the min-max statistics
        double meanDimX = (columnStatsX.first + columnStatsX.second) / 2;
        double meanDimY = (columnStatsY.first + columnStatsY.second) / 2;

        // Read the table in batches
        uint32_t batchId = 0;

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
            std::cout << "[QuadTreePartitioning] Batch has " << batchRows << " rows" << std::endl;

            // Define filters for the quadrants
            auto batchTable = arrow::Table::FromRecordBatches({recordBatch}).ValueOrDie();
            std::shared_ptr<arrow::dataset::Dataset> batchDataset = std::make_shared<arrow::dataset::InMemoryDataset>(batchTable);
            auto options = std::make_shared<arrow::dataset::ScanOptions>();
            // Possibly cast date to int64 - for column X
            auto toInt64 = arrow::compute::CastOptions::Safe(arrow::int64());
            arrow::Expression columnXExpression;
            auto columnXType = recordBatch->column(dataReader->getColumnIndex(columnX).ValueOrDie())->type();
            if (arrow::is_date(columnXType->id())) {
                columnXExpression = arrow::compute::call("cast",
                                                        {arrow::compute::field_ref(columnX)},
                                                        toInt64);
            }
            else {
                columnXExpression = arrow::compute::field_ref(columnX);
            }
            // Possibly cast date to int64 - for column Y
            arrow::Expression columnYExpression;
            auto columnYType = recordBatch->column(dataReader->getColumnIndex(columnY).ValueOrDie())->type();
            if (arrow::is_date(columnYType->id())) {
                columnYExpression = arrow::compute::call("cast",
                                                         {arrow::compute::field_ref(columnY)},
                                                         toInt64);
            }
            else {
                columnYExpression = arrow::compute::field_ref(columnY);
            }
            // Align the mean value data type
            const auto& meanValueType = arrow::float64();
            filterExpressions = {
                // North West -> (x < meanDimX && y >= meanDimY)
                arrow::compute::and_(
                    arrow::compute::less(
                            columnXExpression,
                            arrow::compute::call("cast",
                                                 {arrow::compute::literal(meanDimX)},
                                                 arrow::compute::CastOptions::Safe(meanValueType)
                            )
                    ),
                    // AND
                    arrow::compute::greater_equal(
                            columnYExpression,
                            arrow::compute::call("cast",
                                                 {arrow::compute::literal(meanDimY)},
                                                 arrow::compute::CastOptions::Safe(meanValueType)
                            )
                )),
                // North East -> (x >= meanDimX && y >= meanDimY)
                arrow::compute::and_(
                    arrow::compute::greater_equal(
                            columnXExpression,
                            arrow::compute::call("cast",
                                                 {arrow::compute::literal(meanDimX)},
                                                 arrow::compute::CastOptions::Safe(meanValueType)
                            )
                    ),
                    // AND
                    arrow::compute::greater_equal(
                            columnYExpression,
                            arrow::compute::call("cast",
                                                 {arrow::compute::literal(meanDimY)},
                                                 arrow::compute::CastOptions::Safe(meanValueType)
                            )
                )),
                // South West -> (x < meanDimX && y < meanDimY)
                arrow::compute::and_(
                    arrow::compute::less(
                            columnXExpression,
                            arrow::compute::call("cast",
                                                 {arrow::compute::literal(meanDimX)},
                                                 arrow::compute::CastOptions::Safe(meanValueType)
                            )
                    ),
                    // AND
                    arrow::compute::less(
                            columnYExpression,
                            arrow::compute::call("cast",
                                                 {arrow::compute::literal(meanDimY)},
                                                 arrow::compute::CastOptions::Safe(meanValueType)
                            )
                )),
                // South East -> (x >= meanDimX && y < meanDimY)
                arrow::compute::and_(
                    arrow::compute::greater_equal(
                            columnXExpression,
                            arrow::compute::call("cast",
                                                 {arrow::compute::literal(meanDimX)},
                                                 arrow::compute::CastOptions::Safe(meanValueType)
                            )
                    ),
                    // AND
                    arrow::compute::less(
                            columnYExpression,
                            arrow::compute::call("cast",
                                                 {arrow::compute::literal(meanDimY)},
                                                 arrow::compute::CastOptions::Safe(meanValueType)
                            )
                ))
            };

            // Filter a batch by the mean values, for each quadrant
            for (int i = 0; i < filterExpressions.size(); ++i) {
                options->filter = filterExpressions.at(i);
                auto builder = arrow::dataset::ScannerBuilder(batchDataset, options);
                auto scanner = builder.Finish().ValueOrDie();
                std::shared_ptr<arrow::Table> filteredBatchTable = scanner->ToTable().ValueOrDie();
                if (filteredBatchTable->num_rows() > 0){
                    std::filesystem::path filteredQuadrantPath = subFolder / std::to_string(i);
                    if (!std::filesystem::exists(filteredQuadrantPath)) {
                        std::filesystem::create_directory(filteredQuadrantPath);
                    }
                    std::filesystem::path filteredBatchPath = filteredQuadrantPath / (std::to_string(batchId) + fileExtension);
                    // Write out filtered batch
                    ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTableToDisk(filteredBatchTable, filteredBatchPath));
                    std::cout << "[QuadTreePartitioning] Exported quadrant " << std::to_string(i) << " for batch " << batchId << std::endl;
                }
            }

            batchId += 1;
        }

        // Merge into 4 quadrants
        for (int i = 0; i < filterExpressions.size(); ++i) {
            std::filesystem::path quadrantPartsPath = subFolder / std::to_string(i);
            if (std::filesystem::exists(quadrantPartsPath)) {
                std::string rootPath;
                ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(quadrantPartsPath, &rootPath));
                ARROW_RETURN_NOT_OK(storage::DataWriter::mergeBatchesInFolder(fs, rootPath));
                std::filesystem::remove_all(quadrantPartsPath);
                std::cout << "[QuadTreePartitioning] Merged batches into 4 quadrants" << std::endl;
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
                std::ignore = partitionQuadrants(partitionFile, depth + 1);
            }
        }
        return arrow::Status::OK();
    }

}