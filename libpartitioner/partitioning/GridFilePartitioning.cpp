#include <algorithm>

#include "partitioning/GridFilePartitioning.h"

namespace partitioning {

    arrow::Status GridFilePartitioning::partition(){
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
         * Idea:
         * 1. Read only partitioning columns
         * 2. Create the linear scales
         * 3. Read the dataset by batches
         * 4. For each batch, assign the right bucket by using the created scales
         *      generate cellsCoordinates from the scales and filter
         *      or (better) read from batches the row index matching the computed row indexes
         *  5. Export the batch into different partition files
         *  6. Merge the batches into partition files
        */

        // Copy original files to destination and work there
        // This way all the batch readers will point to the right folder from the start
        ARROW_RETURN_NOT_OK(copyOriginalToDestination());

        // Initialize the reader, slice size and column index
        auto datasetFile = folder / ("0" + fileExtension);
        std::ignore = dataReader->load(datasetFile);

        // Retrieve domain values
        std::vector<std::pair<double, double>> dimensionRanges;
        for (const auto &column: columns){
            auto minMaxColumn = dataReader->getColumnStats(column);
            dimensionRanges.emplace_back(minMaxColumn->first, minMaxColumn->second);
        }

        // Compute linear scales
        auto datasetPath = dataReader->getReaderPath();
        computeLinearScales(datasetPath, 0, dimensionRanges);

        // Finalize the files
        deleteIntermediateFiles();
        moveCompletedFiles();
        deleteSubfolders();

        return arrow::Status::OK();
    }

    void GridFilePartitioning::computeLinearScales(std::filesystem::path &partitionFile,
                                                   const uint32_t depth,
                                                   const std::vector<std::pair<double, double>> &dimensionRanges) {

        // Base case: reached partition size
        std::ignore = dataReader->load(partitionFile);
        auto currentNumRows = dataReader->getNumRows();;
        if (currentNumRows <= cellCapacity){
            // Rename the processed slice file
            auto basePath = partitionFile.parent_path();
            auto renamedDatasetFile = basePath / ("completed" + partitionFile.filename().string());
            std::filesystem::rename(partitionFile, renamedDatasetFile);
            return;
        }

        // Extract partition id from the file name
        std::string filename = partitionFile.filename();
        size_t lastIndex = filename.find_last_of('.');
        std::string partitionId = filename.substr(0, lastIndex);

        // Generate new sub folder (with this partition id) for this processing iteration
        auto baseFolder = partitionFile.parent_path();
        auto subFolder = baseFolder / partitionId;
        if (!std::filesystem::exists(subFolder)) {
            std::filesystem::create_directory(subFolder);
        }

        // Find mid-point
        uint32_t columnIndex = depth % numColumns;
        std::string columnName = columns.at(columnIndex);
        double minValue = dimensionRanges.at(columnIndex).first;
        double maxValue = dimensionRanges.at(columnIndex).second;
        double midValue = (maxValue + minValue) / 2;

        // Split and recurse
        // Setup DuckDB
        duckdb::DBConfig config;
        config.SetOption("memory_limit", memoryLimit);
        config.SetOption("temp_directory", tempDirectory);
        duckdb::DuckDB db(":memory:", &config);
        duckdb::Connection con(db);

        // Load parquet file into memory
        std::string loadQuery = "CREATE TABLE tbl AS "
                                "SELECT * FROM read_parquet('" + partitionFile.string() + "')";
        auto loadQueryResult = con.Query(loadQuery);

        // Apply filter
        auto dimensionRanges1 = dimensionRanges;
        dimensionRanges1.at(columnIndex).first = minValue;
        dimensionRanges1.at(columnIndex).second = midValue;
        auto destinationFile1 = subFolder / ("0" + fileExtension);
        std::string filterQuery1 = "COPY (SELECT * "
                                  "      FROM tbl "
                                  "      WHERE " + columnName + " >= " + std::to_string(minValue) + " AND "
                                                 + columnName + " <= " + std::to_string(midValue) + ") "
                                  "TO '" + destinationFile1.string() + "' (FORMAT PARQUET)";
        auto filterQueryResult1 = con.Query(filterQuery1);

        auto dimensionRanges2 = dimensionRanges;
        dimensionRanges2.at(columnIndex).first = midValue;
        dimensionRanges2.at(columnIndex).second = maxValue;
        auto destinationFile2 = subFolder / ("1" + fileExtension);
        std::string filterQuery2 = "COPY (SELECT * "
                                   "      FROM tbl "
                                   "      WHERE " + columnName + " > " + std::to_string(midValue) + " AND "
                                                  + columnName + " <= " + std::to_string(maxValue) + ") "
                                   "TO '" + destinationFile2.string() + "' (FORMAT PARQUET)";
        auto filterQueryResult2 = con.Query(filterQuery2);

        computeLinearScales(destinationFile1, depth + 1, dimensionRanges1);
        computeLinearScales(destinationFile2, depth + 1, dimensionRanges2);
    }

}