#include <algorithm>
#include <chrono>

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
        std::cout << "Computing linear scales, depth " << depth << std::endl;
        std::ignore = dataReader->load(partitionFile);
        auto currentNumRows = dataReader->getNumRows();
        if (currentNumRows == 0){
            return;
        }
        if (depth > 150){
            std::cout << "[GridFilePartitioning] Have to stop, we reached excessive depth" << std::endl;
            while(!std::filesystem::exists(partitionFile)){
                if (partitionFile.parent_path() == "/"){
                    return;
                }
                partitionFile = partitionFile.parent_path().parent_path() / partitionFile.filename().string();
            }
            // Rename the processed slice file
            auto basePath = partitionFile.parent_path();
            auto renamedDatasetFile = basePath / ("completed" + partitionFile.filename().string());
            std::filesystem::rename(partitionFile, renamedDatasetFile);
            return;
        }
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
        double minValue;
        double maxValue;
        double midValue;
        auto dimensionRanges1 = dimensionRanges;
        auto dimensionRanges2 = dimensionRanges;
        std::cout << "[GridFilePartitioning] Preparing split on column " << columnName << std::endl;
        minValue = dimensionRanges.at(columnIndex).first;
        maxValue = dimensionRanges.at(columnIndex).second;
        midValue = (maxValue + minValue) / 2;
        std::cout << "[GridFilePartitioning] Splitting left branch on mid value " << midValue << std::endl;

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
        std::string whereClause;
        auto destinationFile1 = subFolder / ("0" + fileExtension);
        minValue = dimensionRanges.at(columnIndex).first;
        maxValue = dimensionRanges.at(columnIndex).second;
        midValue = (maxValue + minValue) / 2;
        dimensionRanges1.at(columnIndex).first = minValue;
        dimensionRanges1.at(columnIndex).second = midValue;
        // TODO: replace with inferring types from schema. Timestamps and datetimes shall be detected
        //  This is a temporary patch
        std::set<std::string> timeColumns = {"tpep_pickup_datetime", "tpep_dropoff_datetime",
                                             "created_at",
                                             "o_orderdate", "l_shipdate"};
        if (timeColumns.find(columnName) != timeColumns.end()){

            std::string minValueTS = getTimestamp(minValue);
            std::string midValueTS = getTimestamp(midValue);

            whereClause = "      WHERE " + columnName + " >= '" + minValueTS + "' AND "
                                         + columnName + " <= '" + midValueTS + "') ";
        }
        else{
            whereClause = "      WHERE " + columnName + " >= " + std::to_string(minValue) + " AND "
                                         + columnName + " <= " + std::to_string(midValue) + ") ";
        }
        std::string filterQuery1 = "COPY (SELECT * "
                                   "      FROM tbl " + whereClause +
                                   "TO '" + destinationFile1.string() + "' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 131072)";
        auto filterQueryResult1 = con.Query(filterQuery1);

        auto destinationFile2 = subFolder / ("1" + fileExtension);
        minValue = dimensionRanges.at(columnIndex).first;
        maxValue = dimensionRanges.at(columnIndex).second;
        midValue = (maxValue + minValue) / 2;
        dimensionRanges2.at(columnIndex).first = midValue;
        dimensionRanges2.at(columnIndex).second = maxValue;
        if (timeColumns.find(columnName) != timeColumns.end()){
            std::string midValueTS = getTimestamp(midValue);
            std::string maxValueTS = getTimestamp(maxValue);
            whereClause = "      WHERE " + columnName + " > '" + midValueTS + "' AND "
                                         + columnName + " <= '" + maxValueTS + "') ";
        }
        else{
            whereClause = "      WHERE " + columnName + " > " + std::to_string(midValue) + " AND "
                                         + columnName + " <= " + std::to_string(maxValue) + ") ";
        }
        std::string filterQuery2 = "COPY (SELECT * "
                                   "      FROM tbl " + whereClause +
                                   "TO '" + destinationFile2.string() + "' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 131072)";
        auto filterQueryResult2 = con.Query(filterQuery2);

        computeLinearScales(destinationFile1, depth + 1, dimensionRanges1);
        computeLinearScales(destinationFile2, depth + 1, dimensionRanges2);
    }

    // Trick from https://stackoverflow.com/questions/31000677/convert-double-to-struct-tm
    std::string GridFilePartitioning::getTimestamp(double value) {
        time_t timeValue = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(
                    std::chrono::duration_cast<std::chrono::seconds>(std::chrono::duration<double>(value))));
        struct tm tmValue = *gmtime(&timeValue);
        char buffer[20];
        strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        std::string valueTimestamp1(buffer);
        return valueTimestamp1;
    }

}