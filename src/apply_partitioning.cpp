#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/FixedGridPartitioning.h"
#include "include/partitioning/KDTreePartitioning.h"
#include "include/partitioning/NoPartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"


int main() {
    // Prepare folder paths for all the datasets
    std::string datasetFolder = "../data/";
    std::string taxiFolder = datasetFolder + "taxi/";

    // For each dataset
        // Read all parquet files in folder, with something like this:
        // https://stackoverflow.com/questions/69486535/how-to-read-multiple-parquet-files-or-a-directory-using-apache-arrow-in-cpp

        // Apply partitioning techniques and write out tables
        // 1. FixedGrid
        // 2. GridFile
        // 3. kd-Tree
        // 4. QuadTree
        // 5. STRTree
        // 6. Hilbert Curve
        // 7. Z-order Curve

    return 0;
}
