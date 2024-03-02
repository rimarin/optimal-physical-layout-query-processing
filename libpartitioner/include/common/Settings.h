#ifndef COMMON_SETTINGS_H
#define COMMON_SETTINGS_H

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <iostream>
#include <parquet/arrow/writer.h>
#include <vector>

namespace common {
    // Struct for storing the common configuration across the library components
    struct Settings{
        // Parquet settings
        inline static const std::string fileExtension = ".parquet";
        inline static const std::string libraryName = "Optimal Layout Partitioner";
        // Row group size is the optimal one
        inline static const int64_t rowGroupSize = 131072;
        // Adjusted depending on the hardware, generally matching the row group size
        inline static const size_t batchSize = rowGroupSize * 7;
        inline static const size_t bufferSize = 4096 * 4;
        inline static const parquet::ParquetVersion::type version = parquet::ParquetVersion::PARQUET_2_6;
        inline static const arrow::Compression::type compression = arrow::Compression::SNAPPY;
        // DuckDB config
        static inline const std::string memoryLimit = "300GB";
        static inline const std::string tempDirectory = "/tmp";
    };
}

#endif //COMMON_SETTINGS_H
