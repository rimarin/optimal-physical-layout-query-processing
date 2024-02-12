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
        inline static const std::string fileExtension = ".parquet";
        inline static const std::string libraryName = "Optimal Layout Partitioner";
        inline static const size_t batchSize = 64 * 1024 * 2;
        // Matching row group size and batch size = 131072
        inline static const int64_t rowGroupSize = batchSize;
        inline static const size_t bufferSize = 4096 * 4;
        inline static const parquet::ParquetVersion::type version = parquet::ParquetVersion::PARQUET_2_6;
        inline static const arrow::Compression::type compression = arrow::Compression::SNAPPY;
    };
}

#endif //COMMON_SETTINGS_H
