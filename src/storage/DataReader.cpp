#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include "../include/storage/DataReader.h"

namespace storage {

    arrow::Result<std::shared_ptr<arrow::Table>> DataReader::ReadTable(std::filesystem::path &inputFile) {
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::shared_ptr<arrow::io::RandomAccessFile> input;
        ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(inputFile));

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

        std::shared_ptr<arrow::Table> table;
        arrow::Status result;
        result = arrow_reader->ReadTable(&table);
        std::cout << "[DataReader] Read table from file " << inputFile.string() << std::endl;
        return table;
    }

} // storage