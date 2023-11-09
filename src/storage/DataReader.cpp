#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include "../include/storage/DataReader.h"

namespace storage {

    arrow::Result<std::shared_ptr<arrow::Table>> DataReader::readTable(std::filesystem::path &inputFile) {
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

    arrow::Result<std::vector<std::shared_ptr<arrow::Array>>>
    DataReader::getColumns(std::shared_ptr<arrow::Table> &table, std::vector<std::string> &columns) {
        std::vector<arrow::compute::InputType> inputTypes;
        std::vector<std::shared_ptr<arrow::Array>> columnData;
        for (const auto &column: columns){
            // Infer the data types of the columns
            auto columnType = table->schema()->GetFieldByName(column)->type();
            assert(("FixedGrid supports only int32 column type at the moment", columnType == arrow::int32()));
            std::cout << "Reading column <" << column << "> of type " << columnType->ToString() << std::endl;
            inputTypes.emplace_back(columnType);
            // Extract column data by getting the chunks and casting them to an arrow array
            auto chunkedColumn = table->GetColumnByName(column);
            auto chunk = chunkedColumn->chunk(0);
            columnData.emplace_back(chunk);
        }
        return columnData;
    }

} // storage