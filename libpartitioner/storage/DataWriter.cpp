#include <iostream>
#include <filesystem>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include "storage/DataWriter.h"

namespace storage {

    arrow::Status DataWriter::WriteTable(std::shared_ptr<arrow::Table>& table,
                                         std::string &filename,
                                         std::filesystem::path &outputFolder) {
        auto debug = table->ToString();
        std::string outPath = outputFolder.string();
        std::string outFilename = outPath + "/" + filename;
        auto outfile = arrow::io::FileOutputStream::Open(outFilename);
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outfile, table->num_rows()));
        std::cout << "[DataWriter] Written table to " << outFilename << std::endl;
        return arrow::Status::OK();
    }

    arrow::Status DataWriter::WritePartitions(std::vector<std::shared_ptr<arrow::Table>>& partitions,
                                              std::string &tableName,
                                              std::filesystem::path &outputFolder) {
        if (!std::filesystem::is_directory(outputFolder)){
            std::filesystem::create_directory(outputFolder);
        }
        for (int32_t i = 0; i < partitions.size(); i++) {
            std::string filename = tableName + std::to_string(i) + ".parquet";
            ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTable(partitions[i], filename, outputFolder));
        }
        return arrow::Status::OK();
    }

    // TODO: get data writer with correct settings
    std::shared_ptr<parquet::ParquetFileWriter> DataWriter::getWriter(){
        std::shared_ptr<parquet::ParquetFileWriter> writer;
        std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder()
                .max_row_group_length(100000)
                ->created_by("Optimal Layout Partitioner")
                ->version(parquet::ParquetVersion::PARQUET_2_6)
                ->data_page_version(parquet::ParquetDataPageVersion::V2)
                ->compression(arrow::Compression::SNAPPY)
                ->build();
        return writer;
    }

    std::shared_ptr<parquet::ArrowWriterProperties> DataWriter::getProperties(){
        // Options to store Arrow schema for easier reads back into Arrow
        return parquet::ArrowWriterProperties::Builder().store_schema()->build();
    }

} // storage