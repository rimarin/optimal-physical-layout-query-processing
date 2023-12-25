#include <iostream>
#include <filesystem>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include "storage/DataWriter.h"

namespace storage {

    arrow::Status DataWriter::WriteTable(std::shared_ptr<arrow::Table>& table,
                                         std::filesystem::path &outputPath) {
        auto outfile = arrow::io::FileOutputStream::Open(outputPath);
        std::unique_ptr<parquet::arrow::FileWriter> writer;
        std::shared_ptr<parquet::WriterProperties> props = getWriterProperties();
        std::shared_ptr<parquet::ArrowWriterProperties> arrow_props = getArrowWriterProperties();
        PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outfile,
                                                         table->num_rows(), props, arrow_props));
        std::cout << "[DataWriter] Written table to " << outputPath << std::endl;
        return arrow::Status::OK();
    }

    std::shared_ptr<parquet::WriterProperties> DataWriter::getWriterProperties(){
        std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder()
                .max_row_group_length(100000)
                ->created_by("Optimal Layout Partitioner")
                ->version(parquet::ParquetVersion::PARQUET_2_6)
                ->data_page_version(parquet::ParquetDataPageVersion::V2)
                ->compression(arrow::Compression::SNAPPY)
                ->build();
        return props;
    }

    std::shared_ptr<parquet::ArrowWriterProperties> DataWriter::getArrowWriterProperties(){
        // Options to store Arrow schema for easier reads back into Arrow
        return parquet::ArrowWriterProperties::Builder().store_schema()->build();
    }

} // storage