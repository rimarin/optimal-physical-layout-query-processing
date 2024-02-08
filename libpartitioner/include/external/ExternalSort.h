#ifndef EXTERNAL_SORT_H
#define EXTERNAL_SORT_H

#include <iostream>
#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>

#include "storage/DataWriter.h"

namespace external {

    class ExternalSort {
    public:
        static arrow::Status writeSortedBatch(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                              const std::string &sortColumn,
                                              const std::filesystem::path &outputPath){
            /*
             * Helper method to sort a RecordBatch by a certain column and export it to a Parquet file
             */
            // Sort by the provided column name
            std::cout << "[ExternalSort] Starting to write sorted file " << outputPath << " for sort column "
                      << sortColumn << std::endl;
            ARROW_ASSIGN_OR_RAISE(auto sort_indices,
                                 arrow::compute::SortIndices(recordBatch->GetColumnByName(sortColumn),
                                 arrow::compute::SortOptions({arrow::compute::SortKey{sortColumn}})));
            ARROW_ASSIGN_OR_RAISE(arrow::Datum sorted, arrow::compute::Take(recordBatch, sort_indices));
            // Prepare the output file
            std::shared_ptr<arrow::io::FileOutputStream> outfile;
            ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(outputPath.string()));
            // Prepare the Parquet writer
            std::unique_ptr<parquet::arrow::FileWriter> writer;
            ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*recordBatch->schema(),
                                                                           arrow::default_memory_pool(),
                                                                           outfile,
                                                                           storage::DataWriter::getWriterProperties(),
                                                                           storage::DataWriter::getArrowWriterProperties()));
            // Write the batch and close the file
            std::cout << "[ExternalSort] Writing " << sorted.record_batch()->num_rows() << " rows" << std::endl;
            ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*sorted.record_batch()));
            ARROW_RETURN_NOT_OK(writer->Close());
            std::cout << "[ExternalSort] Completed, written sorted file to " << outputPath << std::endl;
            return arrow::Status::OK();
        }
        static arrow::Status writeSortedFile(const std::filesystem::path &inputPath,
                                             const std::string &sortColumn,
                                             const std::filesystem::path &outputPath) {
            // Initialize DuckDB
            duckdb::DuckDB db(nullptr);
            duckdb::Connection con(db);

            // Load parquet file into memory and sort it
            std::string loadQuery = "CREATE TABLE tbl AS SELECT * FROM read_parquet('" + inputPath.string() + "') ORDER BY " + sortColumn;
            auto loadQueryResult = con.Query(loadQuery);

            // Write table to disk
            std::string exportQuery = "COPY (SELECT * FROM tbl) TO '" + outputPath.string() + "' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 131072)";
            auto exportQueryResult = con.Query(exportQuery);
            return arrow::Status::OK();
        }
    private:
    };
}

#endif //EXTERNAL_SORT_H
