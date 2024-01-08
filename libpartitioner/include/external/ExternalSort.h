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

namespace external {

    class ExternalSort {
    public:
        static arrow::Status writeSorted(const std::shared_ptr<arrow::RecordBatch> &recordBatch,
                                         const std::string &sortColumn,
                                         const std::filesystem::path &outputPath){
            // Sort by the provided column name
            ARROW_ASSIGN_OR_RAISE(auto sort_indices,
                                 arrow::compute::SortIndices(recordBatch->GetColumnByName(sortColumn),
                                 arrow::compute::SortOptions({arrow::compute::SortKey{sortColumn}})));
            ARROW_ASSIGN_OR_RAISE(arrow::Datum sorted, arrow::compute::Take(recordBatch, sort_indices));
            // Prepare the output file
            std::shared_ptr<arrow::io::FileOutputStream> outfile;
            ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(outputPath.string()));
            // Prepare the Parquet writer
            std::unique_ptr<parquet::arrow::FileWriter> writer;
            ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*recordBatch->schema(), arrow::default_memory_pool(), outfile));
            // Write the batch and close the file
            ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*sorted.record_batch()));
            ARROW_RETURN_NOT_OK(writer->Close());
            return arrow::Status::OK();
        }
    private:
    };
}

#endif //EXTERNAL_SORT_H
