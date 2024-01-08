#ifndef EXTERNAL_MERGE_H
#define EXTERNAL_MERGE_H

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
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

namespace external {

    class ExternalMerge {
    public:
        static arrow::Status mergeFiles(const std::filesystem::path &folder, std::string &columnName){
            uint32_t batchSize = 64 * 1024;
            // Prepare vector of file readers for each sorted batch
            std::vector<std::pair<std::shared_ptr<arrow::RecordBatchReader>, uint32_t>> readers;
            parquet::arrow::FileReaderBuilder reader_builder;
            // List files in folder
            for (const auto &folderFile : std::filesystem::directory_iterator(folder)) {
                std::filesystem::path filePath = folderFile.path();
                if (filePath.extension() == ".parquet"){
                    ARROW_RETURN_NOT_OK(reader_builder.OpenFile(filePath, /*memory_map=*/false));
                    std::shared_ptr<parquet::arrow::FileReader> reader;
                    ARROW_ASSIGN_OR_RAISE(reader, reader_builder.Build());
                    std::shared_ptr<arrow::RecordBatchReader> rbReader;
                    ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(&rbReader));
                    readers.emplace_back(rbReader, 0);
                }
            }

            // Prepare a vector of empty columns matching the schema. It will be populated with the merged data
            if (readers.size() > 0 ) {
                std::vector<std::shared_ptr<arrow::Array>> newBatchColumns;
                auto reader = readers.at(0).first;
                auto schema = reader->schema();
                auto sortingColumnIndex = schema->GetFieldIndex(columnName);
                auto empty_batch = arrow::RecordBatch::MakeEmpty(schema);
                auto fields = schema->fields();
                for (int i = 0; i < fields.size(); ++i) {
                    arrow::Result<std::shared_ptr<arrow::Array>> columnArray;
                    ARROW_ASSIGN_OR_RAISE(reader->ToRecordBatches()->at(0)->Slice(0, 1)->column(i), columnArray);
                    columnArray->reset();
                    newBatchColumns.emplace_back(columnArray.ValueOrDie());
                }

                while(readers.size() > 0) {
                    uint32_t usedBatchRows = 0;
                    // Use slice to select parts of each batch
                    while(usedBatchRows < batchSize){
                        // Find the minimum from the all the readers and keep track of the corresponding index
                        auto min = arrow::MakeScalar(std::numeric_limits<uint32_t>::max());
                        auto minReaderIndex = 0;
                        for (int i = 0; i < readers.size(); ++i) {
                            auto batchReader = readers.at(i).first;
                            auto batchReaderIndex = readers.at(i).second;
                            auto readerSlice = batchReader->ToRecordBatches()->at(0)->Slice(batchReaderIndex, 1);
                            auto readerValue = readerSlice->column(sortingColumnIndex)->GetScalar(0).ValueOrDie();
                            if (readerValue < min){
                                minReaderIndex = i;
                                min = readerValue;
                            }
                        }
                        // Extract the rows (by reading the column values at index)
                        for (int i = 0; i <schema->num_fields(); ++i) {

                        }
                        // Advance the pointer of the reader containing the minimum

                        usedBatchRows += 1;
                    }
                }
            }

        }
    private:
    };
}

#endif //EXTERNAL_MERGE_H
