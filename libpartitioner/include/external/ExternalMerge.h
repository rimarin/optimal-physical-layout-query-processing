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
        static arrow::Status mergeFiles(const std::filesystem::path &folder, const std::string &columnName){
            /*
             * External Merge Sort: merge a set of sorted files on disk
             * https://en.wikipedia.org/wiki/K-way_merge_algorithm
             */
            // Handle a standard batch size of 65536
            uint32_t batchSize = 64 * 1024;

            // Prepare vector of file readers for each sorted batch
            std::vector<std::pair<std::shared_ptr<parquet::arrow::FileReader>, uint32_t>> readers;
            parquet::arrow::FileReaderBuilder reader_builder;

            // List files in folder
            for (const auto &folderFile : std::filesystem::directory_iterator(folder)) {
                const std::filesystem::path& filePath = folderFile.path();
                if (filePath.extension() == ".parquet"){
                    // Define properties for the file readers
                    auto reader_properties = parquet::ReaderProperties(arrow::default_memory_pool());
                    reader_properties.set_buffer_size(4096 * 4);
                    reader_properties.enable_buffered_stream();
                    auto arrow_reader_props = parquet::ArrowReaderProperties();
                    arrow_reader_props.set_batch_size(batchSize);  // default 64 * 1024
                    ARROW_RETURN_NOT_OK(reader_builder.OpenFile(filePath, /*memory_map=*/false, reader_properties));
                    reader_builder.memory_pool(arrow::default_memory_pool());
                    reader_builder.properties(arrow_reader_props);
                    std::shared_ptr<parquet::arrow::FileReader> reader;
                    ARROW_ASSIGN_OR_RAISE(reader, reader_builder.Build());
                    readers.emplace_back(reader, 0);
                }
            }

            auto numReaders = readers.size();

            // Prepare a vector of empty columns matching the schema. It will be populated with the merged data
            if (numReaders > 0 ) {

                // Size of the read from a sorted file. Computed this way so that we merged parts will be exactly
                // one batch, that can be directly written to disk
                uint32_t partSize = batchSize / numReaders;

                // Prepare an array containing the columns of merge sorted batch
                std::shared_ptr<arrow::RecordBatchReader> firstReader;
                ARROW_RETURN_NOT_OK(readers.at(0).first->GetRecordBatchReader(&firstReader));
                auto schema = firstReader->schema();
                // Prepare mergedTable
                auto mergedTable = arrow::Table::MakeEmpty(schema, arrow::default_memory_pool()).ValueOrDie();

                // Determine the index of the ordering column
                auto sortingColumnIndex = schema->GetFieldIndex(columnName);

                uint32_t usedBatchRows = 0;
                // Use slice to select parts of each batch
                while(usedBatchRows < batchSize){
                    // Initialize a priority queue, storing pairs of arrow values and related reader index
                    // The index is needed to identity the source batch and reconstruct the row order later
                    std::priority_queue<std::pair<double, uint32_t>,
                                        std::vector<std::pair<double, uint32_t>>,
                                        std::greater<>> q;
                    for (int i = 0; i < readers.size(); ++i) {
                        std::shared_ptr<arrow::RecordBatchReader> batchReader;
                        ARROW_RETURN_NOT_OK(readers.at(i).first->GetRecordBatchReader(&batchReader));
                        // auto batchReader = readers.at(i).first->GetRecordBatchReader();
                        uint32_t batchReaderIndex = readers.at(i).second;
                        // Load column values from each reader
                        auto batchPart = batchReader->ToRecordBatches()->at(0)->Slice(batchReaderIndex, partSize);
                        // TODO: keep track of readerSlice to know when to terminate (=sum of readerslices size = total rows read)
                        auto numRows = batchPart->num_rows();
                        // Retrieve only the sorted column from the batch part
                        std::vector<std::shared_ptr<arrow::Array>> arrowArray = {batchPart->column(sortingColumnIndex)};
                        // Convert the value to double
                        auto converter = common::ColumnDataConverter();
                        std::vector<std::shared_ptr<std::vector<double>>> castedArray = converter.toDouble({arrowArray}).ValueOrDie();
                        std::shared_ptr<std::vector<double>> castedColumn = castedArray.at(0);
                        for (const auto &columnValue: *castedColumn){
                            // Push the value and the index of the reader to the heap
                            std::pair<double, uint32_t> pair = std::make_pair(columnValue, batchReaderIndex);
                            q.push(pair);
                        }
                    }

                    // Popping from the min heap guarantees that the values are ordered
                    while(!q.empty()){
                        // Pop from the min heap, this will return the minimum
                        auto min = q.top();
                        q.pop();
                        auto readerIndex = min.second;
                        auto readerAndPointer = readers.at(readerIndex);
                        // Information about the batch source
                        auto reader = readerAndPointer.first;
                        // Information about the scanning position within the batch source
                        auto pointer = readerAndPointer.second;

                        // Extract matching rows by
                        std::shared_ptr<arrow::RecordBatchReader> sourceBatchReader;
                        ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(&sourceBatchReader));
                        auto batchPart = sourceBatchReader->ToRecordBatches()->at(0)->Slice(pointer, 1);
                        std::shared_ptr<arrow::Table> rowTable = arrow::Table::FromRecordBatches(schema, {batchPart}).ValueOrDie();
                        // Append the row to the mergedTable
                        mergedTable = arrow::ConcatenateTables({mergedTable, rowTable}).ValueOrDie();
                        // In the end should be partSize * readers.size()
                        usedBatchRows += 1;
                    }
                }
                // Result<std::shared_ptr<Table>> arrow::ConcatenateTables(const std::vector<std::shared_ptr<Table>> &tables, ConcatenateTablesOptions options = ConcatenateTablesOptions::Defaults(), MemoryPool *memory_pool = default_memory_pool())

                // Export the batch to disk
                std::shared_ptr<arrow::io::FileOutputStream> outfile;
                ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(folder.string()));
                // Prepare the Parquet writer
                std::unique_ptr<parquet::arrow::FileWriter> writer;
                ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*mergedTable->schema(), arrow::default_memory_pool(), outfile));
                // Write the batch and close the file
                ARROW_RETURN_NOT_OK(writer->WriteTable(*mergedTable));
                ARROW_RETURN_NOT_OK(writer->Close());

            }
            return arrow::Status::OK();
        }
    private:
    };
}

#endif //EXTERNAL_MERGE_H
