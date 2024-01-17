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
        static arrow::Status mergeFiles(const std::filesystem::path &folder, const std::string &columnName,
                                        const uint32_t batchSize){
            /*
             * External Merge Sort: merge a set of sorted files on disk
             * https://en.wikipedia.org/wiki/K-way_merge_algorithm
             * Idea:
             * 1. Explore a folder with sorted Parquet file
             * 2. Load a vector of readers for those files. Also, keep track of the reading index within each reader
             * 3. Precompute the sum of number of rows of all the readers and maintain the current total read number of rows
             * 4. For each reader, load a fragment of rows. The fragment size is the batchSize / number of readers.
             *    This way, at each step we read a fixed-size block of data from each reader.
             *    We actually only read one column (the sorting column) for these rows, since we do not need the rest.
             *    The fragmentIndex points to the writing position in each file and gets updated after the read.
             * 5. The rows are loaded into a min-heap, to define an ordering on the sorting column.
             *    Together with the sorting value, also the index of the reader and the reading index of the reader are
             *    stored. Therefore, we have a min-heap with the first fragment of each file.
             * 6. We start popping from the min-heap, which returns the sorting values in ascending order.
             *    Together with the value, we retrieve the reader index and value index as well.
             *    The entire row is extracted using the two indexes and appended to a table.
             * 7. The process is repeated until the heap is empty. The obtained table will have then the rows of
             *    the first fragment of all files, in sorted order.
             * 8. The table is written out to disk
             * 9. Repeat from step 4 as long as the number of scanned rows is less than the total rows.
             *    E.g. the same will be done with the second fragment of each file, etc...
             */

            constexpr int readFinished = -1;

            // Prepare vector of file readers for each sorted batch
            // pair<arrow file reader, fragment start index>
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

            // Compute sum of rows of readers, so that later we can compare it with the total read rows
            uint32_t totalNumRows = 0;
            for (const auto &reader: readers){
                std::shared_ptr<arrow::RecordBatchReader> rb_reader;
                ARROW_RETURN_NOT_OK(reader.first->GetRecordBatchReader(&rb_reader));
                std::shared_ptr<arrow::RecordBatch> record_batch;
                while (true) {
                    RETURN_NOT_OK(rb_reader->ReadNext(&record_batch));
                    if (record_batch == nullptr) {
                        break;
                    }
                    totalNumRows += record_batch->num_rows();
                }
            }

            // Prepare a vector of empty columns matching the schema. It will be populated with the merged data
            if (numReaders > 0 ) {

                // Size of the read from a sorted file. Computed this way so that we merged fragments will be exactly
                // one batch, that can be directly written to disk
                uint32_t fragmentSize = batchSize / numReaders;

                // Prepare an array containing the columns of merge sorted batch
                std::shared_ptr<arrow::RecordBatchReader> firstReader;
                ARROW_RETURN_NOT_OK(readers.at(0).first->GetRecordBatchReader(&firstReader));
                auto schema = firstReader->schema();
                // Prepare mergedTable
                auto mergedTable = arrow::Table::MakeEmpty(schema, arrow::default_memory_pool()).ValueOrDie();

                // Determine the index of the ordering column
                auto sortingColumnIndex = schema->GetFieldIndex(columnName);
                // Make sure that the specified column exists in the schema
                assert(sortingColumnIndex != -1);

                uint32_t numRowsRead = 0;
                uint32_t completedReaders = 0;
                uint32_t partitionIndex = 0;
                // Termination conditions: the data from all readers is exhausted
                while(numRowsRead < totalNumRows) { // || completedReaders < numReaders){
                    // Initialize a priority queue, storing pairs of arrow values and related reader index
                    // The index is needed to identity the source batch and reconstruct the row order later
                    std::priority_queue<std::tuple<double, uint32_t, uint32_t>,
                                        std::vector<std::tuple<double, uint32_t, uint32_t>>,
                                        std::greater<>> q;
                    for (int i = 0; i < readers.size(); ++i) {
                        std::shared_ptr<arrow::RecordBatchReader> batchReader;
                        std::optional<std::pair<std::shared_ptr<parquet::arrow::FileReader>, uint32_t>> currentReader = readers.at(i);
                        if (currentReader->second != readFinished){
                            ARROW_RETURN_NOT_OK(currentReader->first->GetRecordBatchReader(&batchReader));
                            uint32_t fragmentStart = currentReader->second;
                            // Load fragment-sized values from the reader
                            // Use slice to select fragments of each batch
                            auto batchFragment = batchReader->ToRecordBatches()->at(0)->Slice(fragmentStart, fragmentSize);
                            auto fragmentNumRows = batchFragment->num_rows();
                            // If there is no more data to fetch from the reader, set the finished flag
                            if (fragmentNumRows == 0){
                                currentReader->second = readFinished;
                                completedReaders += 1;
                            }
                            // Otherwise, advance the reading index
                            else{
                                currentReader->second += fragmentNumRows;
                                numRowsRead += fragmentNumRows;
                            }
                            // Retrieve only the sorted column from the batch fragment
                            std::vector<std::shared_ptr<arrow::Array>> arrowArray = {batchFragment->column(sortingColumnIndex)};
                            // Convert the value to double
                            auto converter = common::ColumnDataConverter();
                            std::vector<std::shared_ptr<std::vector<double>>> castedArray = converter.toDouble({arrowArray}).ValueOrDie();
                            std::shared_ptr<std::vector<double>> castedColumn = castedArray.at(0);
                            assert(castedColumn->size() == batchFragment->num_rows());
                            for (int j = 0; j < castedColumn->size(); ++j){
                                auto columnValue = castedColumn->at(j);
                                // Push the value, the index of the reader and the fragment start index to the heap
                                std::tuple<double, uint32_t, uint32_t> tuple = std::make_tuple(columnValue, i, fragmentStart + j);
                                q.push(tuple);
                            }
                        }
                    }

                    // Popping from the min heap guarantees that the values are ordered
                    while(!q.empty()){
                        // Pop from the min heap, this will return the minimum
                        auto minTuple = q.top();
                        q.pop();
                        auto columnValue = std::get<0>(minTuple);
                        auto readerIndex = std::get<1>(minTuple);
                        auto fragmentIndex = std::get<2>(minTuple);
                        // Information about the batch source
                        auto reader = readers.at(readerIndex).first;

                        // Extract matching rows by
                        std::shared_ptr<arrow::RecordBatchReader> sourceBatchReader;
                        ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(&sourceBatchReader));
                        auto batchFragment = sourceBatchReader->ToRecordBatches()->at(0)->Slice(fragmentIndex, 1);
                        std::shared_ptr<arrow::Table> rowTable = arrow::Table::FromRecordBatches(schema, {batchFragment}).ValueOrDie();
                        // Append the row to the mergedTable
                        mergedTable = arrow::ConcatenateTables({mergedTable, rowTable}).ValueOrDie();
                    }
                    // Export the batch to disk
                    std::shared_ptr<arrow::io::FileOutputStream> partitionFile;
                    auto partitionFilePath = folder / (std::to_string(partitionIndex) + ".parquet");
                    ARROW_ASSIGN_OR_RAISE(partitionFile, arrow::io::FileOutputStream::Open(partitionFilePath));
                    // Prepare the Parquet writer
                    std::unique_ptr<parquet::arrow::FileWriter> writer;
                    ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*mergedTable->schema(),
                                                                                   arrow::default_memory_pool(), partitionFile));
                    // Write the batch and close the file
                    ARROW_RETURN_NOT_OK(writer->WriteTable(*mergedTable));
                    ARROW_RETURN_NOT_OK(writer->Close());
                    partitionIndex += 1;
                }
            }
            return arrow::Status::OK();
        }
    private:
    };
}

#endif //EXTERNAL_MERGE_H
