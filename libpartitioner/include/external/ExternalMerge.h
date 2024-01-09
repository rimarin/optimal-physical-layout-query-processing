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
                std::vector<std::shared_ptr<arrow::Array>> newBatchColumns;
                std::shared_ptr<arrow::RecordBatchReader> firstReader;
                ARROW_RETURN_NOT_OK(readers.at(0).first->GetRecordBatchReader(&firstReader));
                auto schema = firstReader->schema();
                auto fields = schema->fields();
                newBatchColumns.reserve(fields.size());

                // Determine the index of the ordering column
                auto sortingColumnIndex = schema->GetFieldIndex(columnName);

                /*
                struct LowestBatchIndexAtTop {
                    bool operator()(const std::pair<arrow::Scalar, uint32_t>& left, const std::pair<arrow::Scalar, uint32_t>& right) const {
                        return left.first.get().CastTo(arrow::Int64Type) > right.first.CastTo(arrow::Int32Type);
                    }
                };
                */

                uint32_t usedBatchRows = 0;
                // Use slice to select parts of each batch
                while(usedBatchRows < batchSize){
                    // Initialize a priority queue, storing pairs of arrow values and related reader index
                    // The index is needed to identity the source batch and reconstruct the row order later
                    std::priority_queue<std::pair<double, uint32_t>,
                                        std::vector<std::pair<double, uint32_t>>,
                                        std::greater<std::pair<double, uint32_t>>> q;
                    for (int i = 0; i < readers.size(); ++i) {
                        std::shared_ptr<arrow::RecordBatchReader> batchReader;
                        ARROW_RETURN_NOT_OK(readers.at(i).first->GetRecordBatchReader(&batchReader));
                        // auto batchReader = readers.at(i).first->GetRecordBatchReader();
                        uint32_t batchReaderIndex = readers.at(i).second;
                        // Load column values from each reader
                        auto readerSlice = batchReader->ToRecordBatches()->at(0)->Slice(batchReaderIndex, partSize);
                        // TODO: keep track of readerSlice to know when to terminate (=sum of readerslices size = total rows read)
                        auto numRows = readerSlice->num_rows();
                        auto type = std::make_shared<arrow::DoubleType>();
                        auto readerValue = readerSlice->column(sortingColumnIndex);
                        auto arrayData = readerValue->data();
                        auto new_data = readerValue->data()->Copy();
                        new_data->type = arrow::float64();
                        arrow::DoubleArray double_arr(new_data);
                        auto arrow_int32_array = std::static_pointer_cast<arrow::Int32Array>(readerValue);
                        auto value = arrow_int32_array->Value(0);
                        auto valueDouble = std::static_pointer_cast<arrow::DoubleArray>(readerValue);
                        auto d = double_arr.Value(0);
                        auto c = valueDouble->Value(0);
                        // Push the value and the index of the reader to the heap
                        int a = 5;
                        // std::pair<double, uint32_t> pair = std::make_pair(valueDouble, batchReaderIndex);
                        // q.push(pair);
                    }

                    while(!q.empty()){
                        auto min = q.top();
                        q.pop();
                        auto readerIndex = min.second;
                        auto readerAndPointer = readers.at(readerIndex);
                        auto reader = readerAndPointer.first;
                        auto pointer = readerAndPointer.second;
                        // Extract the rows (by reading the column values at index)
                        for (int i = 0; i <schema->num_fields(); ++i) {
                            // For each column, add the value of each field to the array newBatchColumns
                            // Slice for reader from pointers
                            // Add results to newBatchRow
                        }
                        // Append the newBatchRow to the newBatchColumns
                        // In the end should be partSize * readers.size()
                        usedBatchRows += 1;
                    }
                }
                // Load newBatchColumn into a batch
                auto newRecordBatch = arrow::RecordBatch::Make(schema,
                                                               newBatchColumns.at(0)->length(),
                                                               newBatchColumns);
                // Export the batch to disk
                std::shared_ptr<arrow::io::FileOutputStream> outfile;
                ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(folder.string()));
                // Prepare the Parquet writer
                std::unique_ptr<parquet::arrow::FileWriter> writer;
                ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*newRecordBatch->schema(), arrow::default_memory_pool(), outfile));
                // Write the batch and close the file
                ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*newRecordBatch));
                ARROW_RETURN_NOT_OK(writer->Close());

            }
            return arrow::Status::OK();
        }
    private:
    };
}

#endif //EXTERNAL_MERGE_H
