#ifndef EXTERNAL_MERGE_H
#define EXTERNAL_MERGE_H

#include <iostream>
#include <filesystem>
#include <map>
#include <regex>
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

#include "common/Settings.h"
#include "partitioning/Partitioning.h"
#include "storage/DataWriter.h"

namespace external {
    struct ExternalFileReader{
        ExternalFileReader(std::shared_ptr<parquet::arrow::FileReader> &_fileReader,
                           std::shared_ptr<arrow::RecordBatchReader> &_recordBatchReader,
                           std::shared_ptr<arrow::RecordBatch> &_recordBatch,
                           int _recordBatchIndex, int _fragmentStartIndex, uint32_t _numRows):
                           fileReader(_fileReader), recordBatchReader(_recordBatchReader), recordBatch(_recordBatch),
                           recordBatchIndex(_recordBatchIndex), fragmentStartIndex(_fragmentStartIndex),
                           numRows(_numRows) {};
        std::shared_ptr<parquet::arrow::FileReader> fileReader;
        std::shared_ptr<arrow::RecordBatchReader> recordBatchReader;
        std::shared_ptr<arrow::RecordBatch> recordBatch;
        uint32_t recordBatchIndex;
        uint32_t fragmentStartIndex;
        uint32_t numRows;
    };

    struct ExternalRow{
        ExternalRow(double &_columnValue, uint32_t _readerIndex, uint32_t _recordBatchIndex, uint32_t _fragmentStartIndex):
                    columnValue(_columnValue), readerIndex(_readerIndex), recordBatchIndex(_recordBatchIndex),
                    fragmentStartIndex(_fragmentStartIndex) {};
        double columnValue;
        uint32_t readerIndex;
        uint32_t recordBatchIndex;
        uint32_t fragmentStartIndex;
    };

    class ExternalMerge {
    public:
        static arrow::Status mergeFilesFromSortedBatches(const std::filesystem::path &folder, const std::string &columnName,
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

            std::cout << "[External Merge] Start external merge" << std::endl;

            constexpr int readFinished = -1;

            // Prepare vector of file readers for each sorted batch
            // pair<arrow file reader, record batch index, fragment start index>
            std::vector<std::shared_ptr<ExternalFileReader>> readers;
            parquet::arrow::FileReaderBuilder reader_builder;

            // Compute sum of rows of readers, so that later we can compare it with the total read rows
            uint32_t totalNumRows = 0;
            // List files in folder. The sorted parts are marked with an initial "s" in the filename
            const std::regex regexFiles{R"(.*s\d+\.parquet)"};
            for (const auto &folderFile : std::filesystem::directory_iterator(folder)) {
                const std::filesystem::path& filePath = folderFile.path();
                if (std::regex_match(filePath.string(), regexFiles)){
                    // Define properties for the file readers
                    auto reader_properties = parquet::ReaderProperties(arrow::default_memory_pool());
                    reader_properties.set_buffer_size(common::Settings::bufferSize);
                    reader_properties.enable_buffered_stream();
                    auto arrow_reader_props = parquet::ArrowReaderProperties();
                    arrow_reader_props.set_batch_size(batchSize);  // default 64 * 1024
                    ARROW_RETURN_NOT_OK(reader_builder.OpenFile(filePath, /*memory_map=*/false, reader_properties));
                    reader_builder.memory_pool(arrow::default_memory_pool());
                    reader_builder.properties(arrow_reader_props);
                    std::shared_ptr<parquet::arrow::FileReader> reader;
                    ARROW_ASSIGN_OR_RAISE(reader, reader_builder.Build());
                    std::shared_ptr<arrow::RecordBatchReader> recordBatchReader;
                    std::shared_ptr<arrow::RecordBatchReader> firstRecordBatchReader;
                    ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(&recordBatchReader));
                    ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(&firstRecordBatchReader));
                    std::shared_ptr<arrow::RecordBatch> currentRecordBatch;
                    std::shared_ptr<arrow::RecordBatch> firstRecordBatch;
                    uint32_t readerNumRows = 0;
                    while (true) {
                        RETURN_NOT_OK(recordBatchReader->ReadNext(&currentRecordBatch));
                        if (currentRecordBatch == nullptr) {
                            break;
                        }
                        readerNumRows += currentRecordBatch->num_rows();
                    }
                    totalNumRows += readerNumRows;
                    RETURN_NOT_OK(firstRecordBatchReader->ReadNext(&firstRecordBatch));
                    readers.emplace_back(std::make_shared<ExternalFileReader>(reader, firstRecordBatchReader,
                                                                              firstRecordBatch, 0, 0, readerNumRows));
                }
            }

            auto numReaders = readers.size();
            std::cout << "[External Merge] Found " << numReaders << " files/readers to merge" << std::endl;
            std::cout << "[External Merge] Expected " << totalNumRows << " rows to merge in total" << std::endl;

            // Prepare a vector of empty columns matching the schema. It will be populated with the merged data
            if (numReaders > 0 ) {

                // Size of the read from a sorted file. Computed this way so that we merged fragments will be exactly
                // one batch, that can be directly written to disk
                uint32_t fragmentSize;
                if(batchSize >= numReaders){
                    fragmentSize = std::ceil(batchSize / numReaders);
                }
                else{
                    fragmentSize = 1;
                }

                assert(fragmentSize > 0);
                std::cout << "[External Merge] Using fragment size " << fragmentSize << std::endl;

                // Obtain the common schema
                auto schema = readers.at(0)->recordBatch->schema();

                // Determine the index of the ordering column
                auto sortingColumnIndex = schema->GetFieldIndex(columnName);
                // Make sure that the specified column exists in the schema
                assert(sortingColumnIndex != -1);

                uint32_t numRowsRead = 0;
                uint32_t completedReaders = 0;
                uint32_t partitionIndex = 0;
                // Termination conditions: the data from all readers is exhausted
                while(numRowsRead < totalNumRows) { // || completedReaders < numReaders) {
                    // Initialize a priority queue, storing pairs of arrow values and related reader index
                    // The index is needed to identity the source batch and reconstruct the row order later
                    auto compare = [](ExternalRow a, ExternalRow b) { return a.columnValue > b.columnValue; };
                    std::priority_queue<ExternalRow, std::vector<ExternalRow>, decltype(compare)> q(compare);
                    // Prepare mergedTable
                    auto mergedTable = arrow::Table::MakeEmpty(schema, arrow::default_memory_pool()).ValueOrDie();
                    // Fragment rows read
                    uint32_t numRowsFragments = 0;
                    for (int i = 0; i < numReaders; ++i) {
                        // If we already reached the desired batch size
                        if (numRowsFragments >= batchSize){
                            break;
                        }
                        std::shared_ptr<ExternalFileReader> currentReader = readers.at(i);
                        if (currentReader->fragmentStartIndex != readFinished){
                            std::shared_ptr<arrow::RecordBatchReader> batchReader;
                            ARROW_RETURN_NOT_OK(currentReader->fileReader->GetRecordBatchReader(&batchReader));
                            uint32_t recordBatchIndex = currentReader->recordBatchIndex;
                            uint32_t fragmentStart = currentReader->fragmentStartIndex;
                            // Load fragment-sized values from the reader
                            // Use slice to select fragments of each batch
                            assert(fragmentStart >= 0);
                            auto currentRecordBatch = currentReader->recordBatch;
                            if (currentRecordBatch == nullptr){
                                continue;
                            }
                            auto batchFragment = currentRecordBatch->Slice(fragmentStart, fragmentSize);
                            auto fragmentNumRows = batchFragment->num_rows();
                            // If there is no more data to fetch from the record batch
                            if (fragmentNumRows == 0){
                                // If this was the last record batch, set the finished flag
                                if (fragmentStart >= currentReader->numRows){
                                    readers.at(i)->fragmentStartIndex = readFinished;
                                    completedReaders += 1;
                                }
                                // Otherwise, update the index to the next record batch. Consequently, the
                                // fragmentIndex is reset to zero
                                else{
                                    RETURN_NOT_OK(readers.at(i)->recordBatchReader->ReadNext(&currentRecordBatch));
                                    readers.at(i)->recordBatch = currentRecordBatch;
                                    readers.at(i)->recordBatchIndex += 1;
                                    readers.at(i)->fragmentStartIndex = 0;
                                }
                                continue;
                            }
                            // Otherwise, advance the reading index
                            else{
                                readers.at(i)->fragmentStartIndex += fragmentNumRows;
                                numRowsRead += fragmentNumRows;
                                numRowsFragments += fragmentNumRows;
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
                                // Push the value, the index of the reader, the index of the record batch and the
                                // fragment start index to the heap
                                ExternalRow heapRow = ExternalRow(columnValue, i, recordBatchIndex, fragmentStart + j);
                                q.push(heapRow);
                            }
                        }
                    }

                    // Prepare record batch readers

                    std::vector<std::shared_ptr<arrow::RecordBatch>> rowBatches;
                    // Popping from the min heap guarantees that the values are ordered
                    while(!q.empty()){
                        // Pop from the min heap, this will return the minimum
                        auto minRow = q.top();
                        q.pop();
                        // Extract information about the reader/batch source
                        auto columnValue = minRow.columnValue;
                        auto readerIndex = minRow.readerIndex;
                        auto recordBatchIndex = minRow.recordBatchIndex;
                        auto fragmentIndex = minRow.fragmentStartIndex;

                        // Extract matching rows using the fragment index
                        auto recordBatch = getRecordBatch(readers.at(readerIndex), recordBatchIndex).ValueOrDie();
                        auto batchFragment = recordBatch->Slice(fragmentIndex, 1);

                        rowBatches.emplace_back(batchFragment);

                        // Early export of table in case we already reached desired batch size
                        if (rowBatches.size() >= batchSize){
                            // Append the row to the mergedTable
                            mergedTable = arrow::Table::FromRecordBatches(schema, {rowBatches}).ValueOrDie();
                            std::cout << "[External Merge] Reached batch size" << std::endl;
                            auto partitionFilePath = folder / (std::to_string(partitionIndex) + common::Settings::fileExtension);
                            if (exportTableToDisk(mergedTable, partitionFilePath) == arrow::Status::OK()){
                                partitionIndex += 1;
                                std::cout << "[External Merge] Exported " << numRowsRead << " out of " << totalNumRows << std::endl;
                                rowBatches = {};
                            }
                        }
                    }
                    // Export the batch to disk
                    mergedTable = arrow::Table::FromRecordBatches(schema, {rowBatches}).ValueOrDie();
                    auto partitionFilePath = folder / (std::to_string(partitionIndex) + common::Settings::fileExtension);
                    if (exportTableToDisk(mergedTable, partitionFilePath) == arrow::Status::OK()){
                        partitionIndex += 1;
                        std::cout << "[External Merge] Exported " << numRowsRead << " out of " << totalNumRows << std::endl;
                    }
                    std::ignore = rowBatches.empty();
                    mergedTable.reset();
                }
                std::cout << "[External Merge] Completed, scanned " << numRowsRead << " in total" << std::endl;
                assert(numRowsRead == totalNumRows);
                // Remove sorted parts (files starting with 's')
                for (const auto &folderFile : std::filesystem::directory_iterator(folder)) {
                    const std::filesystem::path& filePath = folderFile.path();
                    if (std::regex_match(filePath.string(), regexFiles)) {
                        std::filesystem::remove(filePath);
                    }
                }
                std::cout << "[External Merge] Removed intermediate, sorted files" << std::endl;
            }
            return arrow::Status::OK();
        }

        static arrow::Status sortMergeFiles(const std::filesystem::path &folder, const std::string &columnName,
                                            const int32_t partitionSize){
            if (partitionSize <= 0){
                std::cout << "Cannot sort merge, invalid partition size" << std::endl;
                return arrow::Status::Invalid("Invalid partition size");
            }

            // Initialize DuckDB
            duckdb::DBConfig config;
            config.SetOption("memory_limit", partitioning::MultiDimensionalPartitioning::memoryLimit);
            config.SetOption("temp_directory", partitioning::MultiDimensionalPartitioning::tempDirectory);
            duckdb::DuckDB db(":memory:", &config);
            duckdb::Connection con(db);

            // Load parquet files from folder into memory and sort it
            std::string loadQuery = "CREATE TABLE tbl AS SELECT * FROM read_parquet('" + folder.string() + "/*.parquet') ORDER BY " + columnName;
            auto loadQueryResult = con.Query(loadQuery);
            std::cout << "Loaded table into memory for folder " << folder.string() << std::endl;

            // Get total size
            std::string sizeQuery = "SELECT COUNT(*) FROM tbl";
            auto sizeQueryResult = con.Query(sizeQuery);
            size_t totalSize;
            for (const auto &row : *sizeQueryResult) {
                totalSize = row.GetValue<int>(0);
            }

            // Remove parts
            for (const auto & folderIter : std::filesystem::recursive_directory_iterator(folder))
            {
                if (folderIter.path().extension() == ".parquet")
                {
                    std::filesystem::remove(folderIter.path());
                }
            }

            size_t index = 0;
            size_t offset = 0;
            while (totalSize > 0){
                // Write table to disk, partition-sized blocks
                std::string exportedFile = folder.string() + "/" + std::to_string(index) + ".parquet";
                std::string exportQuery = "COPY (SELECT * FROM tbl "
                                          "      LIMIT " + std::to_string(partitionSize) +
                                          "      OFFSET " + std::to_string(offset) + " )"
                                          "TO '" + exportedFile + "' (FORMAT PARQUET)";
                auto exportQueryResult = con.Query(exportQuery);
                std::cout << "Written to disk " << exportedFile << std::endl;
                offset += partitionSize;
                if (totalSize < partitionSize){
                     break;
                }
                totalSize -= partitionSize;
                index++;
            }

            return arrow::Status::OK();
        }

    private:
        static arrow::Status exportTableToDisk(const std::shared_ptr<arrow::Table> &table,
                                               const std::filesystem::path &outputPath){
            // Export the batch to disk
            if (table->num_rows() > 0) {
                std::shared_ptr<arrow::io::FileOutputStream> outFile;
                ARROW_ASSIGN_OR_RAISE(outFile, arrow::io::FileOutputStream::Open(outputPath));
                // Prepare the Parquet writer
                std::unique_ptr<parquet::arrow::FileWriter> writer;
                ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*table->schema(),
                                                                               arrow::default_memory_pool(),
                                                                               outFile,
                                                                               storage::DataWriter::getWriterProperties(),
                                                                               storage::DataWriter::getArrowWriterProperties()));
                // Write the batch and close the file
                ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
                ARROW_RETURN_NOT_OK(writer->Close());
                std::cout << "[External Merge] Exported " << table->num_rows() << " rows to file " << outputPath << std::endl;
                return arrow::Status::OK();
            }
            return arrow::Status::IOError("Table is empty");
        }

        static arrow::Result<std::shared_ptr<arrow::RecordBatch>> getRecordBatch(std::shared_ptr<ExternalFileReader> &reader, size_t index) {
            std::shared_ptr<arrow::RecordBatch> currentRecordBatch;
            // The reader already holds the record batch looked for: return it directly
            if (index == reader->recordBatchIndex) {
                return reader->recordBatch;
            }
            // The searched record batch has an index greater than the current: continue search from current record batch
            else if (index > reader->recordBatchIndex) {
                currentRecordBatch = reader->recordBatch;
            }
            // If the index is lower than the current, we need to restart from the first record batch
            // Keep the currentRecordBatch variable uninitialized to do so.
            else{
                std::shared_ptr<arrow::RecordBatchReader> firstRecordBatchReader;
                ARROW_RETURN_NOT_OK(reader->fileReader->GetRecordBatchReader(&firstRecordBatchReader));
                reader->recordBatchReader = firstRecordBatchReader;
            };
            // Get the record batch reader
            // Iterate over the record batches
            while (true) {
                RETURN_NOT_OK(reader->recordBatchReader->ReadNext(&currentRecordBatch));
                // Stop when the index was found, or we reached the end
                if (reader->recordBatchIndex == index || currentRecordBatch == nullptr) {
                    break;
                }
                reader->recordBatchIndex += 1;
            }
            return currentRecordBatch;
        }

    };
}

#endif //EXTERNAL_MERGE_H
