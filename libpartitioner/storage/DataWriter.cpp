#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <filesystem>
#include <iostream>
#include <parquet/arrow/writer.h>

#include "storage/DataWriter.h"

namespace storage {

    // Write to disk a table, given its pointer and the output path
    arrow::Status DataWriter::WriteTableToDisk(std::shared_ptr<arrow::Table>& table,
                                               std::filesystem::path &outputPath) {
        std::cout << "[DataWriter] Starting to write table to file " << outputPath << std::endl;
        // Prepare the output file
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(outputPath.string()));
        // Prepare the Parquet writer
        std::unique_ptr<parquet::arrow::FileWriter> writer;
        ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*table->schema(),
                                                                       arrow::default_memory_pool(),
                                                                       outfile,
                                                                       storage::DataWriter::getWriterProperties(),
                                                                       storage::DataWriter::getArrowWriterProperties()));
        // Write the batch and close the file
        std::cout << "[DataWriter] Writing " << table->num_rows() << " rows" << std::endl;
        ARROW_RETURN_NOT_OK(writer->WriteTable(*table));
        ARROW_RETURN_NOT_OK(writer->Close());
        std::cout << "[DataWriter] Completed, written table to file " << outputPath << std::endl;
        return arrow::Status::OK();
    }

    // Define common writer properties within the project
    std::shared_ptr<parquet::WriterProperties> DataWriter::getWriterProperties(){
        std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder()
                // Optimal row group length is around 120000 according to several sources
                .max_row_group_length(common::Settings::rowGroupSize)
                ->created_by(common::Settings::libraryName)
                ->version(common::Settings::version)
                ->data_page_version(parquet::ParquetDataPageVersion::V2)
                // Worse compression ratio than zstd but faster reads
                ->compression(common::Settings::compression)
                ->build();
        return props;
    }

    // Define common arrow writer properties
    std::shared_ptr<parquet::ArrowWriterProperties> DataWriter::getArrowWriterProperties(){
        return parquet::ArrowWriterProperties::Builder().store_schema()->build();
    }

    // Given a path with several sub folders with parts of batches, merge the parts together into partitions
    // Then remove
    arrow::Status DataWriter::mergeBatches(const std::filesystem::path &basePath, const std::set<uint32_t> &partitionIds){
        std::cout << "[DataWriter] Start merging batches" << std::endl;
        // We expect to have a folder for each partition id. Inside the folder, the parquet files from each batch
        // are to be found. For example, assuming partition 1 from 3 batches, the content will be:
        //  --/1/
        //    -- b0.parquet
        //    -- b1.parquet
        //    -- b2.parquet
        auto numPartitions = partitionIds.size();
        for (const uint32_t &partitionId: partitionIds){
            std::string rootPath;
            std::filesystem::path subPartitionsFolder = basePath / std::to_string(partitionId);
            // Merge batches for a batch folder
            if (std::filesystem::exists(subPartitionsFolder)) {
                ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(subPartitionsFolder, &rootPath));
                std::cout << "[DataWriter] Start merging batches for partition id " << partitionId << std::endl;
                auto statusPartitionMerge = mergeBatchesInFolder(fs, rootPath);
                if (statusPartitionMerge == arrow::Status::OK()){
                    std::cout << "[DataWriter] Partition " << partitionId << " (out of " << numPartitions <<
                                 " ) merged :" << statusPartitionMerge.ToString() << std::endl;
                } else{
                    std::cout << "[DataWriter] Failed partition merge :" << statusPartitionMerge.ToString() << std::endl;
                }
            }
            else{
                std::cout << "[DataWriter] Batch folder empty for partition id " << partitionId << std::endl;
            }
        }
        std::cout << "[DataWriter] Partitioned batches have been merged into partitions" << std::endl;
        // Delete all the folders and their content once the merged tables are ready
        for (const auto &partitionId: partitionIds){
            std::filesystem::path subPartitionsFolder = basePath / std::to_string(partitionId);
            auto numDeleted = std::filesystem::remove_all(subPartitionsFolder);
            std::cout   << "[DataWriter] Cleaned up folder " << subPartitionsFolder << " with " << numDeleted << " partition fragments" << std::endl;
        }
        return arrow::Status::OK();
    }

    arrow::Status DataWriter::mergeBatchesInFolder(const std::shared_ptr<arrow::fs::FileSystem> &filesystem,
                                                   const std::string &base_dir) {
        arrow::fs::FileSelector selector;
        selector.base_dir = base_dir;
        ARROW_ASSIGN_OR_RAISE(auto factory,
                              arrow::dataset::FileSystemDatasetFactory::Make(
                                      filesystem,
                                      selector,
                                      std::make_shared<arrow::dataset::ParquetFileFormat>(),
                                      arrow::dataset::FileSystemFactoryOptions()
                              )
        );
        ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish());
        ARROW_ASSIGN_OR_RAISE(auto fragments, dataset->GetFragments())
        for (const auto& fragment : fragments) {
            std::cout << "[DataWriter] Found partition fragment: " << (*fragment)->ToString() << std::endl;
        }
        ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());
        ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
        auto mergedTable = scanner->ToTable();
        if (mergedTable.status() == arrow::Status::OK()){
            std::filesystem::path mergedFragmentsFilePath = base_dir + common::Settings::fileExtension;
            std::cout << "[DataWriter] Exporting merged batches from folder " << base_dir << std::endl;
            std::cout << "[DataWriter] Merged table has " << mergedTable.ValueOrDie()->num_rows() << " rows" << std::endl;
            ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTableToDisk(*mergedTable, mergedFragmentsFilePath));
            mergedTable->reset();
        } else{
            std::cout << "[DataWriter] Error while scanning merged table " << mergedTable.status().ToString() << std::endl;
        }
        return arrow::Status::OK();
    }

    // Restore a clean state by removing all Parquet files from a specified folder
    void DataWriter::cleanUpFolder(const std::filesystem::path &folder){
        if (std::filesystem::is_directory(folder)){
            for (const auto &folderIter : std::filesystem::recursive_directory_iterator(folder))
            {
                if (folderIter.path().extension() == common::Settings::fileExtension)
                {
                    std::filesystem::remove(folderIter.path());
                }
            }
        }
    }

} // storage