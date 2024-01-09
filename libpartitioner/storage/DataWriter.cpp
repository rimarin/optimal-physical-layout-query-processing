#include <iostream>
#include <filesystem>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include "storage/DataWriter.h"

namespace storage {

    // Write to disk a table, given its pointer and the output path
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

    // Define common writer properties within the project
    std::shared_ptr<parquet::WriterProperties> DataWriter::getWriterProperties(){
        std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder()
                // Optimal row group length is around 120000 according to several sources
                .max_row_group_length(100000)
                ->created_by("Optimal Layout Partitioner")
                ->version(parquet::ParquetVersion::PARQUET_2_6)
                ->data_page_version(parquet::ParquetDataPageVersion::V2)
                // Worse compression ratio than zstd but faster reads
                ->compression(arrow::Compression::SNAPPY)
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
        for (const uint32_t &partitionId: partitionIds){
            std::string root_path;
            std::filesystem::path subPartitionsFolder = basePath / std::to_string(partitionId);
            // Merge batches for a batch folder
            if (std::filesystem::exists(subPartitionsFolder)) {
                ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUriOrPath(subPartitionsFolder, &root_path));
                std::cout << "[DataWriter] Start merging batches for partition id " << partitionId << std::endl;
                ARROW_RETURN_NOT_OK(mergeBatchesForPartition(partitionId, fs, root_path));
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

    arrow::Status DataWriter::mergeBatchesForPartition(const uint32_t &partitionId,
                                                       const std::shared_ptr<arrow::fs::FileSystem> &filesystem,
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
            std::cout << "[FixedGridPartitioning] Found partition fragment: " << (*fragment)->ToString() << std::endl;
        }
        ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());
        ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
        auto mergedTable = scanner->ToTable();
        std::filesystem::path mergedFragmentsFilePath = base_dir + ".parquet";
        std::cout << "[FixedGridPartitioning] Exporting merged batches from folder " << base_dir << std::endl;
        std::cout << "[FixedGridPartitioning] Merged table has " << mergedTable.ValueOrDie()->num_rows() << " rows" << std::endl;
        ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTable(*mergedTable, mergedFragmentsFilePath));
        mergedTable->reset();
        return arrow::Status::OK();
    }

    // Restore a clean state by removing all Parquet files from a specified folder
    void DataWriter::cleanUpFolder(const std::filesystem::path &folder){
        if (std::filesystem::is_directory(folder)){
            for (const auto &folderIter : std::filesystem::directory_iterator(folder))
            {
                if (folderIter.path().extension() == ".parquet")
                {
                    std::filesystem::remove(folderIter.path());
                }
            }
        }
    }

} // storage