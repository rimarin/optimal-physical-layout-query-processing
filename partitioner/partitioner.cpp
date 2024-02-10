#include <filesystem>
#include <iostream>
#include <set>

#include "experimentsConfig.cpp"
#include "include/storage/DataReader.h"
#include "partitioning/PartitioningFactory.h"

int main(int argc, char **argv) {

    // Check the overall number of arguments
    if (argc < 6){
        std::cout << "Insufficient number of arguments\n" << std::endl;
        std::cout << "Expected syntax: partitioner <dataset_base_path> <dataset_name> <partitioning_scheme>"
                     " <partition_size> <columns>\n" << std::endl;
        exit(1);
    }

    // Validate the dataset path
    std::filesystem::path argDatasetPath(argv[1]);
    if (!std::filesystem::exists(argDatasetPath)){
        std::cout << "Dataset folder " << argDatasetPath << " does not exist" << std::endl;
        exit(1);
    }

    // Validate the dataset name
    std::string argDatasetName = argv[2];
    if (ExperimentsConfig::realDatasets.find(argDatasetName) == ExperimentsConfig::realDatasets.end()){
        std::cout << "Dataset not available/recognized" << std::endl;
        exit(1);
    }

    // Validate the partitioning scheme
    std::string argPartitioningScheme = argv[3];
    partitioning::PartitioningScheme scheme = partitioning::mapNameToScheme.at(argPartitioningScheme);
    if (partitioning::mapNameToScheme.find(argPartitioningScheme) == partitioning::mapNameToScheme.end()){
        std::cout << "Partitioning scheme not available/recognized" << std::endl;
        exit(1);
    }

    // Load the partition size
    std::string argPartitionSize = argv[4];
    auto partitionSize = std::stoi(argPartitionSize);

    // Load the partitioning columns
    std::string argColumns = argv[5];
    std::vector<std::string> partitioningColumns;
    std::string segment;
    std::stringstream ss(argColumns);
    while(std::getline(ss, segment, ','))
    {
        partitioningColumns.push_back(segment);
    }

    // Validate the actual dataset file
    std::filesystem::path datasetFilePath = argDatasetPath / ExperimentsConfig::noPartition / (argDatasetName + ExperimentsConfig::fileExtension);
    if (!std::filesystem::exists(datasetFilePath)){
        std::cout << "Not partitioned source dataset not found in " << datasetFilePath << std::endl;
        exit(1);
    }

    // Remove files from the folder before starting
    std::filesystem::path outputPath = argDatasetPath / argPartitioningScheme;
    storage::DataWriter::cleanUpFolder(outputPath);

    // Load dataset file
    auto dataReader = std::make_shared<storage::DataReader>();
    std::ignore = dataReader->load(datasetFilePath);
    auto datasetNumRows = dataReader->getNumRows();

    // Load partitioning scheme
    auto partitioningScheme = partitioning::PartitioningFactory::create(scheme, dataReader, partitioningColumns, partitionSize, outputPath);

    // Apply the partitioning
    if (!partitioningScheme->isFinished()){
        arrow::Status status = partitioningScheme->partition();
    }

    // Check correctness
    if (ExperimentsConfig::checkCorrectness){
        uint32_t fileCount = 0;
        uint32_t partitionsTotalRows = 0;
        for (auto &fileSystemItem: std::filesystem::directory_iterator(outputPath)) {
            if (fileSystemItem.is_regular_file() &&
                fileSystemItem.path().extension() == common::Settings::fileExtension) {
                fileCount += 1;
                auto partitionPath = fileSystemItem.path();
                std::ignore = dataReader->load(partitionPath);
                partitionsTotalRows += dataReader->getNumRows();
            }
        }
        if (fileCount == 0){
            std::cout << "[Partitioner] Incorrect, no files created";
        }
        if (datasetNumRows != partitionsTotalRows){
            std::cout << "[Partitioner] Incorrect, number of rows do not match";
            std::cout << "[Partitioner] Expected " << datasetNumRows << " rows and found " << partitionsTotalRows;
        }
        else{
            std::cout << "[Partitioner] Good, at the least the number of rows match";
        }
    }

    return 0;
}
