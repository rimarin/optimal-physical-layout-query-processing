#include <filesystem>
#include <iostream>
#include <set>

#include "include/storage/DataReader.h"
#include "experimentsConfig.cpp"

int main(int argc, char **argv) {
    if (argc < 6){
        std::cout << "Insufficient number of arguments\n" << std::endl;
        std::cout << "Expected syntax: partitioner <dataset_base_path> <dataset_name> <partitioning_technique> <partition_size> <columns>\n" << std::endl;
        exit(1);
    }
    std::filesystem::path argDatasetPath(argv[1]);
    if (!std::filesystem::exists(argDatasetPath)){
        std::cout << "Dataset folder " << argDatasetPath << " does not exist" << std::endl;
        exit(1);
    }
    std::string argDatasetName = argv[2];
    if (ExperimentsConfig::realDatasets.find(argDatasetName) == ExperimentsConfig::realDatasets.end()){
        std::cout << "Dataset not available/recognized" << std::endl;
        exit(1);
    }
    std::string argPartitioningTechnique = argv[3];
    std::set<std::string> partitioningTechniques = {};
    for (const auto &item: ExperimentsConfig::nameToPartitioningTechnique){
        partitioningTechniques.insert(item.first);
    }
    if (ExperimentsConfig::nameToPartitioningTechnique.find(argPartitioningTechnique) ==
    ExperimentsConfig::nameToPartitioningTechnique.end()){
        std::cout << "Partitioning technique not available/recognized" << std::endl;
        exit(1);
    }
    std::string argPartitionSize = argv[4];
    auto partitionSize = std::stoi(argPartitionSize);
    std::string argColumns = argv[5];
    std::vector<std::string> partitioningColumns;
    std::string segment;
    std::stringstream ss(argColumns);
    while(std::getline(ss, segment, ','))
    {
        partitioningColumns.push_back(segment);
    }

    std::filesystem::path datasetFilePath = argDatasetPath / ExperimentsConfig::noPartition / (argDatasetName + ".parquet");
    if (!std::filesystem::exists(datasetFilePath)){
        std::cout << "Not partitioned source dataset not found in " << datasetFilePath << std::endl;
        exit(1);
    }
    std::filesystem::path outputPath = argDatasetPath / argPartitioningTechnique;
    storage::DataWriter::cleanUpFolder(outputPath);
    auto dataReader = storage::DataReader();
    std::ignore = dataReader.load(datasetFilePath);
    auto mapNameToTechnique = ExperimentsConfig::nameToPartitioningTechnique;
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> partitioningTechnique = mapNameToTechnique[argPartitioningTechnique];
    arrow::Status status = partitioningTechnique->partition(dataReader, partitioningColumns, partitionSize, outputPath);
    return 0;
}
