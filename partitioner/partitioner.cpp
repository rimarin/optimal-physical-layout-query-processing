#include <filesystem>
#include <iostream>
#include <set>

#include "include/storage/DataReader.h"
#include "experimentsConfig.cpp"

int main(int argc, char **argv) {
    if (argc < 5){
        std::cout << "Insufficient number of arguments\n";
        std::cout << "Expected syntax: partitioner <dataset_name> <partitioning_technique> <partition_size> <columns>\n";
        exit(1);
    }
    std::string argDatasetName = argv[1];
    if (ExperimentsConfig::realDatasets.find(argv[1]) == ExperimentsConfig::realDatasets.end()){
        std::cout << "Dataset not available/recognized";
        exit(1);
    }
    std::string argPartitioningTechnique = argv[2];
    std::set<std::string> partitioningTechniques = {};
    for (const auto &item: ExperimentsConfig::nameToPartitioningTechnique){
        partitioningTechniques.insert(item.first);
    }
    if (ExperimentsConfig::nameToPartitioningTechnique.find(argPartitioningTechnique) ==
    ExperimentsConfig::nameToPartitioningTechnique.end()){
        std::cout << "Partitioning technique not available/recognized";
        exit(1);
    }
    std::string argPartitionSize = argv[3];
    auto partitionSize = std::stoi(argPartitionSize);
    std::string argColumns = argv[4];
    std::vector<std::string> partitioningColumns;
    std::string segment;
    std::stringstream ss(argColumns);
    while(std::getline(ss, segment, ','))
    {
        partitioningColumns.push_back(segment);
    }

    std::filesystem::path projectPath = std::filesystem::current_path().parent_path().parent_path();
    std::filesystem::path datasetPath = projectPath / "benchmark" / "datasets" / argDatasetName;

    std::filesystem::path datasetFilePath = datasetPath / ExperimentsConfig::noPartition / (argDatasetName + ".parquet");
    if (!std::filesystem::exists(datasetPath)){
        std::cout << "Dataset not found in " << datasetPath;
        exit(1);
    }
    std::filesystem::path outputPath = datasetPath / argPartitioningTechnique;
    storage::DataWriter::cleanUpFolder(outputPath);
    auto dataReader = storage::DataReader();
    std::ignore = dataReader.load(datasetFilePath);
    auto mapNameToTechnique = ExperimentsConfig::nameToPartitioningTechnique;
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> partitioningTechnique = mapNameToTechnique[argPartitioningTechnique];
    arrow::Status status = partitioningTechnique->partition(dataReader, partitioningColumns, partitionSize, outputPath);
    return 0;
}
