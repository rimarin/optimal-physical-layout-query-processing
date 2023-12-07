#include <filesystem>
#include <iostream>
#include <set>

#include "../libpartitioner/include/storage/DataWriter.h"
#include "../libpartitioner/include/partitioning/Partitioning.h"
#include "../libpartitioner/include/storage/DataReader.h"
#include "../test/fixture.cpp"

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

    std::filesystem::path datasetPath = std::filesystem::current_path().parent_path() / "benchmark" / "datasets" / argDatasetName /
            ExperimentsConfig::noPartition / (argDatasetName + ".parquet");
    std::filesystem::path outputPath = std::filesystem::current_path().parent_path() / "benchmark" / "datasets" / argDatasetName / argPartitioningTechnique;
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataReader::readTable(datasetPath);
    auto mapNameToTechnique = ExperimentsConfig::nameToPartitioningTechnique;
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> partitioningTechnique = mapNameToTechnique[argPartitioningTechnique];
    arrow::Status status = partitioningTechnique->partition(*table, partitioningColumns, partitionSize, outputPath);
    return 0;
}
