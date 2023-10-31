#include <arrow/table.h>
#include <arrow/result.h>
#include <filesystem>
#include <set>
#include <string>

#include "gtest/gtest.h"

#include "include/storage/DataReader.h"


class TestOptimalLayoutFixture: public ::testing::Test {
public:

    // Dataset names
    std::string datasetSchoolName = "school";
    std::string datasetWeatherName = "weather";
    std::set<std::string> testDatasets = {datasetSchoolName, datasetWeatherName};

    std::string datasetOSMName = "osm";
    std::string datasetTaxiName = "taxi";
    std::string datasetTPCHName = "tpch";
    std::set<std::string> realDatasets = {datasetOSMName, datasetTaxiName, datasetTPCHName};
    std::map<std::string, std::string> datasetToFileName = {
            {datasetOSMName, "..."},
            { datasetTaxiName, "trips_2018-01"},
            { datasetTPCHName, "lineitem_sf1"}
    };

    // Folder names
    std::filesystem::path noPartitionFolder = std::filesystem::current_path() / "NoPartition";
    std::filesystem::path fixedGridFolder = std::filesystem::current_path() / "FixedGrid";
    std::filesystem::path gridFileFolder = std::filesystem::current_path() / "GridFile";
    std::filesystem::path kdTreeFolder = std::filesystem::current_path() / "KDTree";
    std::filesystem::path strTreeFolder = std::filesystem::current_path() / "STRTree";
    std::filesystem::path quadTreeFolder = std::filesystem::current_path() / "QuadTree";

    std::filesystem::path testsFolder = std::filesystem::current_path();
    std::filesystem::path benchmarkFolder = testsFolder.parent_path() / "benchmark";
    std::filesystem::path datasetsFolder = benchmarkFolder / "datasets";

    // Partitioning config
    int partitionSize = 20000;
    std::string parquetFileExtension = ".parquet";
    std::string fileExtension = parquetFileExtension;

    TestOptimalLayoutFixture( ) {
        // initialization;
        // can also be done in SetUp()
    }

    void SetUp( ) {
        // Initialization to run before each test
    }

    void TearDown( ) {
        // code to run after each test;
        // can be used instead of a destructor,
        // but exceptions can be handled in this function only
    }

    ~TestOptimalLayoutFixture( )  {
        // resources cleanup, no exceptions allowed
    }

    void cleanUpFolder(std::filesystem::path folder){
        for (const auto & folderIter : std::filesystem::directory_iterator(folder))
        {
            if (folderIter.path().extension() == parquetFileExtension)
            {
                std::filesystem::remove(folderIter.path());
            }
        }
    }

    arrow::Result<std::shared_ptr<arrow::Table>> getDataset(std::string datasetName){
        if (testDatasets.count(datasetName)){
            std::filesystem::path datasetFile = noPartitionFolder / (datasetName + "0" + fileExtension);
            return storage::DataReader::ReadTable(datasetFile);
        } else if(realDatasets.count(datasetName)){
            std::filesystem::path datasetFile = datasetsFolder / datasetName / (datasetToFileName[datasetName] + fileExtension);
            return storage::DataReader::ReadTable(datasetFile);
        } else {
            return arrow::Status::IOError("Dataset not found");
        }
    }
};
