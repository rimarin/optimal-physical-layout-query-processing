#include <filesystem>
#include <arrow/table.h>
#include <arrow/result.h>
#include "gtest/gtest.h"


class TestOptimalLayoutFixture: public ::testing::Test {
public:
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

    void cleanUpFolder(std::string folder){
        auto path =  std::filesystem::current_path() / folder;
        for (const auto & folderIter : std::filesystem::directory_iterator(path))
        {
            if (folderIter.path().extension() == ".parquet")
            {
                std::filesystem::remove(folderIter.path());
            }
        }
    }

    arrow::Result<std::shared_ptr<arrow::Table>> getDataset(std::string datasetName){
        return arrow::Status::OK();
    }

    // Dataset names
    std::string datasetWeatherName = "weather";
    std::string datasetSchoolName = "school";

    // Folder names
    std::string noPartitionFolder = "NoPartition";
    std::string fixedGridFolder = "FixedGrid";
    std::string gridFileFolder = "GridFile";
    std::string kdTreeFolder = "KDTree";
    std::string strTreeFolder = "STRTree";
    std::string quadTreeFolder = "QuadTree";
};
