#include <filesystem>
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
            std::cout << folderIter.path() << std::endl;
            if (folderIter.path().extension() == ".parquet")
            {
                std::filesystem::remove(folderIter.path());
            }
        }
    }

    // shared user data
    std::string example1TableName = "weather";
    std::string example2TableName = "school";

    std::string noPartitionFolder = "NoPartition";
    std::string fixedGridFolder = "FixedGrid";
    std::string kdTreeFolder = "KDTree";
    std::string quadTreeFolder = "QuadTree";
};
