#include <arrow/table.h>
#include <arrow/result.h>
#include <filesystem>
#include <string>

#include "gtest/gtest.h"

#include "../experimentsConfig.cpp"


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

    void cleanUpFolder(std::filesystem::path folder){
        if (std::filesystem::is_directory(folder)){
            for (const auto & folderIter : std::filesystem::directory_iterator(folder))
            {
                if (folderIter.path().extension() == ExperimentsConfig::parquetFileExtension)
                {
                    std::filesystem::remove(folderIter.path());
                }
            }
        }
    }

    arrow::Result<std::shared_ptr<arrow::Table>> getDataset(std::string &datasetName){
        if (ExperimentsConfig::testDatasets.count(datasetName)){
            std::filesystem::path datasetFile = ExperimentsConfig::noPartitionFolder / (datasetName + "0" + ExperimentsConfig::fileExtension);
            return storage::DataReader::readTable(datasetFile);
        } else if(ExperimentsConfig::realDatasets.count(datasetName)){
            std::filesystem::path datasetFile = ExperimentsConfig::datasetsFolder / datasetName / (datasetName + ExperimentsConfig::fileExtension);
            return storage::DataReader::readTable(datasetFile);
        } else {
            return arrow::Status::IOError("Dataset not found");
        }
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::Result<std::vector<typename T::c_type>> readColumn(std::filesystem::path &filename, std::string columnName){
        std::shared_ptr<arrow::Table> partition = storage::DataReader::readTable(filename).ValueOrDie();
        std::vector<arrow::Datum> columnData;
        auto columnChunk = std::static_pointer_cast<arrow::NumericArray<T>>(
                partition->GetColumnByName(columnName)->chunk(0));
        std::vector<typename T::c_type> columnVector;
        for (int64_t i = 0; i < columnChunk->length(); ++i)
        {
            columnVector.push_back(columnChunk->Value(i));
        }
        return columnVector;
    }
};
