#include <arrow/table.h>
#include <arrow/result.h>
#include <filesystem>
#include <string>

#include "gtest/gtest.h"

#include "experimentsConfig.cpp"


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

    static arrow::Result<std::shared_ptr<arrow::Table>> getDataset(const std::string &datasetName){
        std::filesystem::path datasetFile = getDatasetPath(datasetName);
        return storage::DataReader::getTable(datasetFile);
    }

    static std::filesystem::path getDatasetPath(const std::string &datasetName){
        if(ExperimentsConfig::testDatasets.count(datasetName)) {
            return ExperimentsConfig::noPartitionFolder / (datasetName + "0" + ExperimentsConfig::fileExtension);
        }
        else if(ExperimentsConfig::realDatasets.count(datasetName)){
            return ExperimentsConfig::datasetsFolder / datasetName / ExperimentsConfig::noPartition / (datasetName + ExperimentsConfig::fileExtension);
        }
        throw std::invalid_argument("Dataset not found");
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_number<T, arrow::Result<std::vector<typename T::c_type>>> readColumn(std::filesystem::path &filename,
                                                                                          const std::string &columnName){
        std::shared_ptr<arrow::Table> partition = storage::DataReader::getTable(filename).ValueOrDie();
        auto columnChunk = std::static_pointer_cast<arrow::NumericArray<T>>(partition->GetColumnByName(columnName)->chunk(0));
        std::vector<typename T::c_type> columnVector;
        for (int64_t i = 0; i < columnChunk->length(); ++i)
        {
            columnVector.push_back(columnChunk->Value(i));
        }
        return columnVector;
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_string<T, arrow::Result<std::vector<std::string>>> readColumn(std::filesystem::path &filename,
                                                                                   const std::string &columnName){
        std::shared_ptr<arrow::Table> partition = storage::DataReader::getTable(filename).ValueOrDie();
        std::vector<arrow::Datum> columnData;
        auto columnChunk = std::static_pointer_cast<arrow::StringArray>(partition->GetColumnByName(columnName)->chunk(0));
        std::vector<std::string> columnVector;
        for (int64_t i = 0; i < columnChunk->length(); ++i)
        {
            columnVector.emplace_back(columnChunk->Value(i));
        }
        return columnVector;
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_number<T, arrow::Status> checkPartition(std::filesystem::path partitionPath, std::string columnName,
                        std::vector<typename T::c_type> columnVector){
        if (readColumn<ArrayType>(partitionPath, columnName) == columnVector){
            return arrow::Status::OK();
        }
        return arrow::Status::Invalid("Read column does not match data");
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_string<T, arrow::Status> checkPartition(std::filesystem::path partitionPath, std::string columnName,
                                 std::vector<std::string> columnVector){
        if (readColumn<ArrayType>(partitionPath, columnName) == columnVector){
            return arrow::Status::OK();
        }
        return arrow::Status::Invalid("Read column does not match data");
    }

    static bool addColumnPartitionId(const std::string &datasetName){
        // Test datasets should have an additional column with the partition id
        if(ExperimentsConfig::testDatasets.count(datasetName)) {
            return true;
        }
        return false;
    };


};
