#include <arrow/io/api.h>
#include <filesystem>

#include "common/Exception.h"
#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningFailures){
    auto folder = ExperimentsConfig::fixedGridFolder;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetCities);
    cleanUpFolder(folder);
    auto dataReader = std::make_shared<storage::DataReader>();
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    EXPECT_THROW({
         try
         {
             auto partitioning = partitioning::PartitioningFactory::create(partitioning::FIXED_GRID, dataReader,
                                                                           {"x"}, 20, folder);
             partitioning->partition().ToString();
         }
         catch( const InsufficientNumberOfColumns& e )
         {
             throw;
         }
     }, InsufficientNumberOfColumns );
    EXPECT_THROW({
         try
         {
             auto partitioning = partitioning::PartitioningFactory::create(partitioning::STR_TREE, dataReader,
                                                                           {"x", "y"}, 0, folder);
             partitioning->partition().ToString();
         }
         catch( const InvalidPartitionSize& e )
         {
             throw;
         }
     }, InvalidPartitionSize );
    EXPECT_THROW({
         try
         {
             auto partitioning = partitioning::PartitioningFactory::create(partitioning::HILBERT_CURVE, dataReader,
                                                                           {"x", "y", "42"}, 8, folder);
             partitioning->partition().ToString();
         }
         catch( const InvalidColumn& e )
         {
             throw;
         }
     }, InvalidColumn );
}

