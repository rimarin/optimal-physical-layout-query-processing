#include <arrow/io/api.h>
#include <filesystem>

#include "fixture.cpp"
#include "gtest/gtest.h"
#include "partitioning/PartitioningFactory.h"
#include "storage/TableGenerator.h"

TEST_F(TestOptimalLayoutFixture, TestExternalMergeSort){
    auto folder = ExperimentsConfig::testsFolder / "external-merge-sort";
    auto fileExtension = ExperimentsConfig::fileExtension;
    auto dataset = getDatasetPath(ExperimentsConfig::datasetCities);
    cleanUpFolder(folder);
    auto dataReader = std::make_shared<storage::DataReader>();
    std::vector<std::string> partitioningColumns = {"x", "y", "year"};
    ASSERT_EQ(dataReader->load(dataset), arrow::Status::OK());
    auto batch_reader = dataReader->getTableBatchReader().ValueOrDie();
    while (true) {
        std::shared_ptr<arrow::RecordBatch> record_batch;
        ASSERT_EQ(batch_reader->ReadNext(&record_batch), arrow::Status::OK());
        if (record_batch == nullptr) {
            break;
        }
        ASSERT_EQ(external::ExternalSort::writeSorted(record_batch->Slice(2, 2), "x", folder / ("s0" + fileExtension)),
                  arrow::Status::OK());
        ASSERT_EQ(external::ExternalSort::writeSorted(record_batch->Slice(4, 2), "x", folder / ("s1" + fileExtension)),
                  arrow::Status::OK());
        ASSERT_EQ(external::ExternalSort::writeSorted(record_batch->Slice(0, 2), "x", folder / ("s2" + fileExtension)),
                  arrow::Status::OK());
        ASSERT_EQ(external::ExternalSort::writeSorted(record_batch->Slice(6, 2), "x", folder / ("s3" + fileExtension)),
                  arrow::Status::OK());
    }
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("s0" + fileExtension), "x", std::vector<int32_t>({5, 35})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("s0" + fileExtension), "y", std::vector<int32_t>({45, 42})), arrow::Status::OK());

    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("s1" + fileExtension), "x", std::vector<int32_t>({27, 52})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("s1" + fileExtension), "y", std::vector<int32_t>({35, 10})), arrow::Status::OK());

    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("s2" + fileExtension), "x", std::vector<int32_t>({62, 82})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("s2" + fileExtension), "y", std::vector<int32_t>({77, 65})), arrow::Status::OK());

    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("s3" + fileExtension), "x", std::vector<int32_t>({85, 90})), arrow::Status::OK());
    ASSERT_EQ(checkPartition<arrow::Int32Array>(folder / ("s3" + fileExtension), "y", std::vector<int32_t>({15, 5})), arrow::Status::OK());

    ASSERT_EQ(external::ExternalMerge::mergeFiles(folder, "hilbert_curve"), arrow::Status::OK());
}
