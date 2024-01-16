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
        std::filesystem::path sortedBatchPath = folder / ("s" + std::to_string(0) + fileExtension);
        ASSERT_EQ(external::ExternalSort::writeSorted(record_batch->Slice(2, 2), "x", sortedBatchPath), arrow::Status::OK());
        std::filesystem::path sortedBatchPath2 = folder / ("s" + std::to_string(1) + fileExtension);
        ASSERT_EQ(external::ExternalSort::writeSorted(record_batch->Slice(4, 2), "x", sortedBatchPath2), arrow::Status::OK());
        std::filesystem::path sortedBatchPath3 = folder / ("s" + std::to_string(2) + fileExtension);
        ASSERT_EQ(external::ExternalSort::writeSorted(record_batch->Slice(0, 2), "x", sortedBatchPath3), arrow::Status::OK());
        std::filesystem::path sortedBatchPath4 = folder / ("s" + std::to_string(3) + fileExtension);
        ASSERT_EQ(external::ExternalSort::writeSorted(record_batch->Slice(6, 2), "x", sortedBatchPath4), arrow::Status::OK());
    }
    auto pathPartition1 = folder / ("s0" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "x"), std::vector<int32_t>({5, 35}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition1, "y"), std::vector<int32_t>({45, 42}));
    auto pathPartition2 = folder / ("s1" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "x"), std::vector<int32_t>({27, 52}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition2, "y"), std::vector<int32_t>({35, 10}));
    auto pathPartition3 = folder / ("s2" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "x"), std::vector<int32_t>({62, 82}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition3, "y"), std::vector<int32_t>({77, 65}));
    auto pathPartition4 = folder / ("s3" + fileExtension);
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition4, "x"), std::vector<int32_t>({85, 90}));
    ASSERT_EQ(readColumn<arrow::Int32Array>(pathPartition4, "y"), std::vector<int32_t>({15, 5}));
    ASSERT_EQ(external::ExternalMerge::mergeFiles(folder, "hilbert_curve"), arrow::Status::OK());
}
