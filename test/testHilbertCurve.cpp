#include <iostream>
#include <filesystem>

#include <arrow/io/api.h>

#include "include/partitioning/HilbertCurvePartitioning.h"
#include "include/storage/DataWriter.h"
#include "include/storage/DataReader.h"
#include "fixture.cpp"

#include "gtest/gtest.h"

TEST_F(TestOptimalLayoutFixture, TestPartitioningHilbertCurve){
    cleanUpFolder(hilbertCurveFolder);
    arrow::Result<std::shared_ptr<arrow::Table>> table = storage::DataWriter::GenerateExampleSchoolTable().ValueOrDie();
    std::vector<std::string> partitioningColumns = {"Age", "Student_id"};
    std::shared_ptr<partitioning::MultiDimensionalPartitioning> hilbertCurvePartitioning = std::make_shared<partitioning::HilbertCurvePartitioning>();
    auto partitions = hilbertCurvePartitioning->partition(*table, partitioningColumns, partitionSizeTest).ValueOrDie();
    arrow::Status statusHilbertCurve = storage::DataWriter::WritePartitions(partitions, datasetSchoolName, hilbertCurveFolder);
}