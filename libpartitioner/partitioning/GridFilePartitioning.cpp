#include <algorithm>

#include "partitioning/GridFilePartitioning.h"
#include "arrow/testing/gtest_util.h"

namespace partitioning {

    arrow::Status GridFilePartitioning::partition(storage::DataReader &dataReader,
                                                  const std::vector<std::string> &partitionColumns,
                                                  const size_t partitionSize,
                                                  const std::filesystem::path &outputFolder){
        /*
         * In the literature there are different implementations of adaptive grid.
         * Here we use a bulk-load implementation of the original paper:
         * "The Grid File: An Adaptable, Symmetric Multikey File Structure",
         * https://www.cs.ucr.edu/~tsotras/cs236/W15/grid-file.pdf
         * A grid directory consists of two parts: first, a dynamic k-dimensional array called the grid array;
         * its elements (pointers to data buckets) are in one-to-one correspondence with the grid blocks of the
         * partition; and second, k one-dimensional arrays called linear scales, each defines a partition of a domain S;
         * As shown in Fig 9 and Fig 15, we'll try to fit elements in each bucket according to their capacity c,
         * that here is equal to the partition size. This is achieved by splitting the linear scales in half
         * until the desired bucket size is reached. This is done in a circular fashion on all partitioning dimensions.
         * Algorithm:
         * 1. Read only partitioning columns
         * 2. Create the linear scales
         * 3. Read the dataset by batches
         * 4. For each batch, assign the right bucket by using the created scales
         *      generate cellsCoordinates from the scales and filter
         *      or (better) read from batches the row index matching the computed row indexes
         *  5. Export the batch into different partition files
         *  6. Merge the batches into partition files
        */
        std::cout << "[GridFilePartitioning] Initializing partitioning technique" << std::endl;
        std::string displayColumns;
        for (const auto &column : partitionColumns) displayColumns + " " += column;
        std::cout << "[GridFilePartitioning] Partition has to be done on columns: " << displayColumns << std::endl;

        cellCapacity = partitionSize;
        columns = partitionColumns;
        folder = outputFolder;
        numColumns = columns.size();

        auto numRows = dataReader.getNumRows();

        if (partitionSize >= numRows) {
            std::cout << "[GridFilePartitioning] Partition size greater than the available rows" << std::endl;
            std::cout << "[GridFilePartitioning] Therefore put all data in one partition" << std::endl;
            std::filesystem::path source = dataReader.getReaderPath();
            std::filesystem::path destination = outputFolder / "0.parquet";
            std::filesystem::copy(source, destination);
            return arrow::Status::OK();
        }

        std::vector<std::shared_ptr<arrow::ChunkedArray>> columnsData = dataReader.getColumns(partitionColumns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto rows = converter.toRows(columnsData);
        columnsData.clear();

        uint32_t coordIdx = 0;
        dataReader.getNumRows();

        linearScales = {};
        for (int i = 0; i < numColumns; ++i) {
            std::pair<double_t, double_t> columnStats = dataReader.getColumnStats(partitionColumns[i]).ValueOrDie();
            std::vector<double_t> columnDomain = {columnStats.first, columnStats.second};
            linearScales.emplace_back(columnDomain);
        }
        computeLinearScales(rows, coordIdx);

        for (const auto &linearScale: linearScales){
            std::string linearScalesStr(linearScale.begin(), linearScale.end());
            std::cout << "[GridFilePartitioning] Generated linear scale " << linearScalesStr << " rows" << std::endl;
        }

        std::vector<std::vector<std::pair<double, double>>> cellsCoordinates;

        for (const auto& dimensionScale : linearScales) {
            std::vector<std::vector<std::pair<double, double>>> tempCoordinates;
            for (int i = 0; i < dimensionScale.size() - 1; ++i) {
                // If cellsCoordinates vector is empty, add each pair as a separate combination
                if (cellsCoordinates.empty()) {
                    tempCoordinates.push_back({std::make_pair(dimensionScale[i], dimensionScale[i+1])});
                } else {
                    // Otherwise, append the current pair to each existing combination
                    for (const auto& cellCoordinates : cellsCoordinates) {
                        std::vector<std::pair<double, double>> newCell = cellCoordinates;
                        newCell.push_back({std::make_pair(dimensionScale[i], dimensionScale[i+1])});
                        tempCoordinates.push_back(newCell);
                    }
                }
            }
            cellsCoordinates = tempCoordinates;
        }

        std::set<uint32_t> uniquePartitionIds;
        for (int i = 0; i < cellsCoordinates.size(); ++i) {
            uniquePartitionIds.emplace(i);
        }
        auto numPartitions = uniquePartitionIds.size();

        auto batch_reader = dataReader.getTableBatchReader().ValueOrDie();
        uint32_t batchId = 0;
        uint32_t totalNumRows = 0;
        while (true) {
            std::shared_ptr<arrow::RecordBatch> recordBatch;
            ABORT_NOT_OK(batch_reader->ReadNext(&recordBatch));
            if (recordBatch == nullptr) {
                break;
            }

            uint32_t completedPartitions = 0;

            for (const auto &partitionId: uniquePartitionIds){
                std::vector<uint32_t> partitionIds(recordBatch->num_rows(), partitionId);
                arrow::UInt32Builder int32Builder;
                ARROW_RETURN_NOT_OK(int32Builder.AppendValues(partitionIds));
                std::cout << "[FixedGridPartitioning] Mapped columns to partition ids" << std::endl;
                std::shared_ptr<arrow::Array> partitionIdsArrow;
                ARROW_ASSIGN_OR_RAISE(partitionIdsArrow, int32Builder.Finish());
                std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
                ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "partition_id", partitionIdsArrow));
                auto writeTable = arrow::Table::FromRecordBatches({updatedRecordBatch}).ValueOrDie();
                std::shared_ptr<arrow::dataset::Dataset> dataset = std::make_shared<arrow::dataset::InMemoryDataset>(writeTable);
                auto options = std::make_shared<arrow::dataset::ScanOptions>();
                std::vector<arrow::Expression> filterExpressions;
                filterExpressions.reserve(partitionColumns.size());
                for (int i = 0; i < partitionColumns.size(); ++i) {
                    filterExpressions.emplace_back(arrow::compute::and_(
                                arrow::compute::greater_equal(
                                arrow::compute::field_ref(partitionColumns[i]),
                                arrow::compute::literal(cellsCoordinates[partitionId][i].first)),
                                // AND
                                arrow::compute::less_equal(
                                arrow::compute::field_ref(partitionColumns[i]),
                                arrow::compute::literal(cellsCoordinates[partitionId][i].second))));
                }
                std::cout << arrow::compute::and_(filterExpressions).ToString();
                options->filter = arrow::compute::and_(filterExpressions);
                auto builder = arrow::dataset::ScannerBuilder(dataset, options);
                auto scanner = builder.Finish();
                std::shared_ptr<arrow::Table> partitionedTable = scanner.ValueOrDie()->ToTable().ValueOrDie();
                if (partitionedTable->num_rows() > 0){
                    std::filesystem::path subPartitionsFolder = folder / std::to_string(partitionId);
                    if (!std::filesystem::exists(subPartitionsFolder)) {
                        std::filesystem::create_directory(subPartitionsFolder);
                    }
                    if (!addColumnPartitionId) {
                        ARROW_ASSIGN_OR_RAISE(partitionedTable, partitionedTable->RemoveColumn(0));
                    }
                    std::filesystem::path outfile = subPartitionsFolder / ("b" + std::to_string(batchId) + ".parquet");
                    ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTable(partitionedTable, outfile));
                    completedPartitions += 1;
                    std::cout << "[GridFilePartitioning] Generate partitioned table with " << partitionedTable->num_rows() << " rows" << std::endl;
                    int progress = (int) (float(completedPartitions) / float(numPartitions) * 100);
                    std::cout << "[GridFilePartitioning] Progress: " << progress << " %" << std::endl;
                }
            }
            totalNumRows += recordBatch->num_rows();
        }
        ARROW_RETURN_NOT_OK(storage::DataWriter::mergeBatches(folder, uniquePartitionIds));
        return arrow::Status::OK();
    }

    void GridFilePartitioning::computeLinearScales(std::vector<std::shared_ptr<common::Point>> &allRows,
                                                   uint32_t initialCoord) {
        std::queue<std::pair<std::vector<std::shared_ptr<common::Point>>, uint32_t>> queue;
        queue.emplace(allRows, initialCoord);
        std::cout << "[GridFilePartitioning] Start computing linear scales" << std::endl;
        while(!queue.empty()){
            auto queuePop = queue.front();
            std::vector<std::shared_ptr<common::Point>> rows = queuePop.first;
            uint32_t coord = queuePop.second;
            auto columnIndex = coord % numColumns;
            auto columnScaleSize = linearScales[columnIndex].size();
            for (int i = 0; i < columnScaleSize - 1; ++i) {
                auto min = linearScales[columnIndex][i];
                auto max = linearScales[columnIndex][i+1];
                auto newSplit = (max + min) / 2;
                linearScales[columnIndex].insert(upper_bound(linearScales[columnIndex].begin(),
                                                             linearScales[columnIndex].end(), newSplit), newSplit);
            }
            // For each slice/interval in the scale for this dimension
            for (int i = 0; i < linearScales[columnIndex].size() - 1; ++i) {
                // Retrieve point coordinates matching the interval
                auto min = linearScales[columnIndex][i];
                auto max = linearScales[columnIndex][i+1];
                std::sort(rows.begin(), rows.end(),
                          [&](const std::shared_ptr<common::Point> &a, const std::shared_ptr<common::Point> &b) {
                              return a->at(columnIndex) < b->at(columnIndex);
                          });
                auto lower = std::make_shared<std::vector<double>>(*rows.at(0));
                auto upper = std::make_shared<std::vector<double>>(*rows.at(0));
                lower->at(columnIndex) = min;
                upper->at(columnIndex) = max;
                auto lowerBound = std::lower_bound(rows.begin(), rows.end(), lower,
                                           [&](const std::shared_ptr<common::Point> &a,
                                               const std::shared_ptr<common::Point> &b) {
                    return a->at(columnIndex) < b->at(columnIndex); });
                auto upperBound = std::upper_bound(rows.begin(), rows.end(), upper,
                                           [&](const std::shared_ptr<common::Point> &a,
                                               const std::shared_ptr<common::Point> &b) {
                    return a->at(columnIndex) < b->at(columnIndex); });
                auto pointsInRange = std::vector<std::shared_ptr<common::Point>>(lowerBound, upperBound);
                if (!queue.empty()){
                    queue.pop();
                }
                // If more splits are needed, add to the queue for further processing
                if (pointsInRange.size() > cellCapacity){
                    queue.emplace(pointsInRange, coord + 1);
                }
                // Otherwise, this set of points is a new partition
            }
        }
    }

}