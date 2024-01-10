#include <algorithm>

#include "partitioning/GridFilePartitioning.h"

namespace partitioning {

    arrow::Status GridFilePartitioning::partition(){
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

        // Convert columns to rows
        std::vector<std::shared_ptr<arrow::ChunkedArray>> columnsData = dataReader->getColumns(columns).ValueOrDie();
        auto converter = common::ColumnDataConverter();
        auto rows = converter.toRows(columnsData);
        columnsData.clear();

        // Initialize linear scales with the domain range values for each dimension
        // Each linear scale will have only 2 elements, min and max
        linearScales = {};
        std::vector<std::pair<double, double>> scaleRangeIndexes;
        for (int i = 0; i < numColumns; ++i) {
            std::pair<double_t, double_t> columnStats = dataReader->getColumnStats(columns[i]).ValueOrDie();
            std::vector<double_t> columnDomain = {columnStats.first, columnStats.second};
            linearScales.emplace_back(columnDomain);
            scaleRangeIndexes.emplace_back(columnStats.first, columnStats.second);
        }

        // Compute linear scales
        computeLinearScales(rows, 0, scaleRangeIndexes);

        // Display generated linear scales
        for (const auto &linearScale: linearScales){
            std::string linearScalesStr(linearScale.begin(), linearScale.end());
            std::cout << "[GridFilePartitioning] Generated linear scale " << linearScalesStr << " rows" << std::endl;
        }

        // Determine the cell coordinates, which are the combinatorial combination of the obtained linear scales
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

        // Determine partition ids, assuming that each obtained cell is a partition
        std::set<uint32_t> uniquePartitionIds;
        for (int i = 0; i < cellsCoordinates.size(); ++i) {
            uniquePartitionIds.emplace(i);
        }
        auto numPartitions = uniquePartitionIds.size();

        // Read the table in batches
        auto batch_reader = dataReader->getTableBatchReader().ValueOrDie();
        uint32_t batchId = 0;
        uint32_t totalNumRows = 0;
        while (true) {
            // Try to load a new batch, when possible
            std::shared_ptr<arrow::RecordBatch> recordBatch;
            ARROW_RETURN_NOT_OK(batch_reader->ReadNext(&recordBatch));
            if (recordBatch == nullptr) {
                break;
            }

            // For each partition id,
            uint32_t completedPartitions = 0;
            for (const auto &partitionId: uniquePartitionIds){
                std::vector<uint32_t> partitionIds(recordBatch->num_rows(), partitionId);
                arrow::UInt32Builder int32Builder;
                ARROW_RETURN_NOT_OK(int32Builder.AppendValues(partitionIds));
                std::cout << "[GridFilePartitioning] Mapped columns to partition ids" << std::endl;
                std::shared_ptr<arrow::Array> partitionIdsArrow;
                ARROW_ASSIGN_OR_RAISE(partitionIdsArrow, int32Builder.Finish());
                std::shared_ptr<arrow::RecordBatch> updatedRecordBatch;
                ARROW_ASSIGN_OR_RAISE(updatedRecordBatch, recordBatch->AddColumn(0, "partition_id", partitionIdsArrow));
                auto writeTable = arrow::Table::FromRecordBatches({updatedRecordBatch}).ValueOrDie();
                std::shared_ptr<arrow::dataset::Dataset> dataset = std::make_shared<arrow::dataset::InMemoryDataset>(writeTable);
                auto options = std::make_shared<arrow::dataset::ScanOptions>();
                std::vector<arrow::Expression> filterExpressions;
                filterExpressions.reserve(columns.size());
                for (int i = 0; i < columns.size(); ++i) {
                    auto toInt32 = arrow::compute::CastOptions::Safe(arrow::int32());
                    // TODO: apply cast only when dealing with dates, otherwise it is an unnecessary overhead
                    filterExpressions.emplace_back(arrow::compute::and_(
                                arrow::compute::greater_equal(
                                    arrow::compute::call("cast",
                                                         {arrow::compute::field_ref(columns[i])},
                                                         toInt32),
                                    arrow::compute::literal(cellsCoordinates[partitionId][i].first)
                                ),
                                // AND
                                arrow::compute::less_equal(
                                    arrow::compute::call("cast",
                                                        {arrow::compute::field_ref(columns[i])},
                                                        toInt32),
                                    arrow::compute::literal(cellsCoordinates[partitionId][i].second))
                                )
                    );
                }
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
                    ARROW_RETURN_NOT_OK(storage::DataWriter::WriteTableToDisk(partitionedTable, outfile));
                    completedPartitions += 1;
                    std::cout << "[GridFilePartitioning] Generate partitioned table with " << partitionedTable->num_rows() << " rows" << std::endl;
                    int progress = (int) (float(completedPartitions) / float(numPartitions) * 100);
                    std::cout << "[GridFilePartitioning] Progress: " << progress << " %" << std::endl;
                }
            }
            totalNumRows += recordBatch->num_rows();
            std::cout << "[GridFilePartitioning] Batch " << batchId << " completed" << std::endl;
            std::cout << "[GridFilePartitioning] Imported " << totalNumRows << " out of " << numRows << " rows" << std::endl;
            batchId += 1;
        }
        std::cout << "[GridFilePartitioning] Partitioning of " << batchId << " batches completed" << std::endl;
        // Merge together the partitioned parts from different batches
        ARROW_RETURN_NOT_OK(storage::DataWriter::mergeBatches(folder, uniquePartitionIds));
        return arrow::Status::OK();
    }

    void GridFilePartitioning::computeLinearScales(std::vector<std::shared_ptr<common::Point>> &allRows,
                                                   uint32_t initialCoord, std::vector<std::pair<double, double>> scaleRangeIndexes) {
        std::queue<std::tuple<std::vector<std::shared_ptr<common::Point>>, uint32_t, std::vector<std::pair<double, double>>>> queue;
        queue.emplace(allRows, initialCoord, scaleRangeIndexes);
        std::cout << "[GridFilePartitioning] Start computing linear scales" << std::endl;
        while(!queue.empty()){
            auto queuePop = queue.front();
            if (!queue.empty()) {
                queue.pop();
            }
            std::vector<std::shared_ptr<common::Point>> rows = std::get<0>(queuePop);
            auto currentNumRows = rows.size();
            if (currentNumRows <= cellCapacity){
                continue;
            }
            uint32_t coord = std::get<1>(queuePop);
            std::vector<std::pair<double, double>> scaleRange = std::get<2>(queuePop);
            uint32_t columnIndex = coord % numColumns;
            double min = scaleRange[columnIndex].first;
            double max = scaleRange[columnIndex].second;
            bool minMaxMatch = max == min | (max - min) < 0.01;
            if (minMaxMatch){
                continue;
            }
            double newMidValue = (max + min) / 2;
            bool notSignificantNewMid = (newMidValue - min) < 0.01 || (max - newMidValue) < 0.01;
            if (notSignificantNewMid){
                continue;
            }
            if (std::find(linearScales[columnIndex].begin(), linearScales[columnIndex].end(), newMidValue) == linearScales[columnIndex].end()) {
                linearScales[columnIndex].insert(upper_bound(linearScales[columnIndex].begin(),
                                                             linearScales[columnIndex].end(), newMidValue),
                                                 newMidValue);
            }
            std::sort(rows.begin(), rows.end(),
                      [&](const std::shared_ptr<common::Point> &a, const std::shared_ptr<common::Point> &b) {
                          return a->at(columnIndex) < b->at(columnIndex);
                      });
            auto lower = std::make_shared<std::vector<double>>(*rows.at(0));
            auto mid = std::make_shared<std::vector<double>>(*rows.at(0));
            auto upper = std::make_shared<std::vector<double>>(*rows.at(0));
            lower->at(columnIndex) = min;
            mid->at(columnIndex) = newMidValue;
            upper->at(columnIndex) = max;
            auto lowerBound = std::lower_bound(rows.begin(), rows.end(), lower,
                                       [&](const std::shared_ptr<common::Point> &a,
                                           const std::shared_ptr<common::Point> &b) {
                return a->at(columnIndex) < b->at(columnIndex); });
            auto midBoundStart = std::upper_bound(rows.begin(), rows.end(), mid,
                                               [&](const std::shared_ptr<common::Point> &a,
                                                   const std::shared_ptr<common::Point> &b) {
                                                   return a->at(columnIndex) < b->at(columnIndex); });
            auto midBoundEnd = std::lower_bound(rows.begin(), rows.end(), mid,
                                               [&](const std::shared_ptr<common::Point> &a,
                                                   const std::shared_ptr<common::Point> &b) {
                                                   return a->at(columnIndex) < b->at(columnIndex); });
            auto upperBound = std::upper_bound(rows.begin(), rows.end(), upper,
                                       [&](const std::shared_ptr<common::Point> &a,
                                           const std::shared_ptr<common::Point> &b) {
                return a->at(columnIndex) < b->at(columnIndex); });
            auto firstHalfPoints = std::vector<std::shared_ptr<common::Point>>(lowerBound, midBoundStart);
            auto secondHalfPoints = std::vector<std::shared_ptr<common::Point>>(midBoundEnd, upperBound);
            // If more splits are needed, add to the queue for further processing
            auto numFirstHalfPoints = firstHalfPoints.size();
            auto numSecondHalfPoints = secondHalfPoints.size();
            if (numFirstHalfPoints > cellCapacity && !minMaxMatch){
                // pass also linear scale indexes, e.g. <1, 3, 2> for
                scaleRange[columnIndex] = std::pair<double, double>(min, newMidValue);
                queue.emplace(firstHalfPoints, coord + 1, scaleRange);
            }
            if (numSecondHalfPoints > cellCapacity && !minMaxMatch){
                // pass also linear scale indexes, e.g. <1, 3, 2> for
                scaleRange[columnIndex] = std::pair<double, double>(newMidValue, max);
                queue.emplace(secondHalfPoints, coord + 1, scaleRange);
            }
            // Otherwise, this set of points is a new partition
        }
    }

}