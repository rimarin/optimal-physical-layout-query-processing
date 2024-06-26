#ifndef COMMON_COLUMN_DATA_CONVERTER_H
#define COMMON_COLUMN_DATA_CONVERTER_H

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <set>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/dataset/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "Point.h"

namespace common {

    class ColumnDataConverter {
    public:

        // Columnar to row layout (given the chunked arrays of the columns)
        // Optionally add a field with the row index
        std::vector<std::shared_ptr<Point>> toRows(std::vector<std::shared_ptr<arrow::ChunkedArray>> &columnsData,
                                                   bool addIndex = false) {
            std::vector<std::shared_ptr<arrow::Array>> columnsArrays = {};
            for (const auto &columnData: columnsData){
                std::shared_ptr<arrow::Array> columnArray = std::static_pointer_cast<arrow::Array>(arrow::ChunkedArray(columnData->chunks()).chunk(0));
                columnsArrays.emplace_back(columnArray);
            }
            if(addIndex){
                std::vector<uint32_t> rowIndexesRaw(columnsData[0]->length());
                std::iota(rowIndexesRaw.begin(), rowIndexesRaw.end(), 0);
                arrow::UInt32Builder int32builder;
                std::ignore = int32builder.AppendValues(rowIndexesRaw);
                std::shared_ptr<arrow::Array> rowIndexes;
                rowIndexes = int32builder.Finish().ValueOrDie();
                columnsArrays.emplace_back(rowIndexes);
            }
            std::vector<std::shared_ptr<common::Point>> columnDataDouble = toDouble(columnsArrays).ValueOrDie();
            return toRows(columnDataDouble);
        }

        // Columnar to row layout (given a columnar vectors of points)
        static std::vector<std::shared_ptr<Point>> toRows(std::vector<std::shared_ptr<common::Point>> &columnData) {
            std::vector<std::shared_ptr<Point>> points;
            size_t numColumns = columnData.size();
            size_t numRows = columnData[0]->size();
            for (size_t i = 0; i < numRows; i++) {
                std::shared_ptr<Point> point = std::make_shared<Point>();
                for (size_t j = 0; j < numColumns; j++) {
                    try{
                        point->emplace_back(columnData[j]->at(i));
                    }
                    catch (std::exception& e) {
                        std::cout << "Could not perform row conversion " << e.what() << std::endl;
                        continue;
                    }
                }
                points.emplace_back(point);
            }
            return points;
        }

        // Compute Pearson's second skewness coefficient (median skewness) https://en.wikipedia.org/wiki/Skewness
        // The result is converted to absolute value, since we do not care whether the skewness is positive or
        // negative, but we only need the intensity of the skewness.
        static double getColumnSkew(std::vector<double> &column){
            int size = column.size();
            double mean = std::accumulate(column.begin(), column.end(), 0.0) / size;
            double median = column.at(column.size() / 2);
            double variance = 0.0;
            std::for_each (std::begin(column), std::end(column), [&](const double d) {
                variance += (d - mean) * (d - mean);
            });
            double stdev = std::sqrt(variance / (size - 1));
            return std::abs(3 * (mean - median) / stdev);
        }

        arrow::Result<std::vector<std::shared_ptr<common::Point>>> toDouble(std::vector<std::shared_ptr<arrow::Array>> &columnData){
            outputType = "double";
            for (const auto &data: columnData){
                ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*data, this));
            }
            return convertedData;
        }

        arrow::Result<std::vector<std::shared_ptr<common::Point>>> toInt32(std::vector<std::shared_ptr<arrow::Array>> &columnData){
            outputType = "int32";
            for (const auto &data: columnData){
                ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*data, this));
            }
            return convertedData;
        }

        arrow::Result<std::vector<std::shared_ptr<common::Point>>> toInt64(std::vector<std::shared_ptr<arrow::Array>> &columnData){
            outputType = "int64";
            for (const auto &data: columnData){
                ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*data, this));
            }
            return convertedData;
        }

        // Default implementation
        arrow::Status Visit(const arrow::Array& array) {
            return arrow::Status::NotImplemented("Cannot convert to double, for this array of type ",
                                                 array.type()->ToString());
        }

        template<typename ArrayType, typename DataType = typename ArrayType::TypeClass, typename CType = typename DataType::c_type>
        arrow::enable_if_number<DataType, arrow::Status> Visit(const ArrayType &array) {
            auto castedArray = std::make_shared<std::vector<double>>();
            for (std::optional<CType> value: array) {
                if (value.has_value()) {
                    if (outputType == "int32"){
                        castedArray->emplace_back(static_cast<int32_t>(value.value()));
                    }
                    else if (outputType == "int64"){
                        castedArray->emplace_back(static_cast<int64_t>(value.value()));
                    }
                    else{
                        castedArray->emplace_back(static_cast<double>(value.value()));
                    }
                }
            }
            if (!castedArray->empty()) {
                convertedData.emplace_back(castedArray);
            }
            return arrow::Status::OK();
        }

        template<typename ArrayType, typename DataType = typename ArrayType::TypeClass, typename CType = typename DataType::c_type>
        arrow::enable_if_date<DataType, arrow::Status> Visit(const ArrayType &array) {
            auto castedArray = std::make_shared<std::vector<double>>();
            for (std::optional<CType> value: array) {
                if (value.has_value()) {
                    if (outputType == "int32"){
                        castedArray->emplace_back(static_cast<int32_t>(value.value()));
                    }
                    else if (outputType == "int64"){
                        castedArray->emplace_back(static_cast<int64_t>(value.value()));
                    }
                    else{
                        castedArray->emplace_back(static_cast<double>(value.value()));
                    }
                }
            }
            if (!castedArray->empty()) {
                convertedData.emplace_back(castedArray);
            }
            return arrow::Status::OK();
        }

        // From decimal conversion
        template<typename ArrayType, typename DataType = typename ArrayType::TypeClass, typename CType = typename DataType::c_type>
        arrow::enable_if_decimal<DataType, arrow::Status> Visit(const ArrayType &array) {
            auto castedArray = std::make_shared<std::vector<double>>();
            for (std::optional<CType> value: array) {
                if (value.has_value()) {
                    if (outputType == "int32"){
                        castedArray->emplace_back(static_cast<int32_t>(value.value()));
                    }
                    else if (outputType == "int64"){
                        castedArray->emplace_back(static_cast<int64_t>(value.value()));
                    }
                    else{
                        castedArray->emplace_back(static_cast<double>(value.value()));
                    }
                }
            }
            if (!castedArray->empty()) {
                convertedData.emplace_back(castedArray);
            }
            return arrow::Status::OK();
        }

        // From fixed size binary conversion
        template<typename ArrayType, typename DataType = typename ArrayType::TypeClass, typename CType = typename DataType::c_type>
        arrow::enable_if_fixed_size_binary<DataType, arrow::Status> Visit(const ArrayType &array) {
            auto castedArray = std::make_shared<std::vector<double>>();
            for (std::optional<CType> value: array) {
                if (value.has_value()) {
                    if (outputType == "int32"){
                        castedArray->emplace_back(static_cast<int32_t>(value.value()));
                    }
                    else if (outputType == "int64"){
                        castedArray->emplace_back(static_cast<int64_t>(value.value()));
                    }
                    else{
                        castedArray->emplace_back(static_cast<double>(value.value()));
                    }
                }
            }
            if (!castedArray->empty()) {
                convertedData.emplace_back(castedArray);
            }
            return arrow::Status::OK();
        }

        // From timestamp conversion
        template<typename ArrayType, typename DataType = typename ArrayType::TypeClass, typename CType = typename DataType::c_type>
        arrow::enable_if_timestamp<DataType, arrow::Status> Visit(const ArrayType &array) {
            auto castedArray = std::make_shared<std::vector<double>>();
            for (std::optional<CType> value: array) {
                if (value.has_value()) {
                    if (outputType == "int32"){
                        castedArray->emplace_back(static_cast<int32_t>(value.value()));
                    }
                    else if (outputType == "int64"){
                        castedArray->emplace_back(static_cast<int64_t>(value.value()));
                    }
                    else{
                        castedArray->emplace_back(static_cast<double>(value.value()));
                    }
                }
            }
            if (!castedArray->empty()) {
                convertedData.emplace_back(castedArray);
            }
            return arrow::Status::OK();
        }

    private:
        std::vector<std::shared_ptr<common::Point>> convertedData = {};
        std::string outputType = "double";
    };
}

#endif //COMMON_COLUMN_DATA_CONVERTER_H
