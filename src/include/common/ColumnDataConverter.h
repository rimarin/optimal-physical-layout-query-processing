#ifndef OPLQP_COLUMNDATACONVERTER_H
#define OPLQP_COLUMNDATACONVERTER_H

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
#include <arrow/table.h>

#include "Point.h"

namespace common {

    class ColumnDataConverter {
    public:

        // Columnar to row layout: vector of columns is transformed into a vector of points (rows)
        static std::vector<Point> toRows(std::vector<std::vector<double>> &columnData) {
            std::vector<Point> points;
            auto numColumns = columnData.size();
            auto numRows = columnData[0].size();
            for (int i = 0; i < numRows; i++) {
                Point point = {};
                for (int j = 0; j < numColumns; j++) {
                    point.emplace_back(columnData[j][i]);
                }
                points.emplace_back(point);
            }
            return points;
        }

        arrow::Result<std::vector<std::vector<double>>> toDouble(std::vector<std::shared_ptr<arrow::Array>> &columnData){
            outputType = "double";
            for (const auto &data: columnData){
                ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*data, this));
            }
            return convertedData;
        }

        arrow::Result<std::vector<std::vector<double>>> toInt64(std::vector<std::shared_ptr<arrow::Array>> &columnData){
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

        template<typename ArrayType, typename T = typename ArrayType::TypeClass>
        arrow::enable_if_number<T, arrow::Status> Visit(const ArrayType &array) {
            std::vector<double> castedArray = {};
            for (std::optional<typename T::c_type> value: array) {
                if (value.has_value()) {
                    if (outputType == "int64"){
                        castedArray.emplace_back(static_cast<int64_t>(value.value()));
                    }
                    else{
                        castedArray.emplace_back(static_cast<double>(value.value()));
                    }
                }
            }
            if (!castedArray.empty()) {
                convertedData.emplace_back(castedArray);
            }
            return arrow::Status::OK();
        }
    private:
        std::vector<std::vector<double>> convertedData = {};
        std::string outputType = "double";
    };
}

#endif //OPLQP_COLUMNDATACONVERTER_H
