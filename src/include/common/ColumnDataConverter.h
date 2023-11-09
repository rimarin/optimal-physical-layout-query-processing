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

namespace common {

    class ColumnDataConverter {
    public:

        arrow::Result<std::vector<std::vector<double>>> toDouble(std::vector<std::shared_ptr<arrow::Array>> &columnData){
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
                    castedArray.emplace_back(static_cast<double>(value.value()));
                }
            }
            if (!castedArray.empty()) {
                convertedData.emplace_back(castedArray);
            }
            return arrow::Status::OK();
        }
    private:
        std::vector<std::vector<double>> convertedData = {};
    };
}

#endif //OPLQP_COLUMNDATACONVERTER_H
