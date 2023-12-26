#ifndef COMMON_Z_ORDER_CURVE_H
#define COMMON_Z_ORDER_CURVE_H

#include <cstdio>
#include <vector>

#include "morton-nd/mortonND_LUT.h"
#include "morton-nd/util.cpp"


namespace common {
    class ZOrderCurve {
        // N-dimensional Morton number is computed from https://github.com/morton-nd/morton-nd
        // https://github.com/ClickHouse/ClickHouse/blob/662c653eec9cf6bd157b116d651d8be7e565ce4a/src/Functions/mortonEncode.cpp
        // https://github.com/morton-nd/morton-nd/blob/3795491a4aa3cdc916c8583094683f0d68df5bc0/test/mortonND_LUT_test.cpp#L9
    public:
        template<typename T, int dim>
        mortonnd::morton_t encode(const T *values) {
            std::array<uint64_t, dim> u;
            for (int d = 0; d < dim; d++) {
                u[d] = static_cast<uint64_t>(values[d]);
            }
            return mortonnd::encode_morton<dim>(u);
        }

        template<typename T>
        mortonnd::morton_t encode(const T *values, int dim) {
            if (dim == 2)
                return encode<T, 2>(values);
            else if (dim == 3)
                return encode<T, 3>(values);
            else if (dim == 4)
                return encode<T, 4>(values);
            else if (dim == 5)
                return encode<T, 5>(values);
            else if (dim == 6)
                return encode<T, 6>(values);
            else if (dim == 7)
                return encode<T, 7>(values);
            else if (dim == 8)
                return encode<T, 8>(values);
            else
                throw std::invalid_argument("Number of dimensions not supported");
        }

        template<typename T, int dim>
        mortonnd::morton_decoded_t decode(const T value) {
            auto u = static_cast<uint64_t>(value);
            return mortonnd::decode_morton<dim>(u);
        }

        template<typename T>
        mortonnd::morton_decoded_t decode(const T value, int dim) {
            if (dim == 2)
                return decode<T, 2>(value);
            else if (dim == 3)
                return decode<T, 3>(value);
            else if (dim == 4)
                return decode<T, 4>(value);
            else if (dim == 5)
                return decode<T, 5>(value);
            else if (dim == 6)
                return decode<T, 6>(value);
            else if (dim == 7)
                return decode<T, 7>(value);
            else if (dim == 8)
                return decode<T, 8>(value);
            else
                throw std::invalid_argument("Number of dimensions not supported");
        }
    };
}

#endif //COMMON_Z_ORDER_CURVE_H
