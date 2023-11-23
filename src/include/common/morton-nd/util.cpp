#include "mortonND_LUT.h"

namespace mortonnd{

    using morton_t = uint64_t;
    using morton_decoded_t = std::vector<uint64_t>;

    static constexpr auto mortonEncoder2d = mortonnd::MortonNDLutEncoder<2, 32, 8>();
    static constexpr auto mortonEncoder3d = mortonnd::MortonNDLutEncoder<3, 21, 8>();
    static constexpr auto mortonEncoder4d = mortonnd::MortonNDLutEncoder<4, 16, 8>();
    static constexpr auto mortonEncoder5d = mortonnd::MortonNDLutEncoder<5, 12, 8>();
    static constexpr auto mortonEncoder6d = mortonnd::MortonNDLutEncoder<6, 10, 8>();
    static constexpr auto mortonEncoder7d = mortonnd::MortonNDLutEncoder<7, 9, 8>();
    static constexpr auto mortonEncoder8d = mortonnd::MortonNDLutEncoder<8, 8, 8>();

    static constexpr auto mortonDecoder2d = mortonnd::MortonNDLutDecoder<2, 32, 8>();
    static constexpr auto mortonDecoder3d = mortonnd::MortonNDLutDecoder<3, 21, 8>();
    static constexpr auto mortonDecoder4d = mortonnd::MortonNDLutDecoder<4, 16, 8>();
    static constexpr auto mortonDecoder5d = mortonnd::MortonNDLutDecoder<5, 12, 8>();
    static constexpr auto mortonDecoder6d = mortonnd::MortonNDLutDecoder<6, 10, 8>();
    static constexpr auto mortonDecoder7d = mortonnd::MortonNDLutDecoder<7, 9, 8>();
    static constexpr auto mortonDecoder8d = mortonnd::MortonNDLutDecoder<8, 8, 8>();

    template <class Tuple, class T = std::decay_t<std::tuple_element_t<0, std::decay_t<Tuple>>>>
    std::vector<T> tuple_to_vector(Tuple&& tuple)
    {
        return std::apply([](auto&&... elems){
            return std::vector<T>{std::forward<decltype(elems)>(elems)...};
        }, std::forward<Tuple>(tuple));
    }

    template<int dim>
    inline morton_t encode_morton(const std::array<uint64_t, dim> &x);

    template<>
    inline morton_t encode_morton<2>(const std::array<uint64_t, 2> &x) {
        return mortonEncoder2d.Encode(x[0], x[1]);
    }

    template<>
    inline morton_t encode_morton<3>(const std::array<uint64_t, 3> &x) {
        return mortonEncoder3d.Encode(x[0], x[1], x[2]);
    }

    template<>
    inline morton_t encode_morton<4>(const std::array<uint64_t, 4> &x) {
        return mortonEncoder4d.Encode(x[0], x[1], x[2], x[3]);
    }

    template<>
    inline morton_t encode_morton<5>(const std::array<uint64_t, 5> &x) {
        return mortonEncoder5d.Encode(x[0], x[1], x[2], x[3], x[4]);
    }

    template<>
    inline morton_t encode_morton<6>(const std::array<uint64_t, 6> &x) {
        return mortonEncoder6d.Encode(x[0], x[1], x[2], x[3], x[4], x[5]);
    }

    template<>
    inline morton_t encode_morton<7>(const std::array<uint64_t, 7> &x) {
        return mortonEncoder7d.Encode(x[0], x[1], x[2], x[3], x[4], x[5], x[6]);
    }

    template<>
    inline morton_t encode_morton<8>(const std::array<uint64_t, 8> &x) {
        return mortonEncoder8d.Encode(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]);
    }

    template<int dim>
    inline morton_decoded_t decode_morton(const uint64_t &x);

    template<>
    inline morton_decoded_t decode_morton<2>(const uint64_t &x) {
        return tuple_to_vector(mortonDecoder2d.Decode(x));
    }

    template<>
    inline morton_decoded_t decode_morton<3>(const uint64_t &x) {
        return tuple_to_vector(mortonDecoder3d.Decode(x));
    }

    template<>
    inline morton_decoded_t decode_morton<4>(const uint64_t &x) {
        return tuple_to_vector(mortonDecoder4d.Decode(x));
    }

    template<>
    inline morton_decoded_t decode_morton<5>(const uint64_t &x) {
        return tuple_to_vector(mortonDecoder5d.Decode(x));
    }

    template<>
    inline morton_decoded_t decode_morton<6>(const uint64_t &x) {
        return tuple_to_vector(mortonDecoder6d.Decode(x));
    }

    template<>
    inline morton_decoded_t decode_morton<7>(const uint64_t &x) {
        return tuple_to_vector(mortonDecoder7d.Decode(x));
    }

    template<>
    inline morton_decoded_t decode_morton<8>(const uint64_t &x) {
        return tuple_to_vector(mortonDecoder8d.Decode(x));
    }


}