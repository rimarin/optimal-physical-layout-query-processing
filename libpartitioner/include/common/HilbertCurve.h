#ifndef COMMON_HILBERTCURVE_H
#define COMMON_HILBERTCURVE_H

#include <cstdio>

namespace common
{
    class HilbertCurve {
        //+++++++++++++++++++++++++++ PUBLIC-DOMAIN SOFTWARE ++++++++++++++++++++++++++
        // Source: https://stackoverflow.com/a/71683904
        // Functions: TransposeToAxes AxesToTranspose
        // Purpose: Transform in-place between Hilbert transpose and geometrical axes
        // Example: b=5 bits for each of n=3 coordinates.
        // 15-bit Hilbert integer = A B C D E F G H I J K L M N O is stored
        // as its Transpose
        //        X[0] = A D G J M             X[2]|
        //        X[1] = B E H K N <------->       | /X[1]
        //        X[2] = C F I L O            axes |/
        //               high  low                 0------ X[0]
        // Axes are stored conventionally as b-bit integers.
        // Author: John Skilling 20 Apr 2001 to 11 Oct 2003
        //-----------------------------------------------------------------------------
        // Other:
        // - Hilbert Curve Visualization: https://pypi.org/project/numpy-hilbert-curve/
        // - Type support: only supports int, due to the bitwise implementation. The bitwise operators do not make
        //   sense when applied to double or float, and the standard says that the bitwise operators (~, &, |, ^, >>,
        //   <<, and the assignment variants) do not accept double or float operands. Both double and float have 3
        //   sections - a sign bit, an exponent, and the mantissa, any attempt to shift would lead to ambiguity.
    public:
        typedef int64_t coord_t;

        void transposeToAxes(coord_t* X, int b, int n) // Position, #bits, dimension
        {
            coord_t N = 2 << (b - 1), P, Q, t;

            // Gray decode by H ^ (H/2)
            t = X[n - 1] >> 1;
            // Corrected error in Skilling's paper on the following line. The appendix had i >= 0 leading to negative array index.
            for (int i = n - 1; i > 0; i--) X[i] ^= X[i - 1];
            X[0] ^= t;

            // Undo excess work
            for (Q = 2; Q != N; Q <<= 1) {
                P = Q - 1;
                for (int i = n - 1; i >= 0; i--)
                    if (X[i] & Q) // Invert
                        X[0] ^= P;
                    else { // Exchange
                        t = (X[0] ^ X[i]) & P;
                        X[0] ^= t;
                        X[i] ^= t;
                    }
            }
        }

        void axesToTranspose(coord_t* X, int b, int n) // Position, #bits, dimension
        {
            coord_t M = 1 << (b - 1), P, Q, t;

            // Inverse undo
            for (Q = M; Q > 1; Q >>= 1) {
                P = Q - 1;
                for (int i = 0; i < n; i++)
                    if (X[i] & Q) // Invert
                        X[0] ^= P;
                    else { // Exchange
                        t = (X[0] ^ X[i]) & P;
                        X[0] ^= t;
                        X[i] ^= t;
                    }
            }

            // Gray encode
            for (int i = 1; i < n; i++) X[i] ^= X[i - 1];
            t = 0;
            for (Q = M; Q > 1; Q >>= 1)
                if (X[n - 1] & Q) t ^= Q - 1;
            for (int i = 0; i < n; i++) X[i] ^= t;
        }

        int interleaveBits(coord_t* X, int b, int n) // Position, #bits, dimension
        {
            unsigned int codex = 0, codey = 0, codez = 0;
            unsigned int code[n];

            for (int i = 0; i < n; ++i) {
                code[i] = 0;
            }

            const int nbits2 = 2 * b;

            for (int i = 0, andbit = 1; i < nbits2; i += 2, andbit <<= 1) {
                for (int j = 0; j < n; j++){
                    code[j] |= (unsigned int)(X[j] & andbit) << i;
                }
                codex |= (unsigned int)(X[0] & andbit) << i;
                codey |= (unsigned int)(X[1] & andbit) << i;
                codez |= (unsigned int)(X[2] & andbit) << i;
            }

            unsigned int result = 0;
            for (int i = 0; i < n; ++i) {
                result |= (code[i] << (n - i - 1));
            }
            return result;
        }

        // From https://github.com/Forceflow/libmorton/blob/main/include/libmorton/morton3D.h
        void uninterleaveBits(coord_t* X, int b, int n, unsigned int code) // Position, #bits, dimension
        {
            X[0] = X[1] = X[2] = 0;

            for (unsigned int i = 0; i <= b; ++i) {
                unsigned int selector = 1;
                unsigned int shift_selector = 3 * i;
                unsigned int shiftback = 2 * i;
                X[2] |= (code & (selector << shift_selector)) >> (shiftback);
                X[1] |= (code & (selector << (shift_selector + 1))) >> (shiftback + 1);
                X[0] |= (code & (selector << (shift_selector + 2))) >> (shiftback + 2);
            }
        }

    };
}

#endif //COMMON_HILBERTCURVE_H

#endif //COMMON_HILBERTCURVE_H

#endif //COMMON_HILBERTCURVE_H
