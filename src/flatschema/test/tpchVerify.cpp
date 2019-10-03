//
// Created by iclab on 10/2/19.
//

#include <iostream>

#ifdef __SSE2__
extern "C"
{
#include <emmintrin.h>
#include <mmintrin.h>
}
#endif

#ifdef HAVE_SSE42

#include <nmmintrin.h>

const size_t vectorLength = (1 << 20);

void simdTest() {
    uint64_t *uint64Vector = new uint64_t[vectorLength];
    uint32_t curCacheLine = 0;
    constexpr uint64_t mask = 0;
    __m128i SIMDmask = _mm_set1_epi64((__m64) mask);
    __m128i *SIMD_IMPRINT_VECTOR = reinterpret_cast<__m128i *>(uint64Vector);
    __m128i curSIMDImprint;
    curSIMDImprint = _mm_load_si128(&(SIMD_IMPRINT_VECTOR[curCacheLine]));
    std::cout << reinterpret_cast<uint64_t *>(SIMD_IMPRINT_VECTOR)[curCacheLine] << " "
              << reinterpret_cast<uint64_t *>(&SIMDmask)[0] << " " << _mm_test_all_zeros(curSIMDImprint, SIMDmask)
              << " " << _mm_test_all_ones(curSIMDImprint) << " " << _mm_test_all_ones(SIMDmask) << " "
              << _mm_test_mix_ones_zeros(curSIMDImprint, SIMDmask) << std::endl;
}

#else

void simdTest() {}

#endif

using namespace std;

string lineitem_raw = "../../../res/tpch/lineitem.tbl";

int main(int argc, char **argv) {
    simdTest();
    return 0;
}