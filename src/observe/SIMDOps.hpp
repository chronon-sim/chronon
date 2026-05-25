// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// SIMDOps.hpp
//
// Portable SIMD utility functions for observation backend hot paths.
// Provides AVX2, SSE2, and scalar fallback paths.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#if defined(__x86_64__) || defined(_M_X64)
#if defined(__AVX2__)
#include <immintrin.h>
#define CHRONON_HAS_AVX2 1
#elif defined(__SSE2__)
#include <emmintrin.h>
#define CHRONON_HAS_SSE2 1
#endif
#endif

namespace chronon::observe::simd {

/**
 * Compute the minimum non-zero value in an array of uint64_t.
 *
 * Used for watermark calculation: finds the minimum cycle across all
 * per-thread queues, skipping queues that never produced events (value == 0).
 *
 * @param data Array of uint64_t values
 * @param count Number of elements
 * @return Minimum non-zero value, or UINT64_MAX if all values are zero
 */
inline uint64_t minNonZero(const uint64_t* data, size_t count) noexcept {
    uint64_t result = UINT64_MAX;

#if CHRONON_HAS_AVX2
    // AVX2 path: process 4 uint64_t per iteration
    // Use integer comparison to filter zeros and find minimum
    if (count >= 4) {
        __m256i vmin = _mm256_set1_epi64x(static_cast<int64_t>(UINT64_MAX));
        __m256i vzero = _mm256_setzero_si256();

        size_t i = 0;
        for (; i + 4 <= count; i += 4) {
            __m256i vals = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));

            // Create mask: 0xFF..FF for non-zero elements, 0 for zero
            // Compare vals == 0, then invert
            __m256i is_zero = _mm256_cmpeq_epi64(vals, vzero);
            // Replace zeros with UINT64_MAX so they don't affect min
            __m256i adjusted = _mm256_blendv_epi8(vals, vmin, is_zero);

            // Manual min via comparison:
            // For unsigned comparison, bias by subtracting 0x8000...0
            // then use signed comparison
            __m256i bias = _mm256_set1_epi64x(static_cast<int64_t>(0x8000000000000000ULL));
            __m256i biased_adj = _mm256_sub_epi64(adjusted, bias);
            __m256i biased_min = _mm256_sub_epi64(vmin, bias);
            __m256i cmp = _mm256_cmpgt_epi64(biased_min, biased_adj);
            vmin = _mm256_blendv_epi8(vmin, adjusted, cmp);
        }

        // Horizontal reduction of 4 lanes
        alignas(32) uint64_t lanes[4];
        _mm256_store_si256(reinterpret_cast<__m256i*>(lanes), vmin);
        for (int j = 0; j < 4; ++j) {
            if (lanes[j] < result) {
                result = lanes[j];
            }
        }

        // Handle remaining elements
        for (; i < count; ++i) {
            if (data[i] > 0 && data[i] < result) {
                result = data[i];
            }
        }

        return result;
    }
#endif

    // Scalar path (also used as fallback for small arrays)
    // Unrolled loop for better ILP
    size_t i = 0;
    for (; i + 4 <= count; i += 4) {
        uint64_t v0 = data[i], v1 = data[i + 1];
        uint64_t v2 = data[i + 2], v3 = data[i + 3];
        if (v0 > 0 && v0 < result) {
            result = v0;
        }
        if (v1 > 0 && v1 < result) {
            result = v1;
        }
        if (v2 > 0 && v2 < result) {
            result = v2;
        }
        if (v3 > 0 && v3 < result) {
            result = v3;
        }
    }
    for (; i < count; ++i) {
        if (data[i] > 0 && data[i] < result) {
            result = data[i];
        }
    }

    return result;
}

}  // namespace chronon::observe::simd
