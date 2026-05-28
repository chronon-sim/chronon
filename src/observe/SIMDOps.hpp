// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

namespace chronon::observe::simd {

inline uint64_t minNonZero(const uint64_t* data, size_t count) noexcept {
    uint64_t result = UINT64_MAX;

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
