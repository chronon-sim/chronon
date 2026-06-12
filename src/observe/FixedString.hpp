// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstddef>
#include <string_view>

namespace chronon::observe {

/** @brief Compile-time string literal usable as a non-type template parameter. */
template <size_t N>
struct FixedString {
    char data[N]{};

    constexpr FixedString() = default;

    constexpr FixedString(const char (&str)[N]) {
        for (size_t i = 0; i < N; ++i) {
            data[i] = str[i];
        }
    }

    constexpr operator std::string_view() const noexcept { return std::string_view(data, N - 1); }

    constexpr const char* c_str() const noexcept { return data; }
    constexpr size_t size() const noexcept { return N - 1; }
    constexpr bool operator==(const FixedString&) const = default;
};

template <size_t N>
FixedString(const char (&)[N]) -> FixedString<N>;

}  // namespace chronon::observe
