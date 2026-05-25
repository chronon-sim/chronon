// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <concepts>
#include <cstddef>
#include <type_traits>

namespace chronon::sender {

/** @brief Phase tag for even cycles; reads slot 0, writes slot 1. */
struct Phase0 {};

/** @brief Phase tag for odd cycles; reads slot 1, writes slot 0. */
struct Phase1 {};

/** @brief Constrains template parameters to Phase0 or Phase1. */
template <typename P>
concept ValidPhase = std::is_same_v<P, Phase0> || std::is_same_v<P, Phase1>;

template <ValidPhase P>
constexpr std::size_t read_slot_index() noexcept {
    if constexpr (std::is_same_v<P, Phase0>) {
        return 0;
    } else {
        return 1;
    }
}

template <ValidPhase P>
constexpr std::size_t write_slot_index() noexcept {
    if constexpr (std::is_same_v<P, Phase0>) {
        return 1;
    } else {
        return 0;
    }
}

}  // namespace chronon::sender
