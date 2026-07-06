// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace chronon::observe {

uint64_t pipelineColorHash(uint64_t id);
uint64_t pipelineColorHash(std::string_view key);
std::string pipelineColorCategory(uint64_t color_hash);
std::string pipelineColoredEventName(std::string_view visible_name, uint64_t color_hash);

}  // namespace chronon::observe
