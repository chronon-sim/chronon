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

#include "Types.hpp"

namespace chronon::observe {

struct PipelineTraceFields {
    std::string_view id;
    std::string_view note;
    std::string track_path;
    std::string category;
    std::string event_name;
    uint64_t flow_id = 0;
    bool has_flow_id = false;
};

bool isPipeCategory(CategoryMask category);

uint64_t pipelineColorHash(uint64_t id);
uint64_t pipelineColorHash(std::string_view key);
std::string pipelineColorCategory(uint64_t color_hash);
std::string pipelineColoredEventName(std::string_view visible_name, uint64_t color_hash);

bool parsePipelineTraceMessage(std::string_view source_name, std::string_view message,
                               PipelineTraceFields& out);

}  // namespace chronon::observe
