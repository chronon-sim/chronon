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

struct PipelineTraceFields {
    std::string_view id;
    std::string_view note;
    std::string track_path;
    std::string category;
    std::string event_name;
    uint32_t stage_order = 0;
    uint64_t flow_id = 0;
};

bool isPipeCategory(uint32_t category);

bool parsePipelineTraceMessage(std::string_view source_name, std::string_view message,
                               PipelineTraceFields& out);

}  // namespace chronon::observe
