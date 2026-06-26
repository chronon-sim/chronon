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

#include "../../observe/PipelineTraceFormat.hpp"

namespace chronon::sender {

struct SchedulerTimelineStyle {
    std::string category;
    std::string name;
};

inline SchedulerTimelineStyle schedulerTimelineColoredStyle(std::string_view visible_name,
                                                            uint64_t color_hash) {
    return {observe::pipelineColorCategory(color_hash),
            observe::pipelineColoredEventName(visible_name, color_hash)};
}

inline SchedulerTimelineStyle schedulerStallStyle(std::string_view stall_name) {
    if (stall_name == "stall: cluster-dep") {
        return schedulerTimelineColoredStyle(stall_name, 0x2bcd8b204bcc2742ULL);
    }
    if (stall_name == "stall: lookahead-floor") {
        return schedulerTimelineColoredStyle(stall_name, 0x65d190ae0c86b4e4ULL);
    }
    if (stall_name == "stall: no-ready-cluster") {
        return schedulerTimelineColoredStyle(stall_name, 0x01895ee632ebb9248ULL);
    }
    return schedulerTimelineColoredStyle(stall_name, 0x2bcd8b204bcc2742ULL);
}

}  // namespace chronon::sender
