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
#include <vector>

namespace chronon::observe {

class PerfettoTraceWriter;

/**
 * @brief Recorded timeline slices, decoupled from their recorder.
 *
 * Wall-clock duration events grouped into per-stream lanes, with strings stored
 * as [offset,len) slices into a per-stream byte arena (zero per-event
 * allocation at record time). Recorders (e.g. the scheduler timeline) build
 * this in place and *move* it to the ObservationBackend at shutdown, so the
 * unified timeline.pftrace export has no lifetime coupling to the recorder.
 */
struct TimelineStreamData {
    /// POD slice; strings are [offset,len) views into the owning stream's arena.
    struct Event {
        uint32_t cat_off = 0, cat_len = 0;
        uint32_t name_off = 0, name_len = 0;
        uint32_t detail_off = 0, detail_len = 0;
        uint64_t cycle = 0;
        uint64_t ts_ns = 0;
        uint64_t dur_ns = 0;
        bool instant = false;
    };

    std::string process_name = "Chronon Scheduler";
    int32_t pid = 2;  ///< Synthetic pid for the timeline's process group track.

    std::vector<std::string> stream_names;
    std::vector<std::vector<Event>> streams;  ///< Slices per stream (lane).
    std::vector<std::string> arenas;          ///< Byte arena per stream backing Event strings.
    uint64_t dropped_events = 0;

    [[nodiscard]] bool empty() const noexcept {
        for (const auto& s : streams) {
            if (!s.empty()) {
                return false;
            }
        }
        return true;
    }
};

/// Emit @p data as complete slices under a dedicated process group track.
void writeTimeline(PerfettoTraceWriter& writer, const TimelineStreamData& data);

}  // namespace chronon::observe
