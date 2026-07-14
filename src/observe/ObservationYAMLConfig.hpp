// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Specifies when observation should be active.
 *
 * RANGE: active for cycles `[range_start, range_end]`.
 * PERIODIC: active for `window` cycles every `period` cycles, starting at `offset`.
 *
 * @code
 *   - range: [10000, 20000]
 *   - periodic: { window: 1000, period: 10000, offset: 0 }
 * @endcode
 */
struct TemporalFilter {
    enum class Type { RANGE, PERIODIC };

    Type type = Type::RANGE;

    uint64_t range_start = 0;
    uint64_t range_end = UINT64_MAX;

    uint64_t window = 0;
    uint64_t period = 0;
    uint64_t offset = 0;

    [[nodiscard]] bool matches(uint64_t cycle) const noexcept {
        switch (type) {
            case Type::RANGE:
                return cycle >= range_start && cycle <= range_end;

            case Type::PERIODIC:
                if (period == 0) return true;  // Invalid period — treat as always-match.
                if (cycle < offset) return false;
                uint64_t pos_in_period = (cycle - offset) % period;
                return pos_in_period < window;
        }
        return true;
    }

    static TemporalFilter range(uint64_t start, uint64_t end) {
        TemporalFilter f;
        f.type = Type::RANGE;
        f.range_start = start;
        f.range_end = end;
        return f;
    }

    static TemporalFilter periodic(uint64_t window, uint64_t period, uint64_t offset = 0) {
        TemporalFilter f;
        f.type = Type::PERIODIC;
        f.window = window;
        f.period = period;
        f.offset = offset;
        return f;
    }
};

/**
 * @brief Glob pattern selecting categories, with optional temporal overrides.
 *
 * Patterns support `*` ("*" matches all, "icache_*" matches prefixes, etc.).
 */
struct CategoryPattern {
    std::string pattern;
    bool enabled = true;

    std::vector<TemporalFilter> temporal;

    /// OR semantics across filters; empty filters list means "always active".
    [[nodiscard]] bool matchesCycle(uint64_t cycle) const noexcept {
        if (temporal.empty()) {
            return true;
        }
        for (const auto& filter : temporal) {
            if (filter.matches(cycle)) {
                return true;
            }
        }
        return false;
    }
};

/**
 * @brief Counter CSV layout.
 *
 * - Long:    One row per (cycle, unit, counter, value); streaming-friendly.
 * - Pivoted: Rows are cycles, columns are counters; compact and readable.
 */
enum class CounterCsvFormat : uint8_t { Long, Pivoted };

/** @brief Counter-collection settings. */
struct CountersYAMLConfig {
    bool enabled = true;
    bool csv_output = true;
    CounterCsvFormat csv_format = CounterCsvFormat::Pivoted;
    /// Nominal interval for owner-thread SPSC snapshots (0 = disabled).
    /// Lookahead workers may sample within their configured run-ahead window.
    uint64_t periodic_dump_cycles = 0;
    bool dump_on_shutdown = true;
};

/** @brief Config for a single log-level channel (debug/info/warn/error). */
struct ChannelConfig {
    bool enabled = true;
    std::string file;  ///< Empty = default events.log.
    std::optional<BackpressurePolicy> backpressure;
    std::optional<uint32_t> backpressure_max_spins;
};

/**
 * @brief Trace channel config.
 *
 * Trace events go to timeline.pftrace (when the timeline sink is enabled) and,
 * when @c text is set, additionally to the text log.
 */
struct TraceChannelConfig {
    bool enabled = false;
    bool text = false;  ///< Also mirror trace events to the text log.
    std::string file;   ///< Text mirror destination; empty = default events.log.

    std::optional<BackpressurePolicy> backpressure;
    std::optional<uint32_t> backpressure_max_spins;
};

/**
 * @brief Unified Perfetto timeline output (timeline.pftrace).
 *
 * One file carries simulation trace events (timestamp = cycle, 1 cycle
 * rendered as 1 ns), counter tracks, and — when the scheduler timeline is
 * enabled — wall-clock scheduler execution slices. Opens in ui.perfetto.dev.
 */
struct TimelineYAMLConfig {
    bool enabled = true;
    std::string file = "timeline.pftrace";  ///< Relative to the run output directory.
    bool trace_events = true;               ///< Simulation trace channel → instant events.
    bool counters = true;                   ///< Counter snapshots → counter tracks.
    bool compress = true;                   ///< Deflate packet batches (compressed_packets).
};

/**
 * @brief Unified per-channel configuration for all event output.
 *
 * Each event type (debug/info/warn/error/trace) is an independent channel with
 * its own enable settings; categories and temporal filters are shared.
 */
struct UnifiedLoggingConfig {
    bool enabled = true;

    ChannelConfig debug_channel;
    ChannelConfig info_channel;
    ChannelConfig warn_channel;
    ChannelConfig error_channel;
    TraceChannelConfig trace_channel;

    std::vector<TemporalFilter> temporal;
    std::vector<CategoryPattern> categories;
};

/** @brief Per-unit configuration overrides. */
struct UnitObservationOverride {
    std::optional<CountersYAMLConfig> counters;
    std::optional<UnifiedLoggingConfig> logging;

    [[nodiscard]] bool hasOverrides() const noexcept {
        return counters.has_value() || logging.has_value();
    }
};

/** @brief Complete observation configuration parsed from YAML. */
struct ObservationYAMLConfig {
    bool enabled = false;

    std::string output_dir = "out";
    size_t queue_capacity = 256 * 1024;

    BackpressurePolicy backpressure = BackpressurePolicy::BoundedWait;
    uint32_t backpressure_max_spins = 4096;

    CountersYAMLConfig counters;
    UnifiedLoggingConfig unified_logging;
    TimelineYAMLConfig timeline;

    /// Key = unit instance name.
    std::unordered_map<std::string, UnitObservationOverride> unit_overrides;

    [[nodiscard]] CountersYAMLConfig getCountersConfig(const std::string& unit_name) const {
        auto it = unit_overrides.find(unit_name);
        if (it != unit_overrides.end() && it->second.counters.has_value()) {
            return *it->second.counters;
        }
        return counters;
    }

    [[nodiscard]] UnifiedLoggingConfig getUnifiedLoggingConfig(const std::string& unit_name) const {
        auto it = unit_overrides.find(unit_name);
        if (it != unit_overrides.end() && it->second.logging.has_value()) {
            return *it->second.logging;
        }
        return unified_logging;
    }
};

}  // namespace chronon::observe
