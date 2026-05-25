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
 * @brief How an event channel writes its output.
 *
 * - Text:   Human-readable (events.log).
 * - Binary: Compact binary (events.ctrace).
 * - Both:   Both sinks.
 */
enum class OutputFormat : uint8_t { Text, Binary, Both };

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
 * @brief Glob pattern selecting categories, with optional temporal and per-channel format
 * overrides.
 *
 * Patterns support `*` ("*" matches all, "icache_*" matches prefixes, etc.).
 * Format priority: channel-specific override > shorthand `format` > channel default.
 */
struct CategoryPattern {
    std::string pattern;
    bool enabled = true;

    /// Shorthand: sets all channels when no channel-specific override is set.
    std::optional<OutputFormat> format;
    std::optional<OutputFormat> trace_format;
    std::optional<OutputFormat> debug_format;
    std::optional<OutputFormat> info_format;
    std::optional<OutputFormat> warn_format;
    std::optional<OutputFormat> error_format;

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

    /// Channel-specific override wins over the shorthand `format`.
    [[nodiscard]] std::optional<OutputFormat> getEffectiveFormat(
        const std::optional<OutputFormat>& channel_override) const noexcept {
        if (channel_override.has_value()) {
            return channel_override;
        }
        return format;
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
    uint64_t periodic_dump_cycles = 0;  ///< 0 = disabled.
    bool dump_on_shutdown = true;
};

/** @brief Compression settings for binary traces. */
struct CompressionConfig {
    bool enabled = true;
    std::string algorithm = "zstd";  ///< Only zstd supported.
    int level = 3;                   ///< 1-22; 3 is the fast default.
    size_t block_size = 65536;
};

/** @brief Config for a single log-level channel (debug/info/warn/error). */
struct ChannelConfig {
    bool enabled = true;
    OutputFormat format = OutputFormat::Text;
    std::string file;  ///< Empty = default events.log.
    std::optional<BackpressurePolicy> backpressure;
    std::optional<uint32_t> backpressure_max_spins;
};

/** @brief Trace channel config with binary-specific extensions. */
struct TraceChannelConfig {
    bool enabled = false;
    OutputFormat format = OutputFormat::Binary;
    std::string file;

    CompressionConfig compression;
    bool embed_schema = true;
    bool generate_index = true;

    std::optional<BackpressurePolicy> backpressure;
    std::optional<uint32_t> backpressure_max_spins;
};

/**
 * @brief Unified per-channel configuration for all event output.
 *
 * Each event type (debug/info/warn/error/trace) is an independent channel with
 * its own enable/format settings; categories and temporal filters are shared.
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

    [[nodiscard]] bool needsTextOutput() const noexcept {
        if (debug_channel.enabled && (debug_channel.format == OutputFormat::Text ||
                                      debug_channel.format == OutputFormat::Both)) {
            return true;
        }
        if (info_channel.enabled && (info_channel.format == OutputFormat::Text ||
                                     info_channel.format == OutputFormat::Both)) {
            return true;
        }
        if (warn_channel.enabled && (warn_channel.format == OutputFormat::Text ||
                                     warn_channel.format == OutputFormat::Both)) {
            return true;
        }
        if (error_channel.enabled && (error_channel.format == OutputFormat::Text ||
                                      error_channel.format == OutputFormat::Both)) {
            return true;
        }
        if (trace_channel.enabled && (trace_channel.format == OutputFormat::Text ||
                                      trace_channel.format == OutputFormat::Both)) {
            return true;
        }
        // A category may override a Binary channel to Text/Both.
        for (const auto& cat : categories) {
            if (cat.format == OutputFormat::Text || cat.format == OutputFormat::Both) {
                return true;
            }
            if (cat.trace_format == OutputFormat::Text || cat.trace_format == OutputFormat::Both) {
                return true;
            }
            if (cat.debug_format == OutputFormat::Text || cat.debug_format == OutputFormat::Both) {
                return true;
            }
            if (cat.info_format == OutputFormat::Text || cat.info_format == OutputFormat::Both) {
                return true;
            }
            if (cat.warn_format == OutputFormat::Text || cat.warn_format == OutputFormat::Both) {
                return true;
            }
            if (cat.error_format == OutputFormat::Text || cat.error_format == OutputFormat::Both) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if any channel uses Binary or Both format.
     */
    [[nodiscard]] bool needsBinaryOutput() const noexcept {
        if (debug_channel.enabled && (debug_channel.format == OutputFormat::Binary ||
                                      debug_channel.format == OutputFormat::Both)) {
            return true;
        }
        if (info_channel.enabled && (info_channel.format == OutputFormat::Binary ||
                                     info_channel.format == OutputFormat::Both)) {
            return true;
        }
        if (warn_channel.enabled && (warn_channel.format == OutputFormat::Binary ||
                                     warn_channel.format == OutputFormat::Both)) {
            return true;
        }
        if (error_channel.enabled && (error_channel.format == OutputFormat::Binary ||
                                      error_channel.format == OutputFormat::Both)) {
            return true;
        }
        if (trace_channel.enabled && (trace_channel.format == OutputFormat::Binary ||
                                      trace_channel.format == OutputFormat::Both)) {
            return true;
        }
        for (const auto& cat : categories) {
            if (cat.format == OutputFormat::Binary || cat.format == OutputFormat::Both) {
                return true;
            }
            if (cat.trace_format == OutputFormat::Binary ||
                cat.trace_format == OutputFormat::Both) {
                return true;
            }
            if (cat.debug_format == OutputFormat::Binary ||
                cat.debug_format == OutputFormat::Both) {
                return true;
            }
            if (cat.info_format == OutputFormat::Binary || cat.info_format == OutputFormat::Both) {
                return true;
            }
            if (cat.warn_format == OutputFormat::Binary || cat.warn_format == OutputFormat::Both) {
                return true;
            }
            if (cat.error_format == OutputFormat::Binary ||
                cat.error_format == OutputFormat::Both) {
                return true;
            }
        }
        return false;
    }
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
