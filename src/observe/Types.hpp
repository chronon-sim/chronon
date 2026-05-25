// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace chronon::observe {

/** @brief Strongly typed counter identifier; indexes into the counter storage array. */
enum class CounterId : uint32_t {};

constexpr CounterId makeCounterId(uint32_t value) noexcept { return static_cast<CounterId>(value); }

constexpr uint32_t toIndex(CounterId id) noexcept { return static_cast<uint32_t>(id); }

/**
 * @brief Bitmask for observability categories.
 *
 * Bits 0-7 are reserved for built-in categories (counters, trace, log levels);
 * bits 8-63 are user-defined unit-specific categories.
 */
using CategoryMask = uint64_t;

namespace category {

constexpr CategoryMask COUNTER = 1ULL << 0;
constexpr CategoryMask TRACE = 1ULL << 1;
constexpr CategoryMask LOG_DEBUG = 1ULL << 2;
constexpr CategoryMask LOG_INFO = 1ULL << 3;
constexpr CategoryMask LOG_WARN = 1ULL << 4;
constexpr CategoryMask LOG_ERROR = 1ULL << 5;

constexpr CategoryMask ALL_LOGS = LOG_DEBUG | LOG_INFO | LOG_WARN | LOG_ERROR;
constexpr CategoryMask ALL = ~0ULL;
constexpr CategoryMask NONE = 0ULL;

/// User categories start at bit 8.
constexpr uint32_t USER_CATEGORY_START = 8;
constexpr CategoryMask USER_CATEGORY_MASK = ~((1ULL << USER_CATEGORY_START) - 1);

}  // namespace category

enum class LogLevel : uint8_t { Debug = 0, Info = 1, Warn = 2, Error = 3 };

constexpr CategoryMask logLevelToCategory(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::Debug:
            return category::LOG_DEBUG;
        case LogLevel::Info:
            return category::LOG_INFO;
        case LogLevel::Warn:
            return category::LOG_WARN;
        case LogLevel::Error:
            return category::LOG_ERROR;
    }
    return category::LOG_INFO;
}

/** @brief Metadata for a registered counter. */
struct CounterInfo {
    std::string name;
    std::string description;
    std::string unit;  ///< e.g., "cycles", "bytes", ""
};

/** @brief Metadata for a registered category. */
struct CategoryInfo {
    std::string name;
    std::string description;
    CategoryMask mask = 0;  ///< Assigned at registration.
};

/** @brief Identifies the observation channel for per-unit stats tracking. */
enum class ObservationChannel : uint8_t {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    NumChannels = 5
};

/** @brief Compile-time trait mapping LogLevel to ObservationChannel. */
template <LogLevel L>
struct LogLevelToChannel;

template <>
struct LogLevelToChannel<LogLevel::Debug> {
    static constexpr ObservationChannel value = ObservationChannel::Debug;
};
template <>
struct LogLevelToChannel<LogLevel::Info> {
    static constexpr ObservationChannel value = ObservationChannel::Info;
};
template <>
struct LogLevelToChannel<LogLevel::Warn> {
    static constexpr ObservationChannel value = ObservationChannel::Warn;
};
template <>
struct LogLevelToChannel<LogLevel::Error> {
    static constexpr ObservationChannel value = ObservationChannel::Error;
};

template <LogLevel L>
inline constexpr ObservationChannel log_level_channel_v = LogLevelToChannel<L>::value;

/** @brief Per-channel emit/drop counts. */
struct ObservationChannelStats {
    uint64_t emitted = 0;
    uint64_t dropped = 0;
};

/**
 * @brief Aggregated per-unit observation statistics.
 *
 * Operations are plain uint64_t increments (~0.3ns), not atomic — the owning unit
 * is single-threaded.
 */
struct ObservationStats {
    std::array<ObservationChannelStats, static_cast<size_t>(ObservationChannel::NumChannels)>
        channels{};

    template <ObservationChannel Ch>
    void recordEmit() noexcept {
        ++channels[static_cast<size_t>(Ch)].emitted;
    }

    template <ObservationChannel Ch>
    void recordDrop() noexcept {
        ++channels[static_cast<size_t>(Ch)].dropped;
    }

    template <ObservationChannel Ch>
    const ObservationChannelStats& get() const noexcept {
        return channels[static_cast<size_t>(Ch)];
    }

    const ObservationChannelStats& get(ObservationChannel ch) const noexcept {
        return channels[static_cast<size_t>(ch)];
    }

    uint64_t totalEmitted() const noexcept {
        uint64_t total = 0;
        for (const auto& ch : channels) {
            total += ch.emitted;
        }
        return total;
    }

    uint64_t totalDropped() const noexcept {
        uint64_t total = 0;
        for (const auto& ch : channels) {
            total += ch.dropped;
        }
        return total;
    }

    static const char* channelName(ObservationChannel ch) noexcept {
        switch (ch) {
            case ObservationChannel::Trace:
                return "trace";
            case ObservationChannel::Debug:
                return "debug";
            case ObservationChannel::Info:
                return "info";
            case ObservationChannel::Warn:
                return "warn";
            case ObservationChannel::Error:
                return "error";
            default:
                return "unknown";
        }
    }
};

using ObserveChannel [[deprecated("Use ObservationChannel")]] = ObservationChannel;
using ObserveChannelStats [[deprecated("Use ObservationChannelStats")]] = ObservationChannelStats;
using ObserveStats [[deprecated("Use ObservationStats")]] = ObservationStats;

/**
 * @brief Producer behavior when SPSC queues are full.
 *
 * - Drop:        Drop immediately when queue full (zero stall, data loss).
 * - SpinWait:    Spin indefinitely until space available (zero data loss, may stall).
 * - BoundedWait: Spin up to max_spins iterations, then drop (recommended default).
 */
enum class BackpressurePolicy : uint8_t {
    Drop,
    SpinWait,
    BoundedWait,
};

#if defined(__GNUC__) || defined(__clang__)
#define OBSERVE_LIKELY(x) __builtin_expect(!!(x), 1)
#define OBSERVE_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define OBSERVE_LIKELY(x) (x)
#define OBSERVE_UNLIKELY(x) (x)
#endif

}  // namespace chronon::observe
