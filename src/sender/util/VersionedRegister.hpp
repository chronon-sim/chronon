// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>

namespace chronon::sender {

/**
 * @brief Lock-free ring buffer of (value, cycle) versions for lookahead-safe shared state.
 *
 * In parallel lookahead execution, units advance at different simulation cycles. A reader
 * at cycle N must not observe writes made at cycle N+K. write() appends; read(cycle) returns
 * the newest version with write_cycle <= reader_cycle. Single-writer/multi-reader safe via
 * acquire/release on write_head_; multiple writers require external serialization.
 */
template <typename T, size_t HistoryDepth = 16>
class VersionedRegister {
    static_assert(HistoryDepth >= 2, "HistoryDepth must be at least 2");
    static_assert(HistoryDepth <= 255, "HistoryDepth must fit in uint8_t index");

    struct Version {
        T value{};
        uint64_t cycle = 0;
    };

    std::array<Version, HistoryDepth> versions_{};
    std::atomic<uint8_t> write_head_{0};

public:
    VersionedRegister() = default;

    explicit VersionedRegister(T initial_value) { versions_[0] = {initial_value, 0}; }

    /// Caller must ensure write_cycle is non-decreasing across successive writes.
    void write(T value, uint64_t write_cycle) {
        uint8_t head = write_head_.load(std::memory_order_relaxed);
        uint8_t next = (head + 1) % HistoryDepth;
        versions_[next] = {value, write_cycle};
        write_head_.store(next, std::memory_order_release);
    }

    /// Returns the newest version with write_cycle <= reader_cycle, or oldest if skew exceeded.
    T read(uint64_t reader_cycle) const {
        uint8_t head = write_head_.load(std::memory_order_acquire);
        for (size_t i = 0; i < HistoryDepth; ++i) {
            auto idx = (head + HistoryDepth - i) % HistoryDepth;
            auto& v = versions_[idx];
            if (v.cycle <= reader_cycle) {
                return v.value;
            }
        }
        // Skew exceeded: return oldest as best effort rather than default-constructed T.
        return versions_[(head + 1) % HistoryDepth].value;
    }

    /// Unfiltered latest read — use only when caller is known to be at the newest cycle.
    T readLatest() const {
        uint8_t head = write_head_.load(std::memory_order_acquire);
        return versions_[head].value;
    }

    void reset(T value = T{}) {
        for (auto& v : versions_) {
            v = {value, 0};
        }
        write_head_.store(0, std::memory_order_relaxed);
    }
};

}  // namespace chronon::sender
