// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace chronon::sender {

/**
 * @brief Lock-free ring buffer of (value, cycle) versions for lookahead-safe shared state.
 *
 * In parallel lookahead execution, units advance at different simulation cycles. A reader
 * at cycle N must not observe writes made at cycle N+K. write() appends; read(cycle) returns
 * the newest version with write_cycle <= reader_cycle. Single-writer/multi-reader safe via
 * acquire/release on write_head_; multiple writers require external serialization.
 *
 * Preferred usage (runtime depth derived from the dependency graph):
 *
 *     auto depth = sim.dependencyGraph()
 *                      .requiredVersionedRegisterDepth(writer, readers, max_lookahead);
 *     VersionedRegister<uint64_t> reg(initial_value, depth);
 *
 * Legacy usage (compile-time depth, deprecated):
 *
 *     VersionedRegister<uint64_t, 16> reg(initial_value);   // deprecated
 *
 * @tparam T            Value type.
 * @tparam HistoryDepth Compile-time depth.  0 (default) selects the new runtime-depth API;
 *                       any positive value selects the deprecated fixed-depth API.
 */
template <typename T, size_t HistoryDepth = 0>
class VersionedRegister {
    struct Version {
        T value{};
        uint64_t cycle = 0;
    };

    static uint32_t checkedDepth(uint32_t depth) {
        if (depth < 2) {
            throw std::invalid_argument("VersionedRegister depth must be >= 2 (got " +
                                        std::to_string(depth) + ")");
        }
        return depth;
    }

    std::vector<Version> versions_;
    uint32_t depth_;
    std::atomic<uint32_t> write_head_{0};

public:
    static constexpr uint32_t kDefaultDepth = 16;

    // ------------------------------------------------------------------
    // Runtime-depth constructors (HistoryDepth == 0, preferred)
    //
    // Two forms:
    //   VersionedRegister<T> reg;              // default value, default depth
    //   VersionedRegister<T> reg(value, depth); // initial value, explicit depth
    //
    // The depth-only form is intentionally absent to avoid ambiguity
    // with the initial-value form when T is an integer type.
    // ------------------------------------------------------------------

    VersionedRegister()
        requires(HistoryDepth == 0)
        : versions_(kDefaultDepth), depth_(kDefaultDepth) {}

    VersionedRegister(T initial_value, uint32_t depth = kDefaultDepth)
        requires(HistoryDepth == 0)
        : versions_(checkedDepth(depth)), depth_(depth) {
        versions_[0] = {initial_value, 0};
    }

    // ------------------------------------------------------------------
    // Fixed-depth constructors (HistoryDepth > 0, deprecated)
    //
    // Migrate to VersionedRegister<T>(initial_value, depth) with depth
    // from DependencyGraph::requiredVersionedRegisterDepth().
    // ------------------------------------------------------------------

    [[deprecated(
        "Use VersionedRegister<T>(initial_value, depth) with runtime depth from "
        "DependencyGraph::requiredVersionedRegisterDepth()")]]
    VersionedRegister()
        requires(HistoryDepth > 0)
        : versions_(HistoryDepth), depth_(static_cast<uint32_t>(HistoryDepth)) {
        static_assert(HistoryDepth >= 2 && HistoryDepth <= 100000);
    }

    [[deprecated(
        "Use VersionedRegister<T>(initial_value, depth) with runtime depth from "
        "DependencyGraph::requiredVersionedRegisterDepth()")]]
    explicit VersionedRegister(T initial_value)
        requires(HistoryDepth > 0)
        : versions_(HistoryDepth), depth_(static_cast<uint32_t>(HistoryDepth)) {
        static_assert(HistoryDepth >= 2 && HistoryDepth <= 100000);
        versions_[0] = {initial_value, 0};
    }

    // ------------------------------------------------------------------
    // Copy / move (shared by both modes)
    // ------------------------------------------------------------------

    VersionedRegister(const VersionedRegister& other)
        : versions_(other.versions_),
          depth_(other.depth_),
          write_head_(other.write_head_.load(std::memory_order_relaxed)) {}

    VersionedRegister& operator=(const VersionedRegister& other) {
        if (this != &other) {
            versions_ = other.versions_;
            depth_ = other.depth_;
            write_head_.store(other.write_head_.load(std::memory_order_relaxed),
                              std::memory_order_relaxed);
        }
        return *this;
    }

    VersionedRegister(VersionedRegister&&) = default;
    VersionedRegister& operator=(VersionedRegister&&) = default;

    // ------------------------------------------------------------------
    // Core operations (shared by both modes)
    // ------------------------------------------------------------------

    void write(T value, uint64_t write_cycle) {
        uint32_t head = write_head_.load(std::memory_order_relaxed);
        uint32_t next = (head + 1) % depth_;
        versions_[next] = {value, write_cycle};
        write_head_.store(next, std::memory_order_release);
    }

    T read(uint64_t reader_cycle) const {
        uint32_t head = write_head_.load(std::memory_order_acquire);
        for (uint32_t i = 0; i < depth_; ++i) {
            auto idx = (head + depth_ - i) % depth_;
            auto& v = versions_[idx];
            if (v.cycle <= reader_cycle) {
                return v.value;
            }
        }
        return versions_[(head + 1) % depth_].value;
    }

    T readLatest() const {
        uint32_t head = write_head_.load(std::memory_order_acquire);
        return versions_[head].value;
    }

    void reset(T value = T{}) {
        for (auto& v : versions_) {
            v = {value, 0};
        }
        write_head_.store(0, std::memory_order_relaxed);
    }

    uint32_t depth() const { return depth_; }
};

}  // namespace chronon::sender
