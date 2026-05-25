// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "Types.hpp"

namespace chronon::observe {

class ObservationContext;

/**
 * @brief Lightweight single-threaded counter (~1-2ns increment).
 *
 * Plain uint64_t with epoch support for speculative-execution rollback.
 * Primary counter type for single-threaded tick-based simulation.
 */
class SimpleCounter {
public:
    SimpleCounter() = default;

    [[gnu::always_inline]] void increment(uint64_t delta = 1) noexcept { value_ += delta; }

    [[gnu::always_inline]] uint64_t get() const noexcept { return value_; }

    /// Commit current value as the epoch baseline.
    void commitEpoch() noexcept { epoch_base_ = value_; }

    /// Rollback to epoch baseline.
    void rollbackEpoch() noexcept { value_ = epoch_base_; }

    /// Increment since last commit.
    uint64_t epochDelta() const noexcept { return value_ - epoch_base_; }

    void reset() noexcept {
        value_ = 0;
        epoch_base_ = 0;
    }

    void set(uint64_t value) noexcept { value_ = value; }

private:
    uint64_t value_ = 0;
    uint64_t epoch_base_ = 0;
};

/**
 * @brief Sparse array of SimpleCounters for per-unit storage.
 *
 * O(1) unchecked access via getUnchecked(). Memory cost is num_counters × 16 bytes.
 */
class FixedCounterStorage {
public:
    explicit FixedCounterStorage(std::string name);

    /**
     * @brief Hot-path access by counter id (~1-2ns).
     *
     * PRECONDITION: Counter must have been added via addCounter(); no bounds checking.
     */
    [[gnu::always_inline]] SimpleCounter& getUnchecked(CounterId id) noexcept {
        return counters_[toIndex(id)];
    }

    [[gnu::always_inline]] const SimpleCounter& getUnchecked(CounterId id) const noexcept {
        return counters_[toIndex(id)];
    }

    /// Bounds-checked access; grows storage if needed.
    SimpleCounter& get(CounterId id) {
        size_t idx = toIndex(id);
        ensureCapacity(idx + 1);
        return counters_[idx];
    }

    const SimpleCounter& get(CounterId id) const {
        size_t idx = toIndex(id);
        if (idx < counters_.size()) {
            return counters_[idx];
        }
        static SimpleCounter default_counter;
        return default_counter;
    }

    std::vector<SimpleCounter>& counters() noexcept { return counters_; }
    const std::vector<SimpleCounter>& counters() const noexcept { return counters_; }

    void ensureCapacity(size_t n) {
        if (counters_.size() < n) {
            counters_.resize(n);
            counter_info_.resize(n);
        }
    }

    /**
     * @brief Register a counter dynamically.
     * @return Counter ID assigned to this counter.
     */
    CounterId addCounter(const std::string& name, const std::string& description = "",
                         const std::string& unit = "") {
        size_t idx = counter_info_.size();
        CounterId id = makeCounterId(static_cast<uint32_t>(idx));
        counter_info_.push_back({name, description, unit});
        if (counters_.size() <= idx) {
            counters_.emplace_back();
        }
        return id;
    }

    const CounterInfo& info(CounterId id) const {
        size_t idx = toIndex(id);
        if (idx < counter_info_.size()) {
            return counter_info_[idx];
        }
        static CounterInfo empty_info{"", "", ""};
        return empty_info;
    }

    template <typename Fn>
    void forEach(Fn&& fn) const {
        for (size_t i = 0; i < counters_.size(); ++i) {
            fn(makeCounterId(static_cast<uint32_t>(i)),
               i < counter_info_.size() ? counter_info_[i] : CounterInfo{"", "", ""},
               counters_[i].get());
        }
    }

    size_t size() const noexcept { return counters_.size(); }

    const std::string& name() const noexcept { return name_; }

    void commitAllEpochs() {
        for (auto& counter : counters_) {
            counter.commitEpoch();
        }
    }

    void rollbackAllEpochs() {
        for (auto& counter : counters_) {
            counter.rollbackEpoch();
        }
    }

    void resetAll() {
        for (auto& counter : counters_) {
            counter.reset();
        }
    }

private:
    std::string name_;
    std::vector<SimpleCounter> counters_;
    std::vector<CounterInfo> counter_info_;
};

inline FixedCounterStorage::FixedCounterStorage(std::string name) : name_(std::move(name)) {}

}  // namespace chronon::observe
