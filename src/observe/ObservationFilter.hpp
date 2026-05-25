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
#include <unordered_map>
#include <vector>

#include "Types.hpp"

namespace chronon::observe {

struct TemporalFilter;

/**
 * @brief Observes for `window` cycles every `period` cycles, starting at `offset`.
 *
 * Example: window=1000, period=10000 observes cycles 0-999, 10000-10999, etc.
 */
struct PeriodicFilter {
    uint64_t window;
    uint64_t period;
    uint64_t offset;

    /// `period == 0` is treated as "always match" rather than divide-by-zero.
    [[nodiscard]] bool matches(uint64_t cycle) const noexcept {
        if (period == 0) return true;
        if (cycle < offset) return false;
        uint64_t pos_in_period = (cycle - offset) % period;
        return pos_in_period < window;
    }
};

/**
 * @brief Fast O(1) bitmask-based filtering for observability events.
 *
 * Hot path is a single atomic load + AND. Safe for concurrent reads; configuration
 * changes should happen before simulation start or during quiescent periods.
 */
class ObservationFilter {
public:
    using CycleRange = std::pair<uint64_t, uint64_t>;

    ObservationFilter() = default;

    /// Hot path: ~2ns.
    [[gnu::always_inline]] bool shouldObserve(CategoryMask category_mask) const noexcept {
        return (enabled_categories_.load(std::memory_order_relaxed) & category_mask) != 0;
    }

    /// Category check plus global cycle-range filtering.
    [[gnu::always_inline]] bool shouldObserve(CategoryMask category_mask,
                                              uint64_t cycle) const noexcept {
        if (!shouldObserve(category_mask)) {
            return false;
        }
        if (OBSERVE_LIKELY(!has_cycle_filter_.load(std::memory_order_relaxed))) {
            return true;
        }
        return isCycleInRange(cycle);
    }

    /// First-check guard so callers can skip all observation logic when nothing is enabled.
    [[gnu::always_inline]] bool anyEnabled() const noexcept {
        return enabled_categories_.load(std::memory_order_relaxed) != 0;
    }

    void enableCategory(CategoryMask mask) noexcept {
        enabled_categories_.fetch_or(mask, std::memory_order_relaxed);
    }

    void disableCategory(CategoryMask mask) noexcept {
        enabled_categories_.fetch_and(~mask, std::memory_order_relaxed);
    }

    void setEnabledCategories(CategoryMask mask) noexcept {
        enabled_categories_.store(mask, std::memory_order_relaxed);
    }

    CategoryMask getEnabledCategories() const noexcept {
        return enabled_categories_.load(std::memory_order_relaxed);
    }

    /// @param start Inclusive. @param end Inclusive.
    void addCycleRange(uint64_t start, uint64_t end) {
        cycle_ranges_.emplace_back(start, end);
        std::sort(cycle_ranges_.begin(), cycle_ranges_.end());
        has_cycle_filter_.store(true, std::memory_order_relaxed);
    }

    void clearCycleRanges() {
        cycle_ranges_.clear();
        updateHasCycleFilter();
    }

    void addPeriodicFilter(uint64_t window, uint64_t period, uint64_t offset = 0) {
        periodic_filters_.push_back({window, period, offset});
        updateHasCycleFilter();
    }

    void clearPeriodicFilters() {
        periodic_filters_.clear();
        updateHasCycleFilter();
    }

    /** @brief Temporal filters scoped to a specific category. */
    struct CategoryTemporalConfig {
        std::vector<CycleRange> ranges;
        std::vector<PeriodicFilter> periodic;

        /// OR semantics across all filters. No filters means "always active".
        [[nodiscard]] bool matchesCycle(uint64_t cycle) const noexcept {
            if (ranges.empty() && periodic.empty()) {
                return true;
            }

            for (const auto& range : ranges) {
                if (cycle >= range.first && cycle <= range.second) {
                    return true;
                }
            }

            for (const auto& pf : periodic) {
                if (pf.matches(cycle)) {
                    return true;
                }
            }

            return false;
        }
    };

    void setCategoryTemporalConfig(CategoryMask category_mask,
                                   const CategoryTemporalConfig& config) {
        if (config.ranges.empty() && config.periodic.empty()) {
            category_temporal_configs_.erase(category_mask);
        } else {
            category_temporal_configs_[category_mask] = config;
            has_category_temporal_filter_.store(true, std::memory_order_relaxed);
        }
    }

    void addCategoryRange(CategoryMask category_mask, uint64_t start, uint64_t end) {
        category_temporal_configs_[category_mask].ranges.emplace_back(start, end);
        has_category_temporal_filter_.store(true, std::memory_order_relaxed);
    }

    void addCategoryPeriodicFilter(CategoryMask category_mask, uint64_t window, uint64_t period,
                                   uint64_t offset = 0) {
        category_temporal_configs_[category_mask].periodic.push_back({window, period, offset});
        has_category_temporal_filter_.store(true, std::memory_order_relaxed);
    }

    void clearCategoryTemporalConfig(CategoryMask category_mask) {
        category_temporal_configs_.erase(category_mask);
        if (category_temporal_configs_.empty()) {
            has_category_temporal_filter_.store(false, std::memory_order_relaxed);
        }
    }

    void clearAllCategoryTemporalConfigs() {
        category_temporal_configs_.clear();
        has_category_temporal_filter_.store(false, std::memory_order_relaxed);
    }

    /**
     * @brief Category check plus global and per-category temporal filtering.
     *
     * Prefer this over shouldObserve() when per-category temporal configs are in use.
     */
    [[gnu::always_inline]] bool shouldObserveCategory(CategoryMask category_mask,
                                                      uint64_t cycle) const noexcept {
        if (!shouldObserve(category_mask)) {
            return false;
        }

        if (has_cycle_filter_.load(std::memory_order_relaxed)) {
            if (!isCycleActive(cycle)) {
                return false;
            }
        }

        if (OBSERVE_UNLIKELY(has_category_temporal_filter_.load(std::memory_order_relaxed))) {
            return checkCategoryTemporal(category_mask, cycle);
        }

        return true;
    }

    void setMinLogLevel(LogLevel level) {
        CategoryMask log_mask = 0;
        if (level <= LogLevel::Debug) log_mask |= category::LOG_DEBUG;
        if (level <= LogLevel::Info) log_mask |= category::LOG_INFO;
        if (level <= LogLevel::Warn) log_mask |= category::LOG_WARN;
        if (level <= LogLevel::Error) log_mask |= category::LOG_ERROR;

        disableCategory(category::ALL_LOGS);
        enableCategory(log_mask);
    }

    void reset() {
        enabled_categories_.store(0, std::memory_order_relaxed);
        cycle_ranges_.clear();
        periodic_filters_.clear();
        category_temporal_configs_.clear();
        has_cycle_filter_.store(false, std::memory_order_relaxed);
        has_category_temporal_filter_.store(false, std::memory_order_relaxed);
    }

private:
    /// O(log N) via binary search over sorted ranges.
    bool isCycleInRange(uint64_t cycle) const {
        auto it =
            std::lower_bound(cycle_ranges_.begin(), cycle_ranges_.end(), cycle,
                             [](const CycleRange& range, uint64_t c) { return range.second < c; });

        if (it == cycle_ranges_.end()) {
            return false;
        }
        return cycle >= it->first && cycle <= it->second;
    }

    /// OR semantics across ranges and periodic filters.
    bool isCycleActive(uint64_t cycle) const {
        if (isCycleInRange(cycle)) {
            return true;
        }

        for (const auto& pf : periodic_filters_) {
            if (pf.matches(cycle)) {
                return true;
            }
        }

        return cycle_ranges_.empty() && periodic_filters_.empty();
    }

    bool checkCategoryTemporal(CategoryMask category_mask, uint64_t cycle) const {
        for (const auto& [mask, config] : category_temporal_configs_) {
            if ((category_mask & mask) != 0) {
                if (!config.matchesCycle(cycle)) {
                    return false;
                }
            }
        }
        return true;
    }

    void updateHasCycleFilter() {
        bool has_filter = !cycle_ranges_.empty() || !periodic_filters_.empty();
        has_cycle_filter_.store(has_filter, std::memory_order_relaxed);
    }

    std::atomic<CategoryMask> enabled_categories_{0};

    std::atomic<bool> has_cycle_filter_{false};
    std::vector<CycleRange> cycle_ranges_;
    std::vector<PeriodicFilter> periodic_filters_;

    std::atomic<bool> has_category_temporal_filter_{false};
    std::unordered_map<CategoryMask, CategoryTemporalConfig> category_temporal_configs_;
};

}  // namespace chronon::observe
