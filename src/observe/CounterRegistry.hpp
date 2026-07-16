// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "Counter.hpp"
#include "CounterSnapshot.hpp"
#include "DerivedCounter.hpp"
#include "Types.hpp"

namespace chronon::observe {
class ObservationContext;
class ObservationQueue;
class ThreadContext;

class CounterRegistry {
public:
    static_assert(std::atomic<uint64_t>::is_always_lock_free,
                  "Periodic counter ownership requires lock-free uint64_t atomics");

    struct CounterKey {
        std::string unit_name;
        CounterId id;
        std::string counter_name;

        bool operator==(const CounterKey& other) const {
            return unit_name == other.unit_name && id == other.id;
        }
    };

    struct CounterKeyHash {
        size_t operator()(const CounterKey& key) const {
            return std::hash<std::string>{}(key.unit_name) ^
                   (std::hash<uint32_t>{}(toIndex(key.id)) << 1);
        }
    };

    void registerCounter(const std::string& unit_name, CounterId id, SimpleCounter* counter_ptr,
                         const std::string& counter_name = "") {
        CounterKey key{unit_name, id, counter_name};
        counters_[key] = counter_ptr;
    }

    void reregisterAll(const std::vector<std::unique_ptr<ObservationContext>>& contexts);

    void dumpFinalSnapshot(uint64_t cycle, ObservationQueue* queue,
                           const std::vector<std::unique_ptr<ObservationContext>>& contexts);

    /**
     * Push and reset counters owned by the supplied scheduler clusters.
     *
     * The caller must be the sole writer for every owner in @p owner_ids.
     * Metadata and record sizes are precomputed by reregisterAll(), so this
     * path performs no allocation and writes only to the caller's SPSC queue.
     */
    bool pushOwnerSnapshots(uint64_t cycle, std::span<const size_t> owner_ids,
                            ThreadContext& thread_context) noexcept;

    /**
     * Return the next nominal periodic cycle for one stable scheduler owner.
     *
     * The returned state follows the owner across worker migration. No counter
     * storage is accessed, and the query is lock-free.
     */
    uint64_t nextOwnerSnapshotCycle(size_t owner_id, uint64_t run_start,
                                    uint64_t period) const noexcept;

    const std::vector<DerivedCounterDef>& derivedDefs() const noexcept { return derived_defs_; }
    const std::vector<CounterSnapshotPlanMetadata>& snapshotPlans() const noexcept {
        return snapshot_plan_metadata_;
    }
    const std::vector<CounterSnapshotEntryMetadata>& counterColumns() const noexcept {
        return counter_column_metadata_;
    }

    void clear() {
        counters_.clear();
        derived_defs_.clear();
        owner_snapshot_plans_.clear();
        snapshot_plan_metadata_.clear();
        counter_column_metadata_.clear();
    }

private:
    struct OwnerSnapshotEntry {
        std::string unit_name;
        std::string counter_name;
        SimpleCounter* counter = nullptr;
        const uint64_t* scalar = nullptr;

        uint64_t value() const noexcept { return counter ? counter->get() : *scalar; }
    };

    struct OwnerSnapshotPlan {
        struct Batch {
            uint32_t plan_id = 0;
            size_t entry_begin = 0;
            size_t entry_count = 0;
            size_t record_size = 0;
        };

        std::vector<OwnerSnapshotEntry> entries;
        std::vector<Batch> batches;
        size_t total_size = 0;
        // Lock-free migration deduplication; counter storage itself remains
        // single-writer under the scheduler's cluster execution claim.
        std::unique_ptr<std::atomic<uint64_t>> last_pushed_cycle =
            std::make_unique<std::atomic<uint64_t>>(UINT64_MAX);
    };

    void rebuildOwnerSnapshotPlans_(
        const std::vector<std::unique_ptr<ObservationContext>>& contexts);

    std::unordered_map<CounterKey, SimpleCounter*, CounterKeyHash> counters_;
    std::vector<DerivedCounterDef> derived_defs_;
    std::vector<OwnerSnapshotPlan> owner_snapshot_plans_;
    std::vector<CounterSnapshotPlanMetadata> snapshot_plan_metadata_;
    std::vector<CounterSnapshotEntryMetadata> counter_column_metadata_;
};

}  // namespace chronon::observe
