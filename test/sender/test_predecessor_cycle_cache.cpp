// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// Direct semantic coverage for the EpochFree worker-local predecessor cache.
// The scheduler integration is covered end to end by:
//   - sender_lookahead_floor_progress (progress and floor gating)
//   - sender_epoch_free_lookahead_determinism (fixed and dynamic drivers)

#include <atomic>
#include <cstdint>
#include <iostream>
#include <string>

#include "sender/core/TickSimulation.hpp"

namespace chronon::sender {

struct CacheObservations {
    uint64_t zero_requirement;
    uint64_t first_remote_refresh;
    uint64_t cached_hit;
    uint64_t second_remote_refresh;
    uint64_t exact_boundary_hit;
    uint64_t third_remote_refresh;
    uint64_t other_predecessor_refresh;
    uint64_t other_predecessor_hit;
    uint64_t original_predecessor_unchanged;
    uint64_t floor_refresh;
    uint64_t floor_hit;
    uint64_t floor_second_refresh;
};

// Befriended by TickSimulation so the optimization's hot-path helper can be
// tested deterministically without adding production instrumentation or counters.
struct PredecessorCycleCacheTestAccess {
    static CacheObservations exercise() {
        TickSimulation::WorkerPredecessorCycleCache cache(/*num_clusters=*/2);
        uint64_t* const cycles = cache.data();

        std::atomic<uint64_t> pred0{10};
        std::atomic<uint64_t> pred1{40};
        std::atomic<uint64_t> floor{7};
        ResolvedDep dep0{&pred0, /*min_delay=*/1, /*pred_id=*/0};
        ResolvedDep dep1{&pred1, /*min_delay=*/3, /*pred_id=*/1};
        ResolvedDep floor_dep{&floor, /*min_delay=*/64, /*pred_id=*/2};

        CacheObservations result{};
        // A zero requirement is proven by the cache's initial lower bound and
        // must not pull the remote value into the local shadow.
        result.zero_requirement = TickSimulation::observePredecessorCycle_(dep0, 0, cycles);
        result.first_remote_refresh = TickSimulation::observePredecessorCycle_(dep0, 5, cycles);

        pred0.store(20, std::memory_order_release);
        result.cached_hit = TickSimulation::observePredecessorCycle_(dep0, 8, cycles);
        result.second_remote_refresh = TickSimulation::observePredecessorCycle_(dep0, 11, cycles);

        pred0.store(30, std::memory_order_release);
        result.exact_boundary_hit = TickSimulation::observePredecessorCycle_(dep0, 20, cycles);
        result.third_remote_refresh = TickSimulation::observePredecessorCycle_(dep0, 21, cycles);

        result.other_predecessor_refresh =
            TickSimulation::observePredecessorCycle_(dep1, 30, cycles);
        pred1.store(50, std::memory_order_release);
        result.other_predecessor_hit = TickSimulation::observePredecessorCycle_(dep1, 39, cycles);
        result.original_predecessor_unchanged =
            TickSimulation::observePredecessorCycle_(dep0, 29, cycles);

        result.floor_refresh = TickSimulation::observePredecessorCycle_(floor_dep, 5, cycles);
        floor.store(9, std::memory_order_release);
        result.floor_hit = TickSimulation::observePredecessorCycle_(floor_dep, 6, cycles);
        result.floor_second_refresh =
            TickSimulation::observePredecessorCycle_(floor_dep, 8, cycles);
        return result;
    }
};

}  // namespace chronon::sender

namespace {

int failures = 0;

void check(bool condition, const std::string& message) {
    if (condition) {
        std::cout << "  [PASS] " << message << "\n";
        return;
    }
    std::cerr << "  [FAIL] " << message << "\n";
    ++failures;
}

}  // namespace

int main() {
    using chronon::sender::PredecessorCycleCacheTestAccess;
    const auto result = PredecessorCycleCacheTestAccess::exercise();

    check(result.zero_requirement == 0, "zero requirement uses the initial local lower bound");
    check(result.first_remote_refresh == 10, "cold insufficient slot acquires remote progress");
    check(result.cached_hit == 10, "sufficient slot returns stale lower bound, not newer remote");
    check(result.second_remote_refresh == 20, "insufficient slot refreshes remote progress");
    check(result.exact_boundary_hit == 20, "equal cached value satisfies the requirement");
    check(result.third_remote_refresh == 30, "higher requirement refreshes the slot again");
    check(result.other_predecessor_refresh == 40, "second predecessor has an independent slot");
    check(result.other_predecessor_hit == 40, "second predecessor independently hits its cache");
    check(result.original_predecessor_unchanged == 30,
          "second predecessor updates do not alter the first slot");
    check(result.floor_refresh == 7, "synthetic floor uses its reserved cache slot");
    check(result.floor_hit == 7, "sufficient floor lower bound skips the newer floor value");
    check(result.floor_second_refresh == 9, "insufficient floor slot refreshes independently");

    std::cout << "\n" << (failures == 0 ? "ALL PASSED" : "FAILED") << "\n";
    return failures == 0 ? 0 : 1;
}
