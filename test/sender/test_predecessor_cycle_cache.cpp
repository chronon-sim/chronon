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

struct MaskedObservations {
    bool first_ready;
    bool second_ready;
    uint64_t hit_lane_cache;
    uint64_t ready_miss_lane_cache;
    uint64_t blocked_miss_lane_cache;
    size_t blocked_predecessor;
    uint64_t blocked_deficit;
};

struct ShortCircuitObservations {
    bool ready;
    uint64_t first_ready_lane_cache;
    uint64_t first_blocked_lane_cache;
    uint64_t later_blocked_lane_cache;
    uint64_t later_ready_lane_cache;
    size_t blocked_predecessor;
    uint64_t blocked_deficit;
    size_t sampled_blocked_predecessor;
    uint64_t sampled_blocked_deficit;
    uint64_t sampled_later_blocked_lane_cache;
    uint64_t sampled_later_ready_lane_cache;
};

// Befriended by TickSimulation so the optimization's hot-path helper can be
// tested deterministically without adding production instrumentation or counters.
struct PredecessorCycleCacheTestAccess {
    static CacheObservations exercise() {
        TickSimulation::WorkerPredecessorCycleCache cache(/*num_clusters=*/2);
        auto* const cycles = cache.data();

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

    static MaskedObservations exerciseMaskedClusterCheck() {
        TickSimulationConfig config;
        config.num_threads = 1;
        TickSimulation sim(config);
        sim.thread_progress_count_ = 4;
        sim.thread_resolved_deps_.resize(1);

        std::atomic<uint64_t> pred0{100};
        std::atomic<uint64_t> pred1{9};
        std::atomic<uint64_t> pred2{6};
        std::atomic<uint64_t> pred3{100};
        sim.thread_resolved_deps_[0] = {
            {&pred0, /*min_delay=*/1, /*pred_id=*/0},
            {&pred1, /*min_delay=*/2, /*pred_id=*/1},
            {&pred2, /*min_delay=*/3, /*pred_id=*/2},
            {&pred3, /*min_delay=*/4, /*pred_id=*/3},
        };

        TickSimulation::WorkerPredecessorCycleCache cache(/*num_clusters=*/4);
        auto* const cycles = cache.data();
        cycles[0] = 10;  // Satisfies needed=10; must not refresh remote 100.
        cycles[3] = 7;   // Four dependencies keep this test on the mask path.

        BlockedClusterInfo first_blocker{};
        MaskedObservations result{};
        result.first_ready = sim.clusterCanAdvance_(0, 10, first_blocker, cycles);
        result.hit_lane_cache = cycles[0];
        result.ready_miss_lane_cache = cycles[1];
        result.blocked_miss_lane_cache = cycles[2];
        result.blocked_predecessor = first_blocker.pred_cluster;
        result.blocked_deficit = first_blocker.deficit;

        pred2.store(8, std::memory_order_release);
        BlockedClusterInfo second_blocker{};
        result.second_ready = sim.clusterCanAdvance_(0, 10, second_blocker, cycles);
        return result;
    }

    static ShortCircuitObservations exerciseFirstBlockerShortCircuit(bool short_circuit) {
        TickSimulationConfig config;
        config.num_threads = 1;
        TickSimulation sim(config);
        sim.thread_progress_count_ = 4;
        sim.thread_resolved_deps_.resize(1);

        std::atomic<uint64_t> pred0{10};
        std::atomic<uint64_t> pred1{7};
        std::atomic<uint64_t> pred2{4};
        std::atomic<uint64_t> pred3{10};
        sim.thread_resolved_deps_[0] = {
            {&pred0, /*min_delay=*/1, /*pred_id=*/0},
            {&pred1, /*min_delay=*/2, /*pred_id=*/1},
            {&pred2, /*min_delay=*/3, /*pred_id=*/2},
            {&pred3, /*min_delay=*/4, /*pred_id=*/3},
        };

        TickSimulation::WorkerPredecessorCycleCache cache(/*num_clusters=*/4);
        auto* const cycles = cache.data();
        BlockedClusterInfo blocker{};
        ShortCircuitObservations result{};
        result.ready = sim.clusterCanAdvance_(0, 10, blocker, cycles, short_circuit);
        result.first_ready_lane_cache = cycles[0];
        result.first_blocked_lane_cache = cycles[1];
        result.later_blocked_lane_cache = cycles[2];
        result.later_ready_lane_cache = cycles[3];
        result.blocked_predecessor = blocker.pred_cluster;
        result.blocked_deficit = blocker.deficit;
        if (short_circuit) {
            // Dynamic scheduling uses the short scan for ordinary readiness
            // checks, then restores a full scan only when a blocked-wait
            // sample will feed rebalance scoring.
            BlockedClusterInfo sampled_blocker{};
            result.ready = sim.clusterCanAdvance_(0, 10, sampled_blocker, cycles,
                                                  /*stop_on_first_blocker=*/false);
            result.sampled_blocked_predecessor = sampled_blocker.pred_cluster;
            result.sampled_blocked_deficit = sampled_blocker.deficit;
            result.sampled_later_blocked_lane_cache = cycles[2];
            result.sampled_later_ready_lane_cache = cycles[3];
        }
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

    const auto masked = PredecessorCycleCacheTestAccess::exerciseMaskedClusterCheck();
    check(!masked.first_ready, "masked check reports an insufficient miss lane");
    check(masked.hit_lane_cache == 10, "masked hit lane does not acquire newer remote progress");
    check(masked.ready_miss_lane_cache == 9, "masked ready miss lane refreshes its cache slot");
    check(masked.blocked_miss_lane_cache == 6, "masked blocked miss lane refreshes its cache slot");
    check(masked.blocked_predecessor == 2, "masked slow lanes preserve blocker identity");
    check(masked.blocked_deficit == 2, "masked slow lanes preserve blocker deficit");
    check(masked.second_ready, "masked check advances after the blocked lane catches up");

    const auto full_scan = PredecessorCycleCacheTestAccess::exerciseFirstBlockerShortCircuit(false);
    check(!full_scan.ready, "full miss scan reports blocked");
    check(full_scan.first_ready_lane_cache == 10 && full_scan.first_blocked_lane_cache == 7,
          "full miss scan refreshes lanes through the first blocker");
    check(full_scan.later_blocked_lane_cache == 4 && full_scan.later_ready_lane_cache == 10,
          "full miss scan refreshes lanes after the first blocker");
    check(full_scan.blocked_predecessor == 2 && full_scan.blocked_deficit == 4,
          "full miss scan retains the maximum-deficit blocker");

    const auto short_scan = PredecessorCycleCacheTestAccess::exerciseFirstBlockerShortCircuit(true);
    check(!short_scan.ready, "first-blocker short circuit reports blocked");
    check(short_scan.first_ready_lane_cache == 10 && short_scan.first_blocked_lane_cache == 7,
          "first-blocker short circuit refreshes through its proof lane");
    check(short_scan.later_blocked_lane_cache == 0 && short_scan.later_ready_lane_cache == 0,
          "first-blocker short circuit skips later remote loads");
    check(short_scan.blocked_predecessor == 1 && short_scan.blocked_deficit == 2,
          "first-blocker short circuit records its exact proof lane");
    check(short_scan.sampled_later_blocked_lane_cache == 4 &&
              short_scan.sampled_later_ready_lane_cache == 10,
          "sampled dynamic wait refinement refreshes the skipped lanes");
    check(short_scan.sampled_blocked_predecessor == 2 && short_scan.sampled_blocked_deficit == 4,
          "sampled dynamic wait refinement restores the maximum-deficit blocker");

    std::cout << "\n" << (failures == 0 ? "ALL PASSED" : "FAILED") << "\n";
    return failures == 0 ? 0 : 1;
}
