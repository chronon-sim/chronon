// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_mpsc_kway_merge.cpp
//
// Verifies that MultiProducerQueueAdapter delivers messages in strict
// (arrive_cycle, sender_id, lane_id) order, regardless of push order or which
// lane holds each message. Replaces the previous round-robin policy which
// produced delivery orders that depended on pop history.

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "sender/port/MessageQueue.hpp"

using chronon::sender::MultiProducerQueueAdapter;

namespace {

int test_count = 0;
int test_passed = 0;

#define RUN_TEST(name)                                                               \
    do {                                                                             \
        ++test_count;                                                                \
        std::cout << "[ RUN      ] " << #name << std::endl;                          \
        try {                                                                        \
            name();                                                                  \
            ++test_passed;                                                           \
            std::cout << "[       OK ] " << #name << std::endl;                      \
        } catch (const std::exception& ex) {                                         \
            std::cout << "[  FAILED  ] " << #name << ": " << ex.what() << std::endl; \
        }                                                                            \
    } while (0)

#define EXPECT_EQ(a, b)                                                                           \
    do {                                                                                          \
        auto _a = (a);                                                                            \
        auto _b = (b);                                                                            \
        if (!(_a == _b)) {                                                                        \
            std::cerr << "Assertion failed at " << __FILE__ << ":" << __LINE__                    \
                      << ": " #a " == " #b << " (got " << _a << " vs " << _b << ")" << std::endl; \
            throw std::runtime_error("EXPECT_EQ failed");                                         \
        }                                                                                         \
    } while (0)

#define EXPECT_TRUE(condition)                                                                    \
    do {                                                                                          \
        if (!(condition)) {                                                                       \
            std::cerr << "Assertion failed at " << __FILE__ << ":" << __LINE__ << ": " #condition \
                      << std::endl;                                                               \
            throw std::runtime_error("EXPECT_TRUE failed");                                       \
        }                                                                                         \
    } while (0)

// Build the canonical 3-thread setup used by tests 1 and 3.
//   q0: (100, 1), (103, 4)
//   q1: (101, 2), (102, 3)
//   q2: (104, 5)
void seedCanonical(MultiProducerQueueAdapter<int>& q, size_t q0, size_t q1, size_t q2) {
    // Do NOT use assert() — NDEBUG strips the call entirely in Release,
    // and no messages would be pushed.
    EXPECT_EQ(q.pushFromThread(q0, 1, 100), true);
    EXPECT_EQ(q.pushFromThread(q0, 4, 103), true);
    EXPECT_EQ(q.pushFromThread(q1, 2, 101), true);
    EXPECT_EQ(q.pushFromThread(q1, 3, 102), true);
    EXPECT_EQ(q.pushFromThread(q2, 5, 104), true);
}

// Test 1: strict cycle-order delivery across producers.
void test_strict_cycle_order() {
    MultiProducerQueueAdapter<int> q;
    size_t q0 = q.addProducerThread(/*thread_id=*/10);
    size_t q1 = q.addProducerThread(/*thread_id=*/20);
    size_t q2 = q.addProducerThread(/*thread_id=*/30);
    EXPECT_EQ(q0, size_t{0});
    EXPECT_EQ(q1, size_t{1});
    EXPECT_EQ(q2, size_t{2});

    seedCanonical(q, q0, q1, q2);

    auto out = q.popAll(/*current_cycle=*/110);
    std::vector<int> expected{1, 2, 3, 4, 5};
    EXPECT_EQ(out.size(), expected.size());
    for (size_t i = 0; i < out.size(); ++i) {
        EXPECT_EQ(out[i], expected[i]);
    }
}

// Test 2: same-cycle tie-break by queue_id.
void test_same_cycle_tiebreak_by_queue_id() {
    MultiProducerQueueAdapter<char> q;
    size_t q0 = q.addProducerThread(7);
    size_t q1 = q.addProducerThread(8);
    size_t q2 = q.addProducerThread(9);

    // Push in non-queue-id order to ensure ordering is not
    // determined by push order.
    EXPECT_EQ(q.pushFromThread(q2, 'c', 100), true);
    EXPECT_EQ(q.pushFromThread(q0, 'a', 100), true);
    EXPECT_EQ(q.pushFromThread(q1, 'b', 100), true);

    auto out = q.popAll(/*current_cycle=*/100);
    std::vector<char> expected{'a', 'b', 'c'};
    EXPECT_EQ(out.size(), expected.size());
    for (size_t i = 0; i < out.size(); ++i) {
        EXPECT_EQ(out[i], expected[i]);
    }
}

// Test 3: run-to-run stability over 1000 iterations.
void test_run_to_run_stability() {
    std::vector<int> golden;
    for (int iter = 0; iter < 1000; ++iter) {
        MultiProducerQueueAdapter<int> q;
        size_t q0 = q.addProducerThread(10);
        size_t q1 = q.addProducerThread(20);
        size_t q2 = q.addProducerThread(30);
        seedCanonical(q, q0, q1, q2);
        auto out = q.popAll(/*current_cycle=*/110);
        if (iter == 0) {
            golden = out;
            EXPECT_EQ(golden.size(), size_t{5});
        } else {
            EXPECT_EQ(out.size(), golden.size());
            for (size_t i = 0; i < out.size(); ++i) {
                EXPECT_EQ(out[i], golden[i]);
            }
        }
    }
}

// Test 4: partial readiness — only ready messages delivered.
void test_partial_readiness() {
    MultiProducerQueueAdapter<int> q;
    size_t q0 = q.addProducerThread(1);
    size_t q1 = q.addProducerThread(2);
    size_t q2 = q.addProducerThread(3);

    EXPECT_EQ(q.pushFromThread(q0, 1, 100), true);
    EXPECT_EQ(q.pushFromThread(q1, 2, 105), true);
    EXPECT_EQ(q.pushFromThread(q2, 3, 101), true);

    auto out = q.popAll(/*current_cycle=*/102);
    std::vector<int> expected{1, 3};  // (100,1) then (101,3); (105,2) stays
    EXPECT_EQ(out.size(), expected.size());
    for (size_t i = 0; i < out.size(); ++i) {
        EXPECT_EQ(out[i], expected[i]);
    }

    // The remaining (105, 2) is still there.
    EXPECT_EQ(q.size(), size_t{1});
    auto min = q.minArrivalCycle();
    EXPECT_EQ(min.has_value(), true);
    EXPECT_EQ(*min, uint64_t{105});

    // Not deliverable until current_cycle catches up.
    EXPECT_EQ(q.hasReady(104), false);
    EXPECT_EQ(q.hasReady(105), true);

    auto rest = q.popAll(/*current_cycle=*/200);
    EXPECT_EQ(rest.size(), size_t{1});
    EXPECT_EQ(rest[0], 2);
    EXPECT_EQ(q.empty(), true);
}

// Large, sparse fan-in takes the active-lane frontier path.  The active lanes
// are deliberately far apart so a word/lane scan bug cannot masquerade as a
// correct dense-prefix implementation.
void test_sparse_high_fanin_frontier_order() {
    constexpr size_t kLanes = 256;
    MultiProducerQueueAdapter<int> q;
    std::vector<size_t> lanes;
    lanes.reserve(kLanes);
    for (size_t i = 0; i < kLanes; ++i) {
        lanes.push_back(q.addProducerThread(1000 + i));
    }
    EXPECT_EQ(q.usesActiveFrontier(), true);

    EXPECT_EQ(q.pushFromThread(lanes[255], 4, 103, 255), true);
    EXPECT_EQ(q.pushFromThread(lanes[191], 2, 101, 191), true);
    EXPECT_EQ(q.pushFromThread(lanes[3], 1, 100, 3), true);
    EXPECT_EQ(q.pushFromThread(lanes[64], 3, 101, 64), true);

    const auto first_arrival = q.minArrivalCycle();
    EXPECT_EQ(first_arrival.has_value(), true);
    EXPECT_EQ(*first_arrival, uint64_t{100});
    EXPECT_EQ(q.hasReady(99), false);
    EXPECT_EQ(q.hasReady(100), true);

    const auto out = q.popAll(103);
    const std::vector<int> expected{1, 3, 2, 4};
    EXPECT_EQ(out.size(), expected.size());
    for (size_t i = 0; i < out.size(); ++i) EXPECT_EQ(out[i], expected[i]);
    EXPECT_EQ(q.empty(), true);
    EXPECT_EQ(q.minArrivalCycle().has_value(), false);
}

// A producer may append while its lane already has a frontier node.  Repeated
// notifications are coalesced, and the consumer must reinsert the lane after
// each pop until it is truly empty.
void test_frontier_requeues_hot_lane() {
    MultiProducerQueueAdapter<int> q;
    std::vector<size_t> lanes;
    for (size_t i = 0; i < 64; ++i) lanes.push_back(q.addProducerThread(i + 1));

    EXPECT_EQ(q.pushFromThread(lanes[63], 10, 10, 63), true);
    EXPECT_EQ(q.pushFromThread(lanes[63], 30, 30, 63), true);
    EXPECT_EQ(q.pushFromThread(lanes[1], 20, 20, 1), true);

    const auto out = q.popAll(100);
    const std::vector<int> expected{10, 20, 30};
    EXPECT_EQ(out.size(), expected.size());
    for (size_t i = 0; i < out.size(); ++i) EXPECT_EQ(out[i], expected[i]);
}

// Crossing the small-fan-in threshold must not orphan messages that were
// published while the adapter was still using the linear scan.  Equal cycle
// and sender ids also exercise the final, stable lane-id tie breaker.
void test_scan_to_frontier_preserves_inflight_messages() {
    MultiProducerQueueAdapter<int> q;
    std::vector<size_t> lanes;
    for (size_t i = 0; i < MultiProducerQueueAdapter<int>::kFrontierLaneThreshold - 1; ++i) {
        lanes.push_back(q.addProducerThread(100 + i));
    }
    EXPECT_EQ(q.usesActiveFrontier(), false);

    EXPECT_EQ(q.pushFromThread(lanes[30], 30, 9, 5), true);
    EXPECT_EQ(q.pushFromThread(lanes[1], 1, 7, 7), true);
    EXPECT_EQ(q.pushFromThread(lanes[0], 0, 7, 7), true);

    lanes.push_back(q.addProducerThread(131));
    EXPECT_EQ(q.usesActiveFrontier(), true);
    EXPECT_EQ(q.pushFromThread(lanes[31], 31, 7, 6), true);
    EXPECT_EQ(q.hasReady(6), false);
    EXPECT_TRUE(q.minArrivalCycle() == std::optional<uint64_t>{7});

    const auto ready = q.popAll(8);
    const std::vector<int> expected_ready{31, 0, 1};
    EXPECT_EQ(ready.size(), expected_ready.size());
    for (size_t i = 0; i < ready.size(); ++i) EXPECT_EQ(ready[i], expected_ready[i]);

    EXPECT_EQ(q.tryPop(8).has_value(), false);
    EXPECT_TRUE(q.tryPop(9) == std::optional<int>{30});
    EXPECT_EQ(q.empty(), true);
}

void test_registration_and_invalid_lane_contracts() {
    MultiProducerQueueAdapter<int> q(/*capacity=*/4, /*min_per_thread_usable_capacity=*/2);
    EXPECT_EQ(q.full(), true);
    EXPECT_EQ(q.push(1, 0), false);
    EXPECT_EQ(q.pushFromThread(SIZE_MAX, 1, 0), false);
    EXPECT_EQ(q.fullForThread(SIZE_MAX), true);
    EXPECT_EQ(q.storageFullForThread(SIZE_MAX), true);
    EXPECT_EQ(q.admissionOccupancyForThread(SIZE_MAX, 0), size_t{0});
    EXPECT_EQ(q.admissionMinArrivalCycleForThread(SIZE_MAX, 0).has_value(), false);
    EXPECT_EQ(q.getQueueIdForThread(999), SIZE_MAX);

    const size_t lane = q.addProducerThread(17, false);
    EXPECT_EQ(q.addProducerThread(17, true), lane);
    EXPECT_EQ(q.getQueueIdForThread(17), lane);
    EXPECT_EQ(q.full(), false);
    EXPECT_EQ(q.capacity(), size_t{4});
    EXPECT_EQ(q.available(), size_t{4});

    // The generic IMessageQueue path is defined to use lane zero.
    EXPECT_EQ(q.push(9, 5), true);
    EXPECT_EQ(q.available(), size_t{3});
    EXPECT_EQ(q.tryPop(4).has_value(), false);
    EXPECT_TRUE(q.tryPop(5) == std::optional<int>{9});

    // Upgrading a duplicate registration enables the simulated-cycle
    // admission ledger on the existing lane.
    EXPECT_EQ(q.admissionOccupancyForThread(lane, 5), size_t{1});
    EXPECT_TRUE(q.admissionMinArrivalCycleForThread(lane, 5) == std::optional<uint64_t>{5});
    EXPECT_EQ(q.admissionOccupancyForThread(lane, 6), size_t{0});
    EXPECT_EQ(q.admissionMinArrivalCycleForThread(lane, 6).has_value(), false);
}

void test_capacity_growth_and_physical_overflow_accounting() {
    MultiProducerQueueAdapter<int> q(std::numeric_limits<size_t>::max(), 2);
    const size_t lane = q.addProducerThread(1, false);
    EXPECT_EQ(q.storageCapacity(), size_t{2});
    EXPECT_EQ(q.pushFromThread(lane, 1, 1), true);
    EXPECT_EQ(q.pushFromThread(lane, 2, 2), true);
    EXPECT_EQ(q.pushFromThread(lane, 3, 3), false);
    EXPECT_EQ(q.transportOverflowEvents(), uint64_t{1});

    bool nonempty_growth_rejected = false;
    try {
        q.ensurePerThreadUsableCapacity(8);
    } catch (const std::length_error&) {
        nonempty_growth_rejected = true;
    }
    EXPECT_EQ(nonempty_growth_rejected, true);

    EXPECT_TRUE(q.tryPop(1) == std::optional<int>{1});
    EXPECT_EQ(q.pushFromThread(lane, 3, 3), true);
    EXPECT_TRUE(q.popAll(10) == std::vector<int>({2, 3}));

    q.ensurePerThreadUsableCapacity(8);
    EXPECT_EQ(q.storageCapacity() >= size_t{8}, true);
    EXPECT_EQ(q.getQueueIdForThread(1), lane);
    EXPECT_EQ(q.pushFromThread(lane, 4, 4), true);
    EXPECT_TRUE(q.tryPop(4) == std::optional<int>{4});

    bool impossible_growth_rejected = false;
    try {
        q.ensurePerThreadUsableCapacity(std::numeric_limits<size_t>::max());
    } catch (const std::length_error&) {
        impossible_growth_rejected = true;
    }
    EXPECT_EQ(impossible_growth_rejected, true);
}

void test_set_capacity_upgrades_existing_unbounded_lanes() {
    MultiProducerQueueAdapter<int> q(std::numeric_limits<size_t>::max(), 2);
    const size_t lane = q.addProducerThread(44, false);

    q.setCapacity(3);
    EXPECT_EQ(q.capacity(), size_t{3});
    EXPECT_EQ(q.storageCapacity() >= size_t{3}, true);
    EXPECT_EQ(q.available(), size_t{3});

    EXPECT_EQ(q.pushFromThread(lane, 7, 10), true);
    EXPECT_TRUE(q.tryPop(10) == std::optional<int>{7});
    EXPECT_EQ(q.admissionOccupancyForThread(lane, 10), size_t{1});
    EXPECT_EQ(q.admissionOccupancyForThread(lane, 11), size_t{0});
}

void exercise_clear_and_reuse(size_t lane_count) {
    MultiProducerQueueAdapter<int> q(/*capacity=*/8, /*min_per_thread_usable_capacity=*/8);
    std::vector<size_t> lanes;
    for (size_t i = 0; i < lane_count; ++i) {
        lanes.push_back(q.addProducerThread(1000 + i));
    }

    EXPECT_EQ(q.pushFromThread(lanes.back(), 3, 30, 3), true);
    EXPECT_EQ(q.pushFromThread(lanes.front(), 1, 10, 1), true);
    EXPECT_EQ(q.pushFromThread(lanes.front(), 2, 20, 1), true);
    EXPECT_TRUE(q.minArrivalCycle() == std::optional<uint64_t>{10});

    q.clear();
    EXPECT_EQ(q.empty(), true);
    EXPECT_EQ(q.size(), size_t{0});
    EXPECT_EQ(q.minArrivalCycle().has_value(), false);
    EXPECT_EQ(q.hasReady(std::numeric_limits<uint64_t>::max()), false);
    EXPECT_EQ(q.admissionOccupancyForThread(lanes.front(), 31), size_t{0});

    EXPECT_EQ(q.getQueueIdForThread(1000), lanes.front());
    EXPECT_EQ(q.pushFromThread(lanes.back(), 5, 41, 5), true);
    EXPECT_EQ(q.pushFromThread(lanes.front(), 4, 40, 4), true);
    EXPECT_TRUE(q.popAll(100) == std::vector<int>({4, 5}));
    EXPECT_EQ(q.empty(), true);
}

void test_clear_resets_scan_and_frontier_state() {
    exercise_clear_and_reuse(3);
    exercise_clear_and_reuse(MultiProducerQueueAdapter<int>::kFrontierLaneThreshold);
}

// Concurrent producers exercise notification-word races while the sole
// consumer continuously empties the frontier. Ordering between unpublished
// lanes is scheduler-owned, so this test checks exactly-once transport rather
// than a global order that a standalone MPSC container cannot promise.
void test_concurrent_frontier_notification_stress() {
    constexpr size_t kLanes = 64;
    constexpr size_t kProducerThreads = 4;
    constexpr size_t kMessagesPerLane = 2000;
    constexpr size_t kTotal = kLanes * kMessagesPerLane;

    MultiProducerQueueAdapter<uint64_t> q(std::numeric_limits<size_t>::max(), 64);
    std::vector<size_t> lanes;
    for (size_t lane = 0; lane < kLanes; ++lane) {
        lanes.push_back(q.addProducerThread(lane + 1));
    }

    std::atomic<bool> start{false};
    std::atomic<bool> abort{false};
    std::atomic<bool> producer_failed{false};
    std::vector<std::thread> producers;
    for (size_t worker = 0; worker < kProducerThreads; ++worker) {
        producers.emplace_back([&, worker] {
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            for (size_t lane = worker; lane < kLanes; lane += kProducerThreads) {
                for (size_t seq = 0; seq < kMessagesPerLane; ++seq) {
                    const uint64_t payload = lane * kMessagesPerLane + seq;
                    while (
                        !q.pushFromThread(lanes[lane], payload, seq, static_cast<uint32_t>(lane))) {
                        if (abort.load(std::memory_order_acquire)) {
                            producer_failed.store(true, std::memory_order_release);
                            return;
                        }
                        std::this_thread::yield();
                    }
                }
            }
        });
    }

    std::vector<bool> seen(kTotal, false);
    size_t received = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    start.store(true, std::memory_order_release);
    while (received < kTotal && std::chrono::steady_clock::now() < deadline) {
        auto value = q.tryPop(std::numeric_limits<uint64_t>::max());
        if (!value) {
            std::this_thread::yield();
            continue;
        }
        if (*value >= kTotal || seen[*value]) {
            producer_failed.store(true, std::memory_order_release);
            break;
        }
        seen[*value] = true;
        ++received;
    }

    abort.store(true, std::memory_order_release);
    for (auto& producer : producers) producer.join();

    EXPECT_EQ(producer_failed.load(std::memory_order_acquire), false);
    EXPECT_EQ(received, kTotal);
    for (bool delivered : seen) EXPECT_EQ(delivered, true);
    EXPECT_EQ(q.empty(), true);
}

}  // namespace

int main() {
    RUN_TEST(test_strict_cycle_order);
    RUN_TEST(test_same_cycle_tiebreak_by_queue_id);
    RUN_TEST(test_run_to_run_stability);
    RUN_TEST(test_partial_readiness);
    RUN_TEST(test_sparse_high_fanin_frontier_order);
    RUN_TEST(test_frontier_requeues_hot_lane);
    RUN_TEST(test_scan_to_frontier_preserves_inflight_messages);
    RUN_TEST(test_registration_and_invalid_lane_contracts);
    RUN_TEST(test_capacity_growth_and_physical_overflow_accounting);
    RUN_TEST(test_set_capacity_upgrades_existing_unbounded_lanes);
    RUN_TEST(test_clear_resets_scan_and_frontier_state);
    RUN_TEST(test_concurrent_frontier_notification_stress);

    std::cout << "\n[==========] " << test_passed << "/" << test_count << " tests passed\n";
    return (test_passed == test_count) ? 0 : 1;
}
