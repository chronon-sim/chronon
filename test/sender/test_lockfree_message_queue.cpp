// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Focused robustness suite for LockFreeMessageQueue (SPSC ring). These tests
// exercise the producer/consumer index-cache fast paths (cached_head_ /
// cached_tail_): full detection, wraparound, empty<->non-empty oscillation,
// the clear()-refreshes-cache invariant, payload move semantics, and a
// real-thread single-producer/single-consumer stress run. Assertions use a
// CHECK macro that fires regardless of NDEBUG (unlike assert), so the suite is
// meaningful in optimized/sanitizer builds too.

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>

#include "sender/port/MessageQueue.hpp"

using chronon::sender::LockFreeMessageQueue;

namespace {

int g_failures = 0;

#define CHECK(cond)                                                                        \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            std::cerr << "\n  CHECK failed: " #cond " at " << __FILE__ << ":" << __LINE__; \
            ++g_failures;                                                                  \
        }                                                                                  \
    } while (0)

// Runs a test function and reports PASSED/FAILED based on how many CHECKs fired
// during it, so per-test status is accurate (not unconditionally "PASSED").
#define RUN_TEST(fn)                            \
    do {                                        \
        std::cout << "Testing " #fn "... ";     \
        int before = g_failures;                \
        fn();                                   \
        if (g_failures == before) {             \
            std::cout << "PASSED\n";            \
        } else {                                \
            std::cout << "\nFAILED: " #fn "\n"; \
        }                                       \
    } while (0)

constexpr size_t kUsable = LockFreeMessageQueue<int>::USABLE_CAPACITY;

// 1. FIFO ordering and arrive_cycle gating, plus head-peek queries.
void test_fifo_and_arrive_cycle_gating() {
    LockFreeMessageQueue<int> q;

    CHECK(q.empty());
    CHECK(q.tryPush(10, 5));
    CHECK(q.tryPush(20, 5));
    CHECK(q.tryPush(30, 7));
    CHECK(!q.empty());
    CHECK(q.size() == 3);

    // Head queries reflect the oldest element (FIFO, not cycle-sorted).
    CHECK(q.minArrivalCycle().has_value() && q.minArrivalCycle().value() == 5);
    auto head = q.peekHead();
    CHECK(head.has_value() && head->first == 5 && head->second == 0);

    // Not ready before the head's arrive_cycle.
    CHECK(!q.tryPop(4).has_value());

    auto a = q.tryPop(5);
    CHECK(a.has_value() && *a == 10);
    auto b = q.tryPop(5);
    CHECK(b.has_value() && *b == 20);

    // Next head arrives at cycle 7 -> still gated at cycle 5.
    CHECK(!q.tryPop(5).has_value());
    auto c = q.tryPop(7);
    CHECK(c.has_value() && *c == 30);

    CHECK(q.empty());
    CHECK(!q.tryPop(1000).has_value());
}

// 2. Fill exactly to capacity; verify rejection and recovery after a pop.
//    Exercises the cached_head_ reload-on-full path.
void test_fill_to_capacity() {
    LockFreeMessageQueue<int> q;

    for (size_t i = 0; i < kUsable; ++i) {
        CHECK(q.tryPush(static_cast<int>(i), 0));
    }
    CHECK(q.full());
    CHECK(q.size() == kUsable);
    CHECK(q.available() == 0);
    CHECK(!q.tryPush(999999, 0));  // rejected: ring full

    // Free one slot and confirm a single push now succeeds (and only one).
    auto v = q.tryPop(0);
    CHECK(v.has_value() && *v == 0);
    CHECK(!q.full());
    CHECK(q.available() == 1);
    CHECK(q.tryPush(123456, 0));
    CHECK(q.full());
    CHECK(!q.tryPush(654321, 0));
}

void test_custom_capacity() {
    constexpr size_t kUserCapacity = 8192;
    LockFreeMessageQueue<int> q{
        LockFreeMessageQueue<int>::physicalCapacityForUserCapacity(kUserCapacity)};

    CHECK(q.usableCapacity() >= kUserCapacity);
    for (size_t i = 0; i < kUserCapacity; ++i) {
        CHECK(q.tryPush(static_cast<int>(i), 0));
    }
    CHECK(q.size() == kUserCapacity);
    for (size_t i = 0; i < kUserCapacity; ++i) {
        auto value = q.tryPop(0);
        CHECK(value.has_value() && *value == static_cast<int>(i));
    }
    CHECK(q.empty());
}

// 3. Push/pop far more than CAPACITY total so head/tail wrap many times.
//    Verifies exact FIFO payload sequence across wraps.
void test_wraparound_integrity() {
    LockFreeMessageQueue<int> q;

    const int N = 100000;  // >> CAPACITY (4096) -> dozens of wraps
    int next_push = 0;
    int next_pop = 0;
    while (next_pop < N) {
        while (next_push < N && q.tryPush(next_push, 0)) {
            ++next_push;
        }
        while (next_pop < next_push) {
            auto v = q.tryPop(0);
            if (!v.has_value()) break;
            CHECK(*v == next_pop);
            ++next_pop;
        }
    }
    CHECK(next_pop == N);
    CHECK(q.empty());
}

// 4. Repeated {push 1, pop 1} drives the ring empty every iteration, forcing a
//    cached_tail_ reload each pop. Also wraps (iterations >> CAPACITY).
void test_empty_nonempty_oscillation() {
    LockFreeMessageQueue<int> q;

    const int N = 200000;
    for (int i = 0; i < N; ++i) {
        CHECK(q.tryPush(i, 0));
        CHECK(!q.empty());
        auto v = q.tryPop(0);
        CHECK(v.has_value() && *v == i);
        CHECK(q.empty());
    }
}

// 5. Regression test for the clear()-must-refresh-cached_tail_ invariant.
//    Arranges cached_tail_ to lag the real tail, then clears: a stale cache
//    would make the next tryPop read the unwritten tail slot.
void test_clear_refreshes_cache() {
    LockFreeMessageQueue<int> q;

    // push 5, pop 5 -> head==tail==5, cached_tail_ has been set to 5.
    for (int i = 0; i < 5; ++i) CHECK(q.tryPush(i, 0));
    for (int i = 0; i < 5; ++i) {
        auto v = q.tryPop(0);
        CHECK(v.has_value() && *v == i);
    }
    CHECK(q.empty());

    // push 3 more WITHOUT popping -> tail advances to 8, cached_tail_ stays 5
    // (stale, lagging the real tail).
    for (int i = 100; i < 103; ++i) CHECK(q.tryPush(i, 0));
    CHECK(q.size() == 3);

    q.clear();

    // With a correctly refreshed cache the ring is empty; with a stale cache
    // the next tryPop would treat buffer_[8] (never written) as a message.
    CHECK(q.empty());
    CHECK(q.size() == 0);
    CHECK(!q.tryPop(0).has_value());

    // Fresh traffic must still be delivered correctly.
    CHECK(q.tryPush(777, 0));
    auto v = q.tryPop(0);
    CHECK(v.has_value() && *v == 777);
    CHECK(q.empty());
}

// 6. sender_id is carried through and visible via peekHead.
void test_sender_id_propagation() {
    LockFreeMessageQueue<int> q;

    CHECK(q.tryPush(42, 9, /*sender_id=*/7));
    CHECK(q.tryPush(43, 9, /*sender_id=*/3));
    auto head = q.peekHead();
    CHECK(head.has_value() && head->first == 9 && head->second == 7);
    CHECK(q.minArrivalCycle().value() == 9);

    auto a = q.tryPop(9);
    CHECK(a.has_value() && *a == 42);
    auto head2 = q.peekHead();
    CHECK(head2.has_value() && head2->first == 9 && head2->second == 3);
}

// 7. Move-only payload: confirms the buffer_[i] = {std::move(data), ...} path
//    moves rather than copies, and tryPop moves out intact.
void test_move_only_payload() {
    LockFreeMessageQueue<std::unique_ptr<int>> q;

    CHECK(q.tryPush(std::make_unique<int>(123), 0));
    CHECK(q.tryPush(std::make_unique<int>(456), 1));
    CHECK(q.size() == 2);

    auto a = q.tryPop(0);
    CHECK(a.has_value() && *a && **a == 123);
    auto b = q.tryPop(1);
    CHECK(b.has_value() && *b && **b == 456);
    CHECK(q.empty());
}

// Real-thread single-producer/single-consumer stress. All entries carry
// arrive_cycle 0 (always ready) and a strictly increasing payload, so the
// consumer can assert no loss, no duplication, and no reordering — the core
// race-freedom check for the index caches.
void run_spsc_stress(uint64_t n) {
    LockFreeMessageQueue<uint64_t> q;
    std::atomic<bool> order_ok{true};
    uint64_t received = 0;

    std::thread consumer([&]() {
        uint64_t expected = 0;
        while (expected < n) {
            auto v = q.tryPop(0);
            if (v.has_value()) {
                if (*v != expected) order_ok.store(false, std::memory_order_relaxed);
                ++expected;
            } else {
                std::this_thread::yield();
            }
        }
        received = expected;
    });

    for (uint64_t i = 0; i < n;) {
        if (q.tryPush(i, 0)) {
            ++i;
        } else {
            std::this_thread::yield();
        }
    }
    consumer.join();

    CHECK(order_ok.load(std::memory_order_relaxed));
    CHECK(received == n);
    CHECK(q.empty());
}

// 8. A few sizes and repeats to shake out timing-dependent races; 1M items
//    forces ~244 ring wraps per run. Total runtime is well under the timeout.
void test_concurrent_spsc_stress() {
    run_spsc_stress(1000);
    run_spsc_stress(100000);
    for (int rep = 0; rep < 3; ++rep) {
        run_spsc_stress(1000000);
    }
}

}  // namespace

int main() {
    std::cout << "=== LockFreeMessageQueue Tests ===\n\n";

    RUN_TEST(test_fifo_and_arrive_cycle_gating);
    RUN_TEST(test_fill_to_capacity);
    RUN_TEST(test_custom_capacity);
    RUN_TEST(test_wraparound_integrity);
    RUN_TEST(test_empty_nonempty_oscillation);
    RUN_TEST(test_clear_refreshes_cache);
    RUN_TEST(test_sender_id_propagation);
    RUN_TEST(test_move_only_payload);
    RUN_TEST(test_concurrent_spsc_stress);

    if (g_failures == 0) {
        std::cout << "\n=== All LockFreeMessageQueue tests PASSED ===\n";
        return 0;
    }
    std::cerr << "\n=== " << g_failures << " LockFreeMessageQueue check(s) FAILED ===\n";
    return 1;
}
