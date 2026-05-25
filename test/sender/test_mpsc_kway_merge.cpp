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
// (arrive_cycle, queue_id) order, regardless of push order or which
// queue holds which message. Replaces the previous round-robin policy
// which produced delivery orders that depended on pop history.

#include <cassert>
#include <iostream>
#include <string>
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

// ----------------------------------------------------------------------
// Test 1: strict cycle-order delivery across producers.
// ----------------------------------------------------------------------
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

// ----------------------------------------------------------------------
// Test 2: same-cycle tie-break by queue_id.
// ----------------------------------------------------------------------
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

// ----------------------------------------------------------------------
// Test 3: run-to-run stability over 1000 iterations.
// ----------------------------------------------------------------------
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

// ----------------------------------------------------------------------
// Test 4: partial readiness — only ready messages delivered.
// ----------------------------------------------------------------------
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
    assert(min.has_value());
    EXPECT_EQ(*min, uint64_t{105});

    // Not deliverable until current_cycle catches up.
    EXPECT_EQ(q.hasReady(104), false);
    EXPECT_EQ(q.hasReady(105), true);

    auto rest = q.popAll(/*current_cycle=*/200);
    EXPECT_EQ(rest.size(), size_t{1});
    EXPECT_EQ(rest[0], 2);
    EXPECT_EQ(q.empty(), true);
}

}  // namespace

int main() {
    RUN_TEST(test_strict_cycle_order);
    RUN_TEST(test_same_cycle_tiebreak_by_queue_id);
    RUN_TEST(test_run_to_run_stability);
    RUN_TEST(test_partial_readiness);

    std::cout << "\n[==========] " << test_passed << "/" << test_count << " tests passed\n";
    return (test_passed == test_count) ? 0 : 1;
}
