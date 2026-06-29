// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// Registered queue tests. Chronon ports model clocked edge buffers; the
// scheduler uses single-thread storage, SPSC rings, or topology-stable MPSC
// staging.

#include <iostream>
#include <string>
#include <vector>

#include "../TestAssertions.hpp"
#include "sender/port/MessageQueue.hpp"

using namespace chronon::sender;

void test_basic_push_pop() {
    std::cout << "Testing basic push/pop... ";

    SingleThreadMessageQueue<int> queue;

    queue.push(1, 10);
    queue.push(2, 15);
    queue.push(3, 10);

    CHECK(!queue.tryPop(5).has_value());
    CHECK(!queue.tryPop(9).has_value());

    auto val1 = queue.tryPop(10);
    CHECK(val1.has_value());
    CHECK(*val1 == 1);

    auto val2 = queue.tryPop(10);
    CHECK(val2.has_value());
    CHECK(*val2 == 3);

    CHECK(!queue.tryPop(14).has_value());

    auto val3 = queue.tryPop(15);
    CHECK(val3.has_value());
    CHECK(*val3 == 2);

    CHECK(!queue.tryPop(100).has_value());
    CHECK(queue.empty());

    std::cout << "PASSED\n";
}

void test_pop_all() {
    std::cout << "Testing popAll... ";

    SingleThreadMessageQueue<int> queue;

    queue.push(1, 10);
    queue.push(2, 10);
    queue.push(3, 15);
    queue.push(4, 10);

    auto all = queue.popAll(10);
    CHECK(all.size() == 3);
    CHECK(all[0] == 1);
    CHECK(all[1] == 2);
    CHECK(all[2] == 4);

    CHECK(queue.size() == 1);

    auto remaining = queue.popAll(20);
    CHECK(remaining.size() == 1);
    CHECK(remaining[0] == 3);
    CHECK(queue.empty());

    std::cout << "PASSED\n";
}

void test_has_ready() {
    std::cout << "Testing hasReady... ";

    SingleThreadMessageQueue<int> queue;

    CHECK(!queue.hasReady(10));

    queue.push(42, 15);
    CHECK(!queue.hasReady(10));
    CHECK(!queue.hasReady(14));
    CHECK(queue.hasReady(15));
    CHECK(queue.hasReady(20));

    std::cout << "PASSED\n";
}

void test_min_arrival_cycle() {
    std::cout << "Testing minArrivalCycle... ";

    SingleThreadMessageQueue<int> queue;

    CHECK(!queue.minArrivalCycle().has_value());

    queue.push(1, 20);
    CHECK(queue.minArrivalCycle().value() == 20);

    queue.push(2, 10);
    CHECK(queue.minArrivalCycle().value() == 10);

    queue.push(3, 15);
    CHECK(queue.minArrivalCycle().value() == 10);

    queue.tryPop(10);
    CHECK(queue.minArrivalCycle().value() == 15);

    std::cout << "PASSED\n";
}

void test_clear() {
    std::cout << "Testing clear... ";

    SingleThreadMessageQueue<int> queue;

    queue.push(1, 10);
    queue.push(2, 20);
    queue.push(3, 30);

    CHECK(queue.size() == 3);
    CHECK(!queue.empty());

    queue.clear();

    CHECK(queue.size() == 0);
    CHECK(queue.empty());
    CHECK(!queue.tryPop(100).has_value());

    std::cout << "PASSED\n";
}

void test_complex_types() {
    std::cout << "Testing complex types... ";

    struct ComplexMessage {
        int id;
        std::string name;
        std::vector<int> data;
    };

    SingleThreadMessageQueue<ComplexMessage> queue;

    queue.push({1, "first", {1, 2, 3}}, 10);
    queue.push({2, "second", {4, 5}}, 15);

    auto msg1 = queue.tryPop(10);
    CHECK(msg1.has_value());
    CHECK(msg1->id == 1);
    CHECK(msg1->name == "first");
    CHECK(msg1->data.size() == 3);

    std::cout << "PASSED\n";
}

void test_capacity_tracking() {
    std::cout << "Testing capacity tracking... ";

    SingleThreadMessageQueue<int> queue(2);
    CHECK(queue.capacity() == 2);
    CHECK(queue.available() == 2);
    CHECK(!queue.full());

    queue.push(1, 0);
    queue.push(2, 0);
    CHECK(queue.full());
    CHECK(queue.available() == 0);

    queue.setCapacity(3);
    CHECK(queue.capacity() == 3);
    CHECK(queue.available() == 1);

    std::cout << "PASSED\n";
}

void test_lock_free_queue_basic() {
    std::cout << "Testing LockFreeMessageQueue basic... ";

    LockFreeMessageQueue<int> queue;

    CHECK(queue.empty());

    CHECK(queue.tryPush(1, 10));
    CHECK(queue.tryPush(2, 15));
    CHECK(queue.tryPush(3, 10));

    CHECK(!queue.empty());
    CHECK(queue.size() == 3);
    CHECK(!queue.tryPop(5).has_value());

    auto val1 = queue.tryPop(10);
    CHECK(val1.has_value());
    CHECK(*val1 == 1);

    auto val2 = queue.tryPop(10);
    CHECK(!val2.has_value());

    val2 = queue.tryPop(15);
    CHECK(val2.has_value());
    CHECK(*val2 == 2);

    auto val3 = queue.tryPop(15);
    CHECK(val3.has_value());
    CHECK(*val3 == 3);

    CHECK(queue.empty());

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== Registered Queue Tests ===\n\n";

    test_basic_push_pop();
    test_pop_all();
    test_has_ready();
    test_min_arrival_cycle();
    test_clear();
    test_complex_types();
    test_capacity_tracking();
    test_lock_free_queue_basic();

    std::cout << "\nAll registered queue tests PASSED!\n";
    return 0;
}
