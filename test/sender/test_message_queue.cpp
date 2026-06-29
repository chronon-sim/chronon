// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// Registered queue tests. Chronon ports model clocked edge buffers; the
// scheduler uses single-thread storage, SPSC rings, or topology-stable MPSC
// staging.

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "sender/port/MessageQueue.hpp"

using namespace chronon::sender;

void test_basic_push_pop() {
    std::cout << "Testing basic push/pop... ";

    SingleThreadMessageQueue<int> queue;

    queue.push(1, 10);
    queue.push(2, 15);
    queue.push(3, 10);

    assert(!queue.tryPop(5).has_value());
    assert(!queue.tryPop(9).has_value());

    auto val1 = queue.tryPop(10);
    assert(val1.has_value());
    assert(*val1 == 1);

    auto val2 = queue.tryPop(10);
    assert(val2.has_value());
    assert(*val2 == 3);

    assert(!queue.tryPop(14).has_value());

    auto val3 = queue.tryPop(15);
    assert(val3.has_value());
    assert(*val3 == 2);

    assert(!queue.tryPop(100).has_value());
    assert(queue.empty());

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
    assert(all.size() == 3);
    assert(all[0] == 1);
    assert(all[1] == 2);
    assert(all[2] == 4);

    assert(queue.size() == 1);

    auto remaining = queue.popAll(20);
    assert(remaining.size() == 1);
    assert(remaining[0] == 3);
    assert(queue.empty());

    std::cout << "PASSED\n";
}

void test_has_ready() {
    std::cout << "Testing hasReady... ";

    SingleThreadMessageQueue<int> queue;

    assert(!queue.hasReady(10));

    queue.push(42, 15);
    assert(!queue.hasReady(10));
    assert(!queue.hasReady(14));
    assert(queue.hasReady(15));
    assert(queue.hasReady(20));

    std::cout << "PASSED\n";
}

void test_min_arrival_cycle() {
    std::cout << "Testing minArrivalCycle... ";

    SingleThreadMessageQueue<int> queue;

    assert(!queue.minArrivalCycle().has_value());

    queue.push(1, 20);
    assert(queue.minArrivalCycle().value() == 20);

    queue.push(2, 10);
    assert(queue.minArrivalCycle().value() == 10);

    queue.push(3, 15);
    assert(queue.minArrivalCycle().value() == 10);

    queue.tryPop(10);
    assert(queue.minArrivalCycle().value() == 15);

    std::cout << "PASSED\n";
}

void test_clear() {
    std::cout << "Testing clear... ";

    SingleThreadMessageQueue<int> queue;

    queue.push(1, 10);
    queue.push(2, 20);
    queue.push(3, 30);

    assert(queue.size() == 3);
    assert(!queue.empty());

    queue.clear();

    assert(queue.size() == 0);
    assert(queue.empty());
    assert(!queue.tryPop(100).has_value());

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
    assert(msg1.has_value());
    assert(msg1->id == 1);
    assert(msg1->name == "first");
    assert(msg1->data.size() == 3);

    std::cout << "PASSED\n";
}

void test_capacity_tracking() {
    std::cout << "Testing capacity tracking... ";

    SingleThreadMessageQueue<int> queue(2);
    assert(queue.capacity() == 2);
    assert(queue.available() == 2);
    assert(!queue.full());

    queue.push(1, 0);
    queue.push(2, 0);
    assert(queue.full());
    assert(queue.available() == 0);

    queue.setCapacity(3);
    assert(queue.capacity() == 3);
    assert(queue.available() == 1);

    std::cout << "PASSED\n";
}

void test_lock_free_queue_basic() {
    std::cout << "Testing LockFreeMessageQueue basic... ";

    LockFreeMessageQueue<int> queue;

    assert(queue.empty());

    assert(queue.tryPush(1, 10));
    assert(queue.tryPush(2, 15));
    assert(queue.tryPush(3, 10));

    assert(!queue.empty());
    assert(queue.size() == 3);
    assert(!queue.tryPop(5).has_value());

    auto val1 = queue.tryPop(10);
    assert(val1.has_value());
    assert(*val1 == 1);

    auto val2 = queue.tryPop(10);
    assert(!val2.has_value());

    val2 = queue.tryPop(15);
    assert(val2.has_value());
    assert(*val2 == 2);

    auto val3 = queue.tryPop(15);
    assert(val3.has_value());
    assert(*val3 == 3);

    assert(queue.empty());

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
