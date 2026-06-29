// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// Registered queue tests. Chronon ports model clocked edge buffers; the
// scheduler uses single-thread storage, SPSC rings, or topology-stable MPSC
// staging.

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "sender/port/MessageQueue.hpp"

using namespace chronon::sender;

void require(bool condition) {
    if (!condition) {
        throw std::runtime_error("registered queue test assertion failed");
    }
}

void test_basic_push_pop() {
    std::cout << "Testing basic push/pop... ";

    SingleThreadMessageQueue<int> queue;

    queue.push(1, 10);
    queue.push(2, 15);
    queue.push(3, 10);

    require(!queue.tryPop(5).has_value());
    require(!queue.tryPop(9).has_value());

    auto val1 = queue.tryPop(10);
    require(val1.has_value());
    require(*val1 == 1);

    auto val2 = queue.tryPop(10);
    require(val2.has_value());
    require(*val2 == 3);

    require(!queue.tryPop(14).has_value());

    auto val3 = queue.tryPop(15);
    require(val3.has_value());
    require(*val3 == 2);

    require(!queue.tryPop(100).has_value());
    require(queue.empty());

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
    require(all.size() == 3);
    require(all[0] == 1);
    require(all[1] == 2);
    require(all[2] == 4);

    require(queue.size() == 1);

    auto remaining = queue.popAll(20);
    require(remaining.size() == 1);
    require(remaining[0] == 3);
    require(queue.empty());

    std::cout << "PASSED\n";
}

void test_has_ready() {
    std::cout << "Testing hasReady... ";

    SingleThreadMessageQueue<int> queue;

    require(!queue.hasReady(10));

    queue.push(42, 15);
    require(!queue.hasReady(10));
    require(!queue.hasReady(14));
    require(queue.hasReady(15));
    require(queue.hasReady(20));

    std::cout << "PASSED\n";
}

void test_min_arrival_cycle() {
    std::cout << "Testing minArrivalCycle... ";

    SingleThreadMessageQueue<int> queue;

    require(!queue.minArrivalCycle().has_value());

    queue.push(1, 20);
    require(queue.minArrivalCycle().value() == 20);

    queue.push(2, 10);
    require(queue.minArrivalCycle().value() == 10);

    queue.push(3, 15);
    require(queue.minArrivalCycle().value() == 10);

    queue.tryPop(10);
    require(queue.minArrivalCycle().value() == 15);

    std::cout << "PASSED\n";
}

void test_clear() {
    std::cout << "Testing clear... ";

    SingleThreadMessageQueue<int> queue;

    queue.push(1, 10);
    queue.push(2, 20);
    queue.push(3, 30);

    require(queue.size() == 3);
    require(!queue.empty());

    queue.clear();

    require(queue.size() == 0);
    require(queue.empty());
    require(!queue.tryPop(100).has_value());

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
    require(msg1.has_value());
    require(msg1->id == 1);
    require(msg1->name == "first");
    require(msg1->data.size() == 3);

    std::cout << "PASSED\n";
}

void test_capacity_tracking() {
    std::cout << "Testing capacity tracking... ";

    SingleThreadMessageQueue<int> queue(2);
    require(queue.capacity() == 2);
    require(queue.available() == 2);
    require(!queue.full());

    queue.push(1, 0);
    queue.push(2, 0);
    require(queue.full());
    require(queue.available() == 0);

    queue.setCapacity(3);
    require(queue.capacity() == 3);
    require(queue.available() == 1);

    std::cout << "PASSED\n";
}

void test_lock_free_queue_basic() {
    std::cout << "Testing LockFreeMessageQueue basic... ";

    LockFreeMessageQueue<int> queue;

    require(queue.empty());

    require(queue.tryPush(1, 10));
    require(queue.tryPush(2, 15));
    require(queue.tryPush(3, 10));

    require(!queue.empty());
    require(queue.size() == 3);
    require(!queue.tryPop(5).has_value());

    auto val1 = queue.tryPop(10);
    require(val1.has_value());
    require(*val1 == 1);

    auto val2 = queue.tryPop(10);
    require(!val2.has_value());

    val2 = queue.tryPop(15);
    require(val2.has_value());
    require(*val2 == 2);

    auto val3 = queue.tryPop(15);
    require(val3.has_value());
    require(*val3 == 3);

    require(queue.empty());

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
