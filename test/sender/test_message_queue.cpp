// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// Registered queue tests. Chronon ports model clocked edge buffers; the
// scheduler uses single-thread storage, SPSC rings, or topology-stable MPSC
// staging.

#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "../TestAssertions.hpp"
#include "sender/port/DelayOneCycleQueueAdapter.hpp"
#include "sender/port/MessageQueue.hpp"

using namespace chronon::sender;

namespace chronon::sender {

struct LockFreeQueueAdapterTestAccess {
    template <typename T>
    static size_t popCreditHistorySize(const LockFreeQueueAdapter<T>& queue) {
        std::lock_guard<std::mutex> lock(queue.admission_mutex_);
        return queue.pop_credits_.size();
    }

    template <typename T>
    static size_t popArrivalHistorySize(const LockFreeQueueAdapter<T>& queue) {
        std::lock_guard<std::mutex> lock(queue.admission_mutex_);
        return queue.pop_arrivals_.size();
    }
};

}  // namespace chronon::sender

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

void test_lock_free_adapter_retires_all_admission_histories() {
    std::cout << "Testing LockFreeQueueAdapter admission history retirement... ";

    LockFreeQueueAdapter<int> queue(1);

    CHECK(queue.push(1, 0));
    auto first = queue.tryPop(0);
    CHECK(first.has_value());
    CHECK(*first == 1);
    CHECK(LockFreeQueueAdapterTestAccess::popCreditHistorySize(queue) == 1);
    CHECK(LockFreeQueueAdapterTestAccess::popArrivalHistorySize(queue) == 1);

    CHECK(queue.admissionOccupancy(1) == 0);
    CHECK(LockFreeQueueAdapterTestAccess::popCreditHistorySize(queue) == 0);
    CHECK(LockFreeQueueAdapterTestAccess::popArrivalHistorySize(queue) == 0);

    CHECK(queue.push(2, 2));
    auto second = queue.tryPop(2);
    CHECK(second.has_value());
    CHECK(*second == 2);
    CHECK(LockFreeQueueAdapterTestAccess::popCreditHistorySize(queue) == 1);
    CHECK(LockFreeQueueAdapterTestAccess::popArrivalHistorySize(queue) == 1);

    CHECK(!queue.admissionMinArrivalCycle(3).has_value());
    CHECK(LockFreeQueueAdapterTestAccess::popCreditHistorySize(queue) == 0);
    CHECK(LockFreeQueueAdapterTestAccess::popArrivalHistorySize(queue) == 0);

    std::cout << "PASSED\n";
}

void test_direct_spsc_adapter_differential_semantics() {
    std::cout << "Testing DirectSPSCQueueAdapter differential semantics... ";

    for (const size_t capacity : {size_t{1}, size_t{2}, size_t{16}, size_t{4097}}) {
        LockFreeQueueAdapter<int> legacy(capacity);
        DirectSPSCQueueAdapter<int> direct(capacity);

        auto check_state = [&](uint64_t cycle) {
            CHECK(legacy.empty() == direct.empty());
            CHECK(legacy.full() == direct.full());
            CHECK(legacy.size() == direct.size());
            CHECK(legacy.capacity() == direct.capacity());
            CHECK(legacy.storageCapacity() == direct.storageCapacity());
            CHECK(legacy.available() == direct.available());
            CHECK(legacy.hasReady(cycle) == direct.hasReady(cycle));
            CHECK(legacy.minArrivalCycle() == direct.minArrivalCycle());
            CHECK(legacy.admissionOccupancy(cycle) == direct.admissionOccupancy(cycle));
            CHECK(legacy.admissionMinArrivalCycle(cycle) == direct.admissionMinArrivalCycle(cycle));
        };

        auto push_both = [&](int value, uint64_t arrive_cycle) {
            const bool legacy_ok = legacy.push(value, arrive_cycle);
            const bool direct_ok = direct.pushDirect(std::move(value), arrive_cycle);
            CHECK(legacy_ok == direct_ok);
        };

        auto pop_both = [&](uint64_t cycle) {
            auto legacy_value = legacy.tryPop(cycle);
            auto direct_value = direct.tryPop(cycle);
            CHECK(legacy_value == direct_value);
            return direct_value;
        };

        check_state(0);
        push_both(10, 2);
        push_both(20, 4);
        check_state(1);
        CHECK(!pop_both(1).has_value());

        auto first = pop_both(2);
        CHECK(first.has_value() && *first == 10);
        // A same-cycle pop is still architecturally occupied and contributes
        // its arrival to the registered-edge admission floor.
        CHECK(direct.admissionOccupancy(2) == 2);
        CHECK(direct.admissionMinArrivalCycle(2).value() == 2);
        check_state(2);

        CHECK(direct.admissionOccupancy(3) == 1);
        CHECK(direct.admissionMinArrivalCycle(3).value() == 4);
        check_state(3);

        auto second = pop_both(4);
        CHECK(second.has_value() && *second == 20);
        CHECK(direct.admissionOccupancy(4) == 1);
        CHECK(direct.admissionMinArrivalCycle(4).value() == 4);
        check_state(4);
        CHECK(direct.admissionOccupancy(5) == 0);
        CHECK(!direct.admissionMinArrivalCycle(5).has_value());
        check_state(5);

        push_both(30, 8);
        check_state(6);
        legacy.clear();
        direct.clear();
        check_state(6);
        CHECK(direct.empty());
        CHECK(direct.admissionOccupancy(6) == 0);

        push_both(40, 9);
        check_state(9);
        auto after_clear = pop_both(9);
        CHECK(after_clear.has_value() && *after_clear == 40);
        check_state(10);

        legacy.setCapacity(capacity + 1);
        direct.setCapacity(capacity + 1);
        check_state(11);
    }

    std::cout << "PASSED\n";
}

void test_direct_spsc_adapter_wraparound_and_backpressure() {
    std::cout << "Testing DirectSPSCQueueAdapter wraparound/backpressure... ";

    LockFreeQueueAdapter<int> legacy(1);
    DirectSPSCQueueAdapter<int> direct(1);
    constexpr uint64_t kRounds = 2 * LockFreeMessageQueue<int>::CAPACITY + 17;

    for (uint64_t cycle = 0; cycle < kRounds; ++cycle) {
        CHECK(legacy.push(static_cast<int>(cycle), cycle));
        int value = static_cast<int>(cycle);
        CHECK(direct.pushDirect(std::move(value), cycle));
        CHECK(legacy.full() == direct.full());
        CHECK(direct.full());
        CHECK(direct.available() == 0);

        auto legacy_value = legacy.tryPop(cycle);
        auto direct_value = direct.tryPop(cycle);
        CHECK(legacy_value == direct_value);
        CHECK(direct_value.has_value() && *direct_value == static_cast<int>(cycle));
        CHECK(legacy.admissionOccupancy(cycle) == direct.admissionOccupancy(cycle));
        CHECK(direct.admissionOccupancy(cycle) == 1);
        CHECK(legacy.admissionMinArrivalCycle(cycle) == direct.admissionMinArrivalCycle(cycle));
        CHECK(direct.admissionMinArrivalCycle(cycle).value() == cycle);
        CHECK(legacy.admissionOccupancy(cycle + 1) == direct.admissionOccupancy(cycle + 1));
        CHECK(direct.admissionOccupancy(cycle + 1) == 0);
    }

    CHECK(direct.empty());
    std::cout << "PASSED\n";
}

void test_direct_spsc_consume_ready_discard_path() {
    std::cout << "Testing DirectSPSCQueueAdapter consume/discard path... ";

    DirectSPSCQueueAdapter<int> queue(2);
    int value = 77;
    CHECK(queue.pushDirect(std::move(value), 5));

    bool visited = false;
    CHECK(!queue.consumeReady(4, [&](int&) { visited = true; }));
    CHECK(!visited);
    CHECK(queue.consumeReady(5, [&](int& payload) {
        CHECK(payload == 77);
        visited = true;  // Cancellation-style discard: deliberately do not move it.
    }));
    CHECK(visited);
    CHECK(queue.empty());
    CHECK(queue.admissionOccupancy(5) == 1);
    CHECK(queue.admissionMinArrivalCycle(5).value() == 5);
    CHECK(queue.admissionOccupancy(6) == 0);
    CHECK(!queue.admissionMinArrivalCycle(6).has_value());

    value = 88;
    CHECK(queue.pushDirect(std::move(value), 8));
    CHECK(queue.peekReady(7) == nullptr);
    int* peeked = queue.peekReady(8);
    CHECK(peeked != nullptr && *peeked == 88);
    CHECK(!queue.empty());  // peek must not publish admission credit or head movement
    queue.consumePeeked(8);
    CHECK(queue.empty());
    CHECK(queue.admissionOccupancy(8) == 1);
    CHECK(queue.admissionOccupancy(9) == 0);

    std::cout << "PASSED\n";
}

void test_delay_one_cycle_queue_differential_semantics() {
    std::cout << "Testing delay-one slab queue differential semantics... ";

    SingleThreadQueueAdapter<int> heap(20);
    DelayOneCycleQueueAdapter<int> slab(20);
    for (uint64_t cycle = 0; cycle < 80; ++cycle) {
        for (int lane = 0; lane < 3; ++lane) {
            const int value = static_cast<int>(cycle * 3 + lane);
            CHECK(heap.push(value, cycle + 1));
            CHECK(slab.push(value, cycle + 1));
        }
        CHECK(heap.size() == slab.size());
        CHECK(heap.full() == slab.full());
        CHECK(heap.available() == slab.available());
        if (cycle >= 24) {
            const auto heap_values = heap.popAll(cycle - 20);
            const auto slab_values = slab.popAll(cycle - 20);
            CHECK(heap_values == slab_values);
        }
    }
    CHECK(heap.popAll(UINT64_MAX) == slab.popAll(UINT64_MAX));
    CHECK(slab.empty());

    slab.push(7, 100);
    slab.clear();
    CHECK(slab.empty());
    CHECK(slab.push(8, 1));
    CHECK(slab.tryPop(1) == std::optional<int>{8});
    std::cout << "PASSED\n";
}

void test_delay_one_cycle_queue_move_only_and_contract() {
    std::cout << "Testing delay-one slab queue move-only/monotonic contract... ";

    DelayOneCycleQueueAdapter<std::unique_ptr<int>> queue;
    for (int i = 0; i < 40; ++i) CHECK(queue.push(std::make_unique<int>(i), i / 2 + 1));
    for (int i = 0; i < 40; ++i) {
        auto value = queue.tryPop(i / 2 + 1);
        CHECK(value.has_value() && **value == i);
    }

    CHECK(queue.push(std::make_unique<int>(1), 10));
    bool rejected = false;
    try {
        queue.push(std::make_unique<int>(2), 9);
    } catch (const std::logic_error&) {
        rejected = true;
    }
    CHECK(rejected);
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
    test_lock_free_adapter_retires_all_admission_histories();
    test_direct_spsc_adapter_differential_semantics();
    test_direct_spsc_adapter_wraparound_and_backpressure();
    test_direct_spsc_consume_ready_discard_path();
    test_delay_one_cycle_queue_differential_semantics();
    test_delay_one_cycle_queue_move_only_and_contract();

    std::cout << "\nAll registered queue tests PASSED!\n";
    return 0;
}
