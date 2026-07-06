// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_local_counter.cpp
//
// Test for Counter per-instance counter system.

#include <cassert>
#include <iostream>
#include <memory>

#include "observe/LocalCounter.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationQueue.hpp"

using namespace chronon::observe;

// Test unit with local counters
class TestUnit : public ObservableUnit {
public:
    // Per-instance counters - each TestUnit instance has its own
    Counter ops{counter_detail::InternalConstructionTag{}, this, "ops", "Operations executed",
                "ops"};
    Counter stalls{counter_detail::InternalConstructionTag{}, this, "stalls", "Stall cycles",
                   "cycles"};
    Counter cache_hits{counter_detail::InternalConstructionTag{}, this, "cache_hits", "Cache hits",
                       "hits"};

    void doWork() {
        ++ops;
        ops += 5;
        stalls += 10;
        ++cache_hits;
    }
};

// Test 1: Counter basic operations
void test_basic_operations() {
    std::cout << "Test 1: Counter basic operations..." << std::endl;

    // Create context
    ObservationQueue queue(1024 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "test_unit");

    // Create unit with local counters
    TestUnit unit;

    // Counters should not be registered yet
    assert(!unit.ops.isRegistered());
    assert(!unit.stalls.isRegistered());
    assert(!unit.cache_hits.isRegistered());

    // Attach observation context - this should register all local counters
    unit.setObservationContext(&ctx);

    // Counters should now be registered
    assert(unit.ops.isRegistered());
    assert(unit.stalls.isRegistered());
    assert(unit.cache_hits.isRegistered());

    // Initial values should be 0
    assert(unit.ops.get() == 0);
    assert(unit.stalls.get() == 0);
    assert(unit.cache_hits.get() == 0);

    std::cout << "  Counters registered successfully" << std::endl;

    // Do some work
    unit.doWork();

    // Verify values
    assert(unit.ops.get() == 6);      // 1 + 5
    assert(unit.stalls.get() == 10);  // 10
    assert(unit.cache_hits.get() == 1);

    std::cout << "  Counter values:" << std::endl;
    std::cout << "    ops: " << unit.ops.get() << " (expected 6)" << std::endl;
    std::cout << "    stalls: " << unit.stalls.get() << " (expected 10)" << std::endl;
    std::cout << "    cache_hits: " << unit.cache_hits.get() << " (expected 1)" << std::endl;

    std::cout << "  Counter operations verified" << std::endl;
}

// Test 2: Multiple instances with separate counters
void test_multiple_instances() {
    std::cout << "\nTest 2: Multiple instances with separate counters..." << std::endl;

    // Create separate contexts for each unit
    ObservationQueue queue1(1024 * 1024);
    ObservationQueue queue2(1024 * 1024);
    ObservationContext ctx1(&queue1, []() { return 0ULL; }, 0, "unit1");
    ObservationContext ctx2(&queue2, []() { return 0ULL; }, 0, "unit2");

    // Create two unit instances
    TestUnit unit1;
    TestUnit unit2;

    // Attach contexts
    unit1.setObservationContext(&ctx1);
    unit2.setObservationContext(&ctx2);

    // Modify unit1 counters
    unit1.ops += 100;
    unit1.stalls += 50;

    // Modify unit2 counters differently
    unit2.ops += 200;
    unit2.stalls += 25;

    // Verify that counters are independent
    assert(unit1.ops.get() == 100);
    assert(unit1.stalls.get() == 50);
    assert(unit2.ops.get() == 200);
    assert(unit2.stalls.get() == 25);

    std::cout << "  Unit1 ops: " << unit1.ops.get() << " (expected 100)" << std::endl;
    std::cout << "  Unit2 ops: " << unit2.ops.get() << " (expected 200)" << std::endl;
    std::cout << "  Counters are independent between instances" << std::endl;
}

// Test 3: Counter without context (graceful degradation)
void test_without_context() {
    std::cout << "\nTest 3: Counter without context (graceful degradation)..." << std::endl;

    // Create unit without attaching context
    TestUnit unit;

    // Should not crash when incrementing without context
    unit.doWork();  // Should be safe even without context

    // Values should be 0 (no-op when not registered)
    assert(unit.ops.get() == 0);
    assert(unit.stalls.get() == 0);

    std::cout << "  Operations without context are safe (no crash)" << std::endl;
}

// Test 4: Counter reset
void test_reset() {
    std::cout << "\nTest 4: Counter reset..." << std::endl;

    ObservationQueue queue(1024 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "test_unit");

    TestUnit unit;
    unit.setObservationContext(&ctx);

    // Increment counters
    unit.ops += 100;
    unit.stalls += 50;

    assert(unit.ops.get() == 100);
    assert(unit.stalls.get() == 50);

    // Reset counters
    unit.ops.reset();
    unit.stalls.reset();

    assert(unit.ops.get() == 0);
    assert(unit.stalls.get() == 0);

    std::cout << "  Counter reset works correctly" << std::endl;
}

// Test 5: Counter metadata
void test_metadata() {
    std::cout << "\nTest 5: Counter metadata..." << std::endl;

    TestUnit unit;

    // Check metadata
    assert(unit.ops.name() == "ops");
    assert(unit.ops.description() == "Operations executed");
    assert(unit.ops.unit() == "ops");

    assert(unit.stalls.name() == "stalls");
    assert(unit.stalls.description() == "Stall cycles");
    assert(unit.stalls.unit() == "cycles");

    std::cout << "  ops: name='" << unit.ops.name() << "', desc='" << unit.ops.description()
              << "', unit='" << unit.ops.unit() << "'" << std::endl;
    std::cout << "  Metadata preserved correctly" << std::endl;
}

int main() {
    std::cout << "=== Counter Tests ===" << std::endl;
    std::cout << std::endl;

    try {
        test_basic_operations();
        test_multiple_instances();
        test_without_context();
        test_reset();
        test_metadata();

        std::cout << "\n=== All tests passed! ===" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "\nTest failed: " << e.what() << std::endl;
        return 1;
    }
}
