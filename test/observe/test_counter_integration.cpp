// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_counter_integration.cpp
//
// Integration test for Counter (per-instance counters) with ObservableUnit.

#include <cassert>
#include <iostream>

#include "observe/LocalCounter.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationQueue.hpp"

using namespace chronon::observe;

// Test unit with Counter members
class TestUnit : public ObservableUnit {
public:
    Counter counter1{counter_detail::InternalConstructionTag{}, this, "counter1",
                     "First test counter"};
    Counter counter2{counter_detail::InternalConstructionTag{}, this, "counter2",
                     "Second test counter"};
    Counter counter3{counter_detail::InternalConstructionTag{}, this, "counter3",
                     "Third test counter"};
};

int main() {
    std::cout << "=== Counter Integration Test ===" << std::endl;

    // Create a standalone context
    ObservationQueue queue(1024 * 1024);  // 1MB queue
    ObservationContext ctx(&queue, []() { return 100ULL; }, 0, "test_unit");

    // Create unit and attach context
    TestUnit unit;
    unit.setObservationContext(&ctx);

    // Verify counters are registered
    assert(unit.counter1.isRegistered());
    assert(unit.counter2.isRegistered());
    assert(unit.counter3.isRegistered());

    // Check counter storage size (should have exactly 3 counters)
    std::cout << "Context counter storage size: " << ctx.counters().size() << std::endl;
    assert(ctx.counters().size() >= 3);

    // Increment counters
    unit.counter1 += 10;
    unit.counter2 += 20;
    unit.counter3 += 30;

    // Verify values
    assert(unit.counter1.get() == 10);
    assert(unit.counter2.get() == 20);
    assert(unit.counter3.get() == 30);

    std::cout << "Counter values verified:" << std::endl;
    std::cout << "  counter1: " << unit.counter1.get() << std::endl;
    std::cout << "  counter2: " << unit.counter2.get() << std::endl;
    std::cout << "  counter3: " << unit.counter3.get() << std::endl;

    // Test epoch operations
    ctx.counters().commitAllEpochs();
    unit.counter1 += 5;
    assert(unit.counter1.get() == 15);

    ctx.counters().rollbackAllEpochs();
    assert(unit.counter1.get() == 10);

    std::cout << "Epoch operations verified" << std::endl;

    // Memory footprint
    size_t num_counters = ctx.counters().size();
    size_t memory_per_counter = sizeof(SimpleCounter);
    size_t total_memory = num_counters * memory_per_counter;
    std::cout << "\nMemory footprint:" << std::endl;
    std::cout << "  " << num_counters << " counters × " << memory_per_counter
              << " bytes = " << total_memory << " bytes (" << (total_memory / 1024.0) << " KB)"
              << std::endl;

    std::cout << "\n=== Test passed! ===" << std::endl;
    return 0;
}
