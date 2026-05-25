// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_counter_units.cpp
//
// Verify that Counter members in cpu_pipeline_common.hpp have proper metadata.

#include <cassert>
#include <iostream>
#include <string>

#include "../../examples/cpu_pipeline_common.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationQueue.hpp"

using namespace chronon::observe;
using namespace cpu_pipeline;

int main() {
    std::cout << "=== Counter Units Verification Test ===" << std::endl;
    std::cout << std::endl;

    // Create a context and a FetchUnit to verify its counter metadata
    ObservationQueue queue(1024 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch");

    FetchUnit fetch;
    fetch.setObservationContext(&ctx);

    // Verify counter metadata via FixedCounterStorage::forEach
    std::cout << "Counter metadata verification:" << std::endl;
    std::cout << "--------------------------------" << std::endl;

    size_t named_count = 0;
    ctx.counters().forEach([&](CounterId, const CounterInfo& info, uint64_t) {
        if (!info.name.empty()) {
            std::cout << "  " << info.name << " [" << info.unit << "]" << std::endl;
            named_count++;
        }
    });

    std::cout << std::endl;
    std::cout << "Named counters: " << named_count << std::endl;
    // FetchUnit should have: fetched, icache_hits, icache_misses, bp_predictions, flushes
    assert(named_count >= 5);

    // Check memory efficiency
    size_t total_size = ctx.counters().size();
    std::cout << "Memory efficiency:" << std::endl;
    std::cout << "  Counter storage size: " << total_size << " counters" << std::endl;
    std::cout << "  Memory per unit: " << (total_size * sizeof(SimpleCounter)) << " bytes"
              << std::endl;
    std::cout << "  Old architecture: 65536 bytes (64 KB)" << std::endl;

    double savings = 100.0 * (1.0 - (total_size * sizeof(SimpleCounter)) / 65536.0);
    std::cout << "  Savings: " << savings << "%" << std::endl;
    assert(savings > 90.0);

    std::cout << std::endl;
    std::cout << "=== Test passed! ===" << std::endl;
    return 0;
}
