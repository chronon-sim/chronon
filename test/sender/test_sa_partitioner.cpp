// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_sa_partitioner.cpp
//
// Unit tests for SimulatedAnnealingPartitioner.

#include <cmath>
#include <iostream>

#include "../TestAssertions.hpp"
#include "sender/schedule/SimulatedAnnealingPartitioner.hpp"
#include "sender/schedule/WeightedPartitioner.hpp"

using namespace chronon::sender;

void assertValidResult(const PartitionResult& result, [[maybe_unused]] size_t num_units,
                       [[maybe_unused]] size_t num_threads) {
    REQUIRE(result.unit_to_thread.size() == num_units);
    for ([[maybe_unused]] size_t thread : result.unit_to_thread) {
        REQUIRE(thread < num_threads);
    }
    if (num_units > 0) {
        REQUIRE(result.imbalance_ratio >= 1.0);
    }
}

void test_empty_input() {
    std::cout << "Testing empty input... ";

    PartitionInput input;
    input.num_units = 0;
    input.num_threads = 0;
    input.sync_cost_ns = 0.0;

    auto result = SimulatedAnnealingPartitioner::partition(input);
    REQUIRE(result.unit_to_thread.empty());
    REQUIRE(result.thread_units.empty());

    std::cout << "PASSED\n";
}

void test_single_unit() {
    std::cout << "Testing single unit... ";

    PartitionInput input;
    input.num_units = 1;
    input.num_threads = 4;
    input.unit_cost_ns = {100.0};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(1);

    auto result = SimulatedAnnealingPartitioner::partition(input);
    REQUIRE(result.unit_to_thread.size() == 1);
    REQUIRE(result.unit_to_thread[0] == 0);

    std::cout << "PASSED\n";
}

void test_more_threads_than_units() {
    std::cout << "Testing more threads than units... ";

    PartitionInput input;
    input.num_units = 2;
    input.num_threads = 8;
    input.unit_cost_ns = {100.0, 200.0};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(2);

    auto result = SimulatedAnnealingPartitioner::partition(input);
    // Threads clamped to 2
    assertValidResult(result, 2, 2);
    // Two units should be on separate threads
    REQUIRE(result.unit_to_thread[0] != result.unit_to_thread[1]);

    std::cout << "PASSED\n";
}

void test_uniform_no_edges() {
    std::cout << "Testing uniform costs, no edges... ";

    PartitionInput input;
    input.num_units = 8;
    input.num_threads = 4;
    input.unit_cost_ns = {100, 100, 100, 100, 100, 100, 100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(8);

    auto result = SimulatedAnnealingPartitioner::partition(input);
    assertValidResult(result, 8, 4);

    // Should be perfectly balanced (2 per thread)
    REQUIRE(result.imbalance_ratio <= 1.01);

    std::cout << "PASSED\n";
}

void test_heavy_unit_isolation() {
    std::cout << "Testing heavy unit isolation... ";

    PartitionInput input;
    input.num_units = 5;
    input.num_threads = 2;
    // Unit 0 is 4x heavier than others
    input.unit_cost_ns = {400, 100, 100, 100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(5);

    auto result = SimulatedAnnealingPartitioner::partition(input);
    assertValidResult(result, 5, 2);

    // Heavy unit should be alone on its thread
    [[maybe_unused]] size_t heavy_thread = result.unit_to_thread[0];
    for (size_t u = 1; u < 5; ++u) {
        REQUIRE(result.unit_to_thread[u] != heavy_thread);
    }

    std::cout << "PASSED\n";
}

void test_delay_zero_colocation() {
    std::cout << "Testing delay=0 co-location... ";

    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 4;
    input.unit_cost_ns = {100, 100, 100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(4);

    // Unit 0 <-> Unit 1 with delay=0 (must co-locate)
    input.adjacency[0].push_back({1, 5, 0});
    input.adjacency[1].push_back({0, 5, 0});

    // Unit 2 <-> Unit 3 with delay=0 (must co-locate)
    input.adjacency[2].push_back({3, 5, 0});
    input.adjacency[3].push_back({2, 5, 0});

    auto result = SimulatedAnnealingPartitioner::partition(input);
    assertValidResult(result, 4, 4);

    // delay=0 pairs must be on the same thread
    REQUIRE(result.unit_to_thread[0] == result.unit_to_thread[1]);
    REQUIRE(result.unit_to_thread[2] == result.unit_to_thread[3]);

    std::cout << "PASSED\n";
}

void test_not_worse_than_2x_weighted() {
    std::cout << "Testing SA not worse than 2x Weighted... ";

    auto run_comparison = [](const PartitionInput& input) {
        auto weighted = WeightedPartitioner::partition(input);
        auto sa = SimulatedAnnealingPartitioner::partition(input);

        REQUIRE(sa.estimated_max_thread_time_ns <=
                weighted.estimated_max_thread_time_ns * 2.0 + 1.0);
    };

    // Case 1: Uniform
    {
        PartitionInput input;
        input.num_units = 8;
        input.num_threads = 4;
        input.unit_cost_ns = {100, 100, 100, 100, 100, 100, 100, 100};
        input.sync_cost_ns = 36.0;
        input.adjacency.resize(8);
        run_comparison(input);
    }

    // Case 2: Skewed costs
    {
        PartitionInput input;
        input.num_units = 6;
        input.num_threads = 3;
        input.unit_cost_ns = {500, 300, 200, 150, 100, 50};
        input.sync_cost_ns = 36.0;
        input.adjacency.resize(6);
        // Chain: 0->1->2->3->4->5
        for (size_t i = 0; i < 5; ++i) {
            input.adjacency[i].push_back({i + 1, 2, 1});
            input.adjacency[i + 1].push_back({i, 2, 1});
        }
        run_comparison(input);
    }

    // Case 3: Dense connectivity
    {
        PartitionInput input;
        input.num_units = 6;
        input.num_threads = 2;
        input.unit_cost_ns = {200, 200, 200, 200, 200, 200};
        input.sync_cost_ns = 50.0;
        input.adjacency.resize(6);
        // All-to-all with delay=3
        for (size_t i = 0; i < 6; ++i) {
            for (size_t j = i + 1; j < 6; ++j) {
                input.adjacency[i].push_back({j, 1, 3});
                input.adjacency[j].push_back({i, 1, 3});
            }
        }
        run_comparison(input);
    }

    std::cout << "PASSED\n";
}

void test_matches_partition_solver_signature() {
    std::cout << "Testing PartitionSolver signature... ";

    PartitionSolver solver = &SimulatedAnnealingPartitioner::partition;

    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 2;
    input.unit_cost_ns = {100, 200, 100, 200};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(4);

    auto result = solver(input);
    assertValidResult(result, 4, 2);

    // Must produce the same result as a direct call
    auto direct = SimulatedAnnealingPartitioner::partition(input);
    REQUIRE(result.unit_to_thread == direct.unit_to_thread);
    REQUIRE(result.estimated_max_thread_time_ns == direct.estimated_max_thread_time_ns);

    std::cout << "PASSED\n";
}

void test_deterministic() {
    std::cout << "Testing determinism... ";

    PartitionInput input;
    input.num_units = 10;
    input.num_threads = 3;
    input.unit_cost_ns = {500, 400, 300, 250, 200, 180, 150, 120, 100, 80};
    input.sync_cost_ns = 40.0;
    input.adjacency.resize(10);
    // Some edges
    input.adjacency[0].push_back({1, 3, 2});
    input.adjacency[1].push_back({0, 3, 2});
    input.adjacency[2].push_back({3, 2, 1});
    input.adjacency[3].push_back({2, 2, 1});
    input.adjacency[4].push_back({5, 1, 5});
    input.adjacency[5].push_back({4, 1, 5});

    auto r1 = SimulatedAnnealingPartitioner::partition(input);
    auto r2 = SimulatedAnnealingPartitioner::partition(input);

    REQUIRE(r1.unit_to_thread == r2.unit_to_thread);
    REQUIRE(r1.estimated_max_thread_time_ns == r2.estimated_max_thread_time_ns);

    std::cout << "PASSED\n";
}

void test_mixed_topology() {
    std::cout << "Testing mixed topology... ";

    // Simulated CPU pipeline: Fetch -> Decode -> Rename -> Dispatch -> IQ -> Exec
    PartitionInput input;
    input.num_units = 6;
    input.num_threads = 2;
    input.unit_cost_ns = {150, 80, 60, 40, 100, 120};  // Varied costs
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(6);

    // Pipeline chain with delay=1
    for (size_t i = 0; i < 5; ++i) {
        input.adjacency[i].push_back({i + 1, 4, 1});
        input.adjacency[i + 1].push_back({i, 4, 1});
    }
    // Feedback from Exec(5) -> Fetch(0) with delay=3
    input.adjacency[5].push_back({0, 2, 3});
    input.adjacency[0].push_back({5, 2, 3});

    auto result = SimulatedAnnealingPartitioner::partition(input);
    assertValidResult(result, 6, 2);

    // Imbalance should be reasonable (< 2.0)
    REQUIRE(result.imbalance_ratio < 2.0);

    std::cout << "PASSED\n";
}

void test_sync_heavy_no_thread_degeneration() {
    std::cout << "Testing sync-heavy no thread degeneration... ";

    // Topology where sync costs dominate compute costs (10x ratio).
    // Under old pure max-thread-time objective, SA would co-locate all
    // units on one thread to eliminate sync cost entirely.
    PartitionInput input;
    input.num_units = 8;
    input.num_threads = 4;
    input.unit_cost_ns = {50, 50, 50, 50, 50, 50, 50, 50};
    input.sync_cost_ns = 500.0;  // 10x compute per unit
    input.adjacency.resize(8);

    // All-to-all bidirectional edges with delay=1
    for (size_t i = 0; i < 8; ++i) {
        for (size_t j = i + 1; j < 8; ++j) {
            input.adjacency[i].push_back({j, 3, 1});
            input.adjacency[j].push_back({i, 3, 1});
        }
    }

    auto result = SimulatedAnnealingPartitioner::partition(input);
    assertValidResult(result, 8, 4);

    // Assert on stats (compute-only), NOT imbalance_ratio (includes sync costs)
    REQUIRE(result.stats.active_threads == 4);
    REQUIRE(result.stats.compute_imbalance_ratio <= 1.8);

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== SimulatedAnnealingPartitioner Tests ===\n\n";

    test_empty_input();
    test_single_unit();
    test_more_threads_than_units();
    test_uniform_no_edges();
    test_heavy_unit_isolation();
    test_delay_zero_colocation();
    test_not_worse_than_2x_weighted();
    test_matches_partition_solver_signature();
    test_deterministic();
    test_mixed_topology();
    test_sync_heavy_no_thread_degeneration();

    std::cout << "\n=== All SimulatedAnnealingPartitioner tests PASSED ===\n";
    return 0;
}
