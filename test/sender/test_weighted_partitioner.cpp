// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_weighted_partitioner.cpp
//
// Unit tests for WeightedPartitioner cost-aware graph partitioning.

#include <cassert>
#include <cmath>
#include <iostream>
#include <set>

#include "sender/schedule/WeightedPartitioner.hpp"

using namespace chronon::sender;

// Helper to verify a partition result meets basic invariants.
void assertValidResult(const PartitionResult& result, [[maybe_unused]] size_t num_units,
                       [[maybe_unused]] size_t num_threads) {
    assert(result.unit_to_thread.size() == num_units);
    for ([[maybe_unused]] size_t thread : result.unit_to_thread) {
        assert(thread < num_threads);
    }
    assert(result.imbalance_ratio >= 1.0);
}

void test_uniform_no_edges() {
    std::cout << "Testing uniform costs, no edges... ";

    PartitionInput input;
    input.num_units = 8;
    input.num_threads = 4;
    input.unit_cost_ns = {100, 100, 100, 100, 100, 100, 100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(8);

    auto result = WeightedPartitioner::partition(input);
    assertValidResult(result, 8, 4);

    // With 8 uniform units across 4 threads, should be perfectly balanced
    // (2 per thread) -> imbalance = 1.0
    assert(result.imbalance_ratio <= 1.01);
    assert(result.cross_thread_connections == 0);

    std::cout << "PASSED\n";
}

void test_delay_aware_sync() {
    std::cout << "Testing delay-aware sync cost... ";

    // 4 units: A-B connected with delay=1, C-D connected with delay=10
    // Same connectivity count, but A-B should be more strongly attracted
    // than C-D since delay=1 has 10x the sync cost penalty.
    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 2;
    input.unit_cost_ns = {100, 100, 100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(4);

    // A(0) <-> B(1) with delay=1
    input.adjacency[0].push_back({1, 5, 1});
    input.adjacency[1].push_back({0, 5, 1});

    // C(2) <-> D(3) with delay=10
    input.adjacency[2].push_back({3, 5, 10});
    input.adjacency[3].push_back({2, 5, 10});

    auto result = WeightedPartitioner::partition(input);
    assertValidResult(result, 4, 2);

    // A and B should be on the same thread (tight coupling)
    assert(result.unit_to_thread[0] == result.unit_to_thread[1]);

    std::cout << "PASSED\n";
}

void test_evaluate_delay_scaling() {
    std::cout << "Testing evaluate with delay scaling... ";

    PartitionInput input;
    input.num_units = 2;
    input.num_threads = 2;
    input.unit_cost_ns = {100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(2);

    // Bidirectional edge: delay=1, 4 connections
    input.adjacency[0].push_back({1, 4, 1});
    input.adjacency[1].push_back({0, 4, 1});

    // Same-thread assignment: no sync cost
    std::vector<size_t> same_thread = {0, 0};
    [[maybe_unused]] double cost_same = WeightedPartitioner::evaluatePartition(input, same_thread);

    // Cross-thread assignment: sync cost = 36 * 4 * (1/1) = 144 per unit
    std::vector<size_t> diff_thread = {0, 1};
    [[maybe_unused]] double cost_diff_delay1 =
        WeightedPartitioner::evaluatePartition(input, diff_thread);

    // Same-thread should have higher compute (200ns on one thread)
    // but no sync cost. diff_thread has 100+144=244ns on each thread.
    assert(cost_same > 0);
    assert(cost_diff_delay1 > 0);

    // Now test with delay=10: sync cost should be 36 * 4 * (1/10) = 14.4
    input.adjacency[0][0].min_delay = 10;
    input.adjacency[1][0].min_delay = 10;

    [[maybe_unused]] double cost_diff_delay10 =
        WeightedPartitioner::evaluatePartition(input, diff_thread);

    // High-delay cross-thread should be cheaper than low-delay cross-thread
    assert(cost_diff_delay10 < cost_diff_delay1);

    std::cout << "PASSED\n";
}

void test_directed_sync_cost_charged_to_consumer() {
    std::cout << "Testing directed sync cost is charged to consumer... ";

    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 2;
    input.unit_cost_ns = {10, 10, 10, 10};
    input.sync_cost_ns = 100.0;
    input.adjacency.resize(4);

    // Fan-in: producers 0,1,2 feed consumer 3 across threads.
    input.adjacency[0].push_back({3, 1, 1});
    input.adjacency[1].push_back({3, 1, 1});
    input.adjacency[2].push_back({3, 1, 1});

    std::vector<size_t> assignment = {0, 0, 0, 1};
    auto per_thread = WeightedPartitioner::evaluatePerThread(input, assignment);

    assert(per_thread.size() == 2);
    assert(per_thread[0] == 30.0);
    assert(per_thread[1] == 310.0);
    assert(WeightedPartitioner::evaluatePartition(input, assignment) == 310.0);

    std::cout << "PASSED\n";
}

void test_heavy_unit_isolation() {
    std::cout << "Testing heavy unit isolation... ";

    // One very heavy unit (6000ns) + many light ones (100ns each)
    PartitionInput input;
    input.num_units = 6;
    input.num_threads = 3;
    input.unit_cost_ns = {6000, 100, 100, 100, 100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(6);

    auto result = WeightedPartitioner::partition(input);
    assertValidResult(result, 6, 3);

    // The heavy unit should be alone on its thread
    [[maybe_unused]] size_t heavy_thread = result.unit_to_thread[0];
    [[maybe_unused]] size_t count_on_heavy = 0;
    for (size_t u = 0; u < 6; ++u) {
        if (result.unit_to_thread[u] == heavy_thread) count_on_heavy++;
    }
    assert(count_on_heavy == 1);

    std::cout << "PASSED\n";
}

void test_parallel_beneficial() {
    std::cout << "Testing parallel beneficial heuristic... ";

    // With costs: 6000, 5000, 4000, 4000 across 4 threads
    // Total = 19000, max = 6000
    // Old: 6000 < 1.5 * 4750 = 7125 -> yes (would have been yes)
    // New: 6000 * 1.10 = 6600 < 19000 -> yes
    // Both say yes for this case.

    // Extreme imbalance: 10000 + 100 + 100 + 100 = 10300
    // Old: 10000 < 1.5 * 2575 = 3862 -> NO
    // New: 10000 * 1.10 = 11000 < 10300 -> NO (correctly rejects)
    // Both reject extreme imbalance.

    // Moderate imbalance that old rejects but new accepts:
    // 6000, 5000, 4000, 3000 = 18000
    // max=6000, avg=4500
    // Old: 6000 < 1.5 * 4500 = 6750 -> yes
    // New: 6000 * 1.10 = 6600 < 18000 -> yes

    // Case where old rejects but new accepts:
    // 8000, 3000, 2000, 1000 = 14000
    // max=8000, avg=3500
    // Old: 8000 < 1.5 * 3500 = 5250 -> NO (rejects 1.75x speedup!)
    // New: 8000 * 1.10 = 8800 < 14000 -> YES (correct: 1.75x speedup)
    // This is the key improvement.

    // Verify via evaluatePartition that the model is consistent
    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 4;
    input.unit_cost_ns = {8000, 3000, 2000, 1000};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(4);

    auto result = WeightedPartitioner::partition(input);
    assertValidResult(result, 4, 4);

    // Each unit should be on its own thread (no edges to couple them)
    std::set<size_t> threads_used;
    for (size_t t : result.unit_to_thread) {
        threads_used.insert(t);
    }
    assert(threads_used.size() == 4);

    std::cout << "PASSED\n";
}

void test_cluster_partition() {
    std::cout << "Testing cluster-level partitioning... ";

    // Simulate cluster partitioning: 2 clusters of 3 units each
    // Cluster 0 (units 0,1,2): total cost = 300ns
    // Cluster 1 (units 3,4,5): total cost = 600ns
    // Connected by delay=2 edge between them
    PartitionInput cluster_input;
    cluster_input.num_units = 2;  // 2 clusters as super-nodes
    cluster_input.num_threads = 2;
    cluster_input.unit_cost_ns = {300, 600};
    cluster_input.sync_cost_ns = 36.0;
    cluster_input.adjacency.resize(2);
    cluster_input.adjacency[0].push_back({1, 3, 2});
    cluster_input.adjacency[1].push_back({0, 3, 2});

    auto result = WeightedPartitioner::partition(cluster_input);
    assertValidResult(result, 2, 2);

    // Clusters should be on different threads
    assert(result.unit_to_thread[0] != result.unit_to_thread[1]);

    std::cout << "PASSED\n";
}

void test_adjacency_aggregation() {
    std::cout << "Testing adjacency aggregation... ";

    // Verify that aggregated adjacency (multiple connections between same
    // pair) produces better results than non-aggregated (duplicate edges).
    PartitionInput aggregated;
    aggregated.num_units = 4;
    aggregated.num_threads = 2;
    aggregated.unit_cost_ns = {200, 200, 200, 200};
    aggregated.sync_cost_ns = 36.0;
    aggregated.adjacency.resize(4);

    // Units 0-1: 5 connections, min delay=1 (aggregated)
    aggregated.adjacency[0].push_back({1, 5, 1});
    aggregated.adjacency[1].push_back({0, 5, 1});
    // Units 2-3: 1 connection, delay=5
    aggregated.adjacency[2].push_back({3, 1, 5});
    aggregated.adjacency[3].push_back({2, 1, 5});

    auto result_agg = WeightedPartitioner::partition(aggregated);
    assertValidResult(result_agg, 4, 2);

    // 0 and 1 should be co-located (strong coupling)
    assert(result_agg.unit_to_thread[0] == result_agg.unit_to_thread[1]);

    // Non-aggregated: same as having individual edges (count=1 each)
    PartitionInput non_aggregated;
    non_aggregated.num_units = 4;
    non_aggregated.num_threads = 2;
    non_aggregated.unit_cost_ns = {200, 200, 200, 200};
    non_aggregated.sync_cost_ns = 36.0;
    non_aggregated.adjacency.resize(4);

    // 5 separate edges between 0-1, each with count=1
    for (int i = 0; i < 5; ++i) {
        non_aggregated.adjacency[0].push_back({1, 1, 1});
        non_aggregated.adjacency[1].push_back({0, 1, 1});
    }
    non_aggregated.adjacency[2].push_back({3, 1, 5});
    non_aggregated.adjacency[3].push_back({2, 1, 5});

    auto result_nonagg = WeightedPartitioner::partition(non_aggregated);

    // Both should keep 0-1 together
    assert(result_nonagg.unit_to_thread[0] == result_nonagg.unit_to_thread[1]);

    std::cout << "PASSED\n";
}

void test_mixed_topology() {
    std::cout << "Testing mixed topology... ";

    // Simulate a simplified CPU pipeline:
    // Fetch(5000) -> Decode(1000) -> Rename(1000) -> Dispatch(1000) -> IQ(500)
    //                                                     -> LSU(6000)
    //                                                     -> ROB(2000)
    PartitionInput input;
    input.num_units = 7;
    input.num_threads = 4;
    input.unit_cost_ns = {5000, 1000, 1000, 1000, 500, 6000, 2000};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(7);

    // Pipeline connections (delay=1)
    auto add_edge = [&](size_t u, size_t v, uint32_t delay) {
        input.adjacency[u].push_back({v, 1, delay});
        input.adjacency[v].push_back({u, 1, delay});
    };

    add_edge(0, 1, 1);  // Fetch -> Decode
    add_edge(1, 2, 1);  // Decode -> Rename
    add_edge(2, 3, 1);  // Rename -> Dispatch
    add_edge(3, 4, 1);  // Dispatch -> IQ
    add_edge(3, 5, 1);  // Dispatch -> LSU
    add_edge(4, 6, 1);  // IQ -> ROB
    add_edge(5, 6, 1);  // LSU -> ROB

    auto result = WeightedPartitioner::partition(input);
    assertValidResult(result, 7, 4);

    // LSU (6000ns) and Fetch (5000ns) should be on different threads
    assert(result.unit_to_thread[0] != result.unit_to_thread[5]);

    // Total cost = 16500, imbalance should be reasonable
    assert(result.imbalance_ratio < 2.0);

    std::cout << "PASSED\n";
}

void test_edge_cases() {
    std::cout << "Testing edge cases... ";

    // Empty input
    {
        PartitionInput input;
        input.num_units = 0;
        input.num_threads = 4;
        input.sync_cost_ns = 36.0;
        auto result = WeightedPartitioner::partition(input);
        assert(result.unit_to_thread.empty());
    }

    // Single unit
    {
        PartitionInput input;
        input.num_units = 1;
        input.num_threads = 4;
        input.unit_cost_ns = {1000};
        input.sync_cost_ns = 36.0;
        input.adjacency.resize(1);
        auto result = WeightedPartitioner::partition(input);
        assert(result.unit_to_thread.size() == 1);
        assert(result.unit_to_thread[0] == 0);
    }

    // More threads than units
    {
        PartitionInput input;
        input.num_units = 2;
        input.num_threads = 8;
        input.unit_cost_ns = {1000, 1000};
        input.sync_cost_ns = 36.0;
        input.adjacency.resize(2);
        auto result = WeightedPartitioner::partition(input);
        assert(result.unit_to_thread.size() == 2);
        // Should use only 2 threads
        assert(result.unit_to_thread[0] != result.unit_to_thread[1]);
    }

    std::cout << "PASSED\n";
}

void test_delay_zero_max_weight() {
    std::cout << "Testing delay=0 edges get max sync weight... ";

    // Two units with delay=0 edge — should always be co-located
    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 2;
    input.unit_cost_ns = {500, 500, 100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(4);

    // delay=0 between 0 and 1 (tight coupling)
    input.adjacency[0].push_back({1, 3, 0});
    input.adjacency[1].push_back({0, 3, 0});

    auto result = WeightedPartitioner::partition(input);
    assertValidResult(result, 4, 2);

    // Units 0 and 1 must be on the same thread (delay=0 -> max sync cost)
    assert(result.unit_to_thread[0] == result.unit_to_thread[1]);

    std::cout << "PASSED\n";
}

void test_lpt_isolates_dominant_unit() {
    std::cout << "Testing LPT isolates dominant unit... ";

    // Mimics Nucleus: LSU(696) coupled to rob(101), decode(72), rename(128)
    // With 4 threads, LSU must be isolated despite coupling
    PartitionInput input;
    input.num_units = 6;
    input.num_threads = 4;
    input.unit_cost_ns = {696, 101, 72, 128, 396, 100};  // lsu,rob,dec,ren,fetch,sched
    input.sync_cost_ns = 52.0;
    input.adjacency.resize(6);

    // LSU connected to rob, rename (delay=1)
    auto add_edge = [&](size_t u, size_t v, size_t conns, uint32_t delay) {
        input.adjacency[u].push_back({v, conns, delay});
        input.adjacency[v].push_back({u, conns, delay});
    };
    add_edge(0, 1, 2, 1);  // lsu <-> rob
    add_edge(0, 3, 1, 1);  // lsu <-> rename (wakeup)
    add_edge(1, 2, 1, 1);  // rob <-> decode
    add_edge(2, 3, 1, 1);  // decode <-> rename
    add_edge(3, 4, 1, 1);  // rename <-> fetch (via pipeline)

    auto result = WeightedPartitioner::partition(input);
    assertValidResult(result, 6, 4);

    // LSU must be alone on its thread (not grouped with rob/decode/rename)
    size_t lsu_thread = result.unit_to_thread[0];
    [[maybe_unused]] size_t count_on_lsu_thread = 0;
    for (size_t t : result.unit_to_thread) {
        if (t == lsu_thread) count_on_lsu_thread++;
    }
    assert(count_on_lsu_thread == 1);

    // With a dominant unit (~47% of total compute), some imbalance is
    // inevitable, but it should stay well below 2x (naive round-robin
    // would be ~2.7x).  This is a relative metric independent of the
    // absolute sync cost value.
    assert(result.imbalance_ratio < 2.0);

    std::cout << "PASSED\n";
}

void test_multi_unit_relocate() {
    std::cout << "Testing multi-unit relocate... ";

    // 5 units across 3 threads, no edges.
    // LPT: 400→T0, 400→T1, 100→T2, 100→T2(200), 100→T2(300)
    // T0=400, T1=400, T2=300. Max=400. Already good.
    // Multi-unit relocate won't change this (optimal by LPT).
    PartitionInput input;
    input.num_units = 5;
    input.num_threads = 3;
    input.unit_cost_ns = {400, 400, 100, 100, 100};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(5);

    auto result = WeightedPartitioner::partition(input);
    assertValidResult(result, 5, 3);

    // Max thread should not exceed ~500 (ideal: 1100/3 ≈ 367)
    assert(result.estimated_max_thread_time_ns <= 500.0);

    std::cout << "PASSED\n";
}

void test_partition_solver_type_alias() {
    std::cout << "Testing PartitionSolver type alias... ";

    // Verify WeightedPartitioner::partition matches the PartitionSolver signature
    PartitionSolver solver = &WeightedPartitioner::partition;

    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 2;
    input.unit_cost_ns = {100, 200, 100, 200};
    input.sync_cost_ns = 36.0;
    input.adjacency.resize(4);

    auto result = solver(input);
    assertValidResult(result, 4, 2);

    // Must produce the same result as a direct call
    auto direct = WeightedPartitioner::partition(input);
    assert(result.unit_to_thread == direct.unit_to_thread);
    assert(result.estimated_max_thread_time_ns == direct.estimated_max_thread_time_ns);

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== WeightedPartitioner Tests ===\n\n";

    test_uniform_no_edges();
    test_delay_aware_sync();
    test_evaluate_delay_scaling();
    test_directed_sync_cost_charged_to_consumer();
    test_heavy_unit_isolation();
    test_parallel_beneficial();
    test_cluster_partition();
    test_adjacency_aggregation();
    test_mixed_topology();
    test_edge_cases();
    test_delay_zero_max_weight();
    test_lpt_isolates_dominant_unit();
    test_multi_unit_relocate();
    test_partition_solver_type_alias();

    std::cout << "\n=== All WeightedPartitioner tests PASSED ===\n";
    return 0;
}
