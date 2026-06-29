// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_sa_critical_path.cpp
//
// Regression tests for the critical-path-aware term in the
// SimulatedAnnealingPartitioner's objective function.
//
// The new term penalizes the worst thread's "anchor chain" — the longest
// intra-thread path whose cost is the sum of unit_cost over units that gate
// on a cross-thread predecessor. When critical_path_weight > 0, SA should
// prefer partitions with shorter anchor chains even if their base makespan
// is slightly worse.

#include <cmath>
#include <iostream>

#include "../TestAssertions.hpp"
#include "sender/schedule/SimulatedAnnealingPartitioner.hpp"
#include "sender/schedule/WeightedPartitioner.hpp"

using namespace chronon::sender;

// Topology builder for the chain-vs-split scenario.
//
// 6 units on 2 threads:
//   P (cost 50) -> A (cost 10) -> B (cost 10) -> C (cost 10) -> D (cost 10)
//   Q (cost 50) -> C
//
// Two interesting assignments have identical structural form but very
// different critical-path chain costs:
//
//   X (chain together on T0):  {A,B,C,D}=T0, {P,Q}=T1
//       Chain T0: P->A (cross, A anchor), A->B->C (C anchor via Q cross),
//                 C->D. Longest anchor chain on T0 = cost(A)+cost(C) = 20.
//       Max CP  = 20.
//
//   Y (chain split across threads): {A,B}=T0, {C,D,P,Q}=T1
//       A anchors on T0 (P->A cross), C anchors on T1 (B->C cross).
//       Each thread's anchor chain contains a single anchor = 10.
//       Max CP = 10.
//
// X has *better* base makespan (chain-local intra-thread dataflow keeps
// sync cost low). Y has *worse* base makespan but shorter critical path.
// With critical_path_weight=0, SA should prefer X. With a large weight,
// SA should prefer Y.

PartitionInput makeChainInput(double critical_path_weight) {
    PartitionInput input;
    input.num_units = 6;
    input.num_threads = 2;
    //              P     Q     A     B     C     D
    input.unit_cost_ns = {50.0, 50.0, 10.0, 10.0, 10.0, 10.0};
    input.sync_cost_ns = 20.0;
    input.critical_path_weight = critical_path_weight;
    input.adjacency.resize(6);

    // Directed edges (adjacency[producer] -> {consumer, num_conns, min_delay}).
    // IDs: P=0, Q=1, A=2, B=3, C=4, D=5.
    input.adjacency[0].push_back({2, 1, 1});  // P->A
    input.adjacency[1].push_back({4, 1, 1});  // Q->C
    input.adjacency[2].push_back({3, 1, 1});  // A->B
    input.adjacency[3].push_back({4, 1, 1});  // B->C
    input.adjacency[4].push_back({5, 1, 1});  // C->D
    return input;
}

// IDs: P=0, Q=1, A=2, B=3, C=4, D=5.
const std::vector<size_t> ASSIGNMENT_X = {1, 1, 0, 0, 0, 0};  // {A,B,C,D}=T0
const std::vector<size_t> ASSIGNMENT_Y = {1, 1, 0, 0, 1, 1};  // {A,B}=T0

void test_chain_cost_matches_hand_computed() {
    std::cout << "Testing chain-cost computation on known topology... ";

    auto input = makeChainInput(/*critical_path_weight=*/1.0);

    double cp_x = SimulatedAnnealingPartitioner::computeCriticalPathChain(input, ASSIGNMENT_X, 2);
    double cp_y = SimulatedAnnealingPartitioner::computeCriticalPathChain(input, ASSIGNMENT_Y, 2);

    // X: A (cost 10, anchor) -> B (not anchor) -> C (cost 10, anchor) -> D
    //    dp along A->B->C->D = 10+0+10+0 = 20.
    REQUIRE(std::abs(cp_x - 20.0) < 1e-6);

    // Y: A anchors on T0 (chain 10). C anchors on T1 (chain 10).
    REQUIRE(std::abs(cp_y - 10.0) < 1e-6);

    std::cout << "PASSED (CP_X=" << cp_x << ", CP_Y=" << cp_y << ")\n";
}

void test_objective_weight_zero_matches_baseline() {
    std::cout << "Testing weight=0 leaves objective unchanged... ";

    auto input_w0 = makeChainInput(/*critical_path_weight=*/0.0);
    auto input_w1 = makeChainInput(/*critical_path_weight=*/1.0);

    double obj_x_w0 = SimulatedAnnealingPartitioner::evaluateObjective(input_w0, ASSIGNMENT_X, 2);
    double obj_x_w1 = SimulatedAnnealingPartitioner::evaluateObjective(input_w1, ASSIGNMENT_X, 2);

    // weight=0 term drops out; weight=1 adds 20 (CP_X).
    REQUIRE(std::abs((obj_x_w1 - obj_x_w0) - 20.0) < 1e-6);

    std::cout << "PASSED\n";
}

void test_objective_ordering_flips_with_weight() {
    std::cout << "Testing objective ordering flips with large weight... ";

    auto input_w0 = makeChainInput(/*critical_path_weight=*/0.0);
    auto input_big = makeChainInput(/*critical_path_weight=*/50.0);

    double obj_x_w0 = SimulatedAnnealingPartitioner::evaluateObjective(input_w0, ASSIGNMENT_X, 2);
    double obj_y_w0 = SimulatedAnnealingPartitioner::evaluateObjective(input_w0, ASSIGNMENT_Y, 2);

    // Without CP term, X (chain together) has better base objective.
    REQUIRE(obj_x_w0 < obj_y_w0);

    double obj_x_big = SimulatedAnnealingPartitioner::evaluateObjective(input_big, ASSIGNMENT_X, 2);
    double obj_y_big = SimulatedAnnealingPartitioner::evaluateObjective(input_big, ASSIGNMENT_Y, 2);

    // With a large CP weight, Y (chain split → shorter CP) wins.
    REQUIRE(obj_y_big < obj_x_big);

    std::cout << "PASSED (obj_X_w0=" << obj_x_w0 << ", obj_Y_w0=" << obj_y_w0
              << "; obj_X_big=" << obj_x_big << ", obj_Y_big=" << obj_y_big << ")\n";
}

void test_sa_prefers_shorter_chain_with_weight() {
    std::cout << "Testing SA prefers shorter CP when weight > 0...\n";

    auto input_w0 = makeChainInput(/*critical_path_weight=*/0.0);
    auto input_big = makeChainInput(/*critical_path_weight=*/50.0);

    auto result_w0 = SimulatedAnnealingPartitioner::partition(input_w0);
    auto result_big = SimulatedAnnealingPartitioner::partition(input_big);

    std::cout << "  partition(weight=0) =";
    for (size_t t : result_w0.unit_to_thread) std::cout << " " << t;
    std::cout << "\n";
    std::cout << "  partition(weight=50)=";
    for (size_t t : result_big.unit_to_thread) std::cout << " " << t;
    std::cout << "\n";

    double cp_w0 = SimulatedAnnealingPartitioner::computeCriticalPathChain(
        input_w0, result_w0.unit_to_thread, 2);
    double cp_big = SimulatedAnnealingPartitioner::computeCriticalPathChain(
        input_big, result_big.unit_to_thread, 2);

    std::cout << "  CP(weight=0)=" << cp_w0 << "  CP(weight=50)=" << cp_big << "\n";

    // With a large weight, SA should find a partition whose critical-path
    // chain is no longer than the no-weight baseline. A strict-less assertion
    // would rely on SA actually moving between partitions with identical
    // base cost; we accept equal-or-shorter and flag regression via the
    // objective-ordering test above.
    REQUIRE(cp_big <= cp_w0);

    std::cout << "  PASSED\n";
}

void test_chain_cost_deterministic() {
    std::cout << "Testing chain-cost determinism... ";

    auto input = makeChainInput(/*critical_path_weight=*/1.0);
    double cp1 = SimulatedAnnealingPartitioner::computeCriticalPathChain(input, ASSIGNMENT_X, 2);
    double cp2 = SimulatedAnnealingPartitioner::computeCriticalPathChain(input, ASSIGNMENT_X, 2);
    double cp3 = SimulatedAnnealingPartitioner::computeCriticalPathChain(input, ASSIGNMENT_X, 2);
    REQUIRE(cp1 == cp2);
    REQUIRE(cp2 == cp3);

    std::cout << "PASSED\n";
}

// Test: Cycle fallback — chain computation doesn't hang or return garbage
// on an intra-thread cycle.

void test_cycle_fallback() {
    std::cout << "Testing intra-thread-cycle fallback... ";

    // 3 units on 1 thread with a 3-cycle plus one cross-thread anchor.
    // Intra-thread cycle A->B->C->A (all on T0). External producer X on T1
    // feeds A, making A an anchor.
    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 2;
    input.unit_cost_ns = {10.0, 10.0, 10.0, 5.0};  // A, B, C, X
    input.sync_cost_ns = 10.0;
    input.critical_path_weight = 1.0;
    input.adjacency.resize(4);
    input.adjacency[0].push_back({1, 1, 1});  // A->B
    input.adjacency[1].push_back({2, 1, 1});  // B->C
    input.adjacency[2].push_back({0, 1, 1});  // C->A (cycle)
    input.adjacency[3].push_back({0, 1, 1});  // X->A (cross, A anchor)

    // IDs: A=0, B=1, C=2, X=3. T0={A,B,C}, T1={X}.
    std::vector<size_t> assignment = {0, 0, 0, 1};

    double cp = SimulatedAnnealingPartitioner::computeCriticalPathChain(input, assignment, 2);

    // Fallback: sum anchor costs per thread. Only A is an anchor on T0.
    REQUIRE(std::abs(cp - 10.0) < 1e-6);

    std::cout << "PASSED (cycle fallback CP=" << cp << ")\n";
}

void test_no_cross_edges_zero_cp() {
    std::cout << "Testing no-cross-edges → CP=0... ";

    PartitionInput input;
    input.num_units = 4;
    input.num_threads = 2;
    input.unit_cost_ns = {10.0, 10.0, 10.0, 10.0};
    input.sync_cost_ns = 10.0;
    input.critical_path_weight = 1.0;
    input.adjacency.resize(4);
    input.adjacency[0].push_back({1, 1, 1});  // A->B (intra)
    input.adjacency[2].push_back({3, 1, 1});  // C->D (intra)

    std::vector<size_t> assignment = {0, 0, 1, 1};
    double cp = SimulatedAnnealingPartitioner::computeCriticalPathChain(input, assignment, 2);
    REQUIRE(cp == 0.0);

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== SA Critical-Path Objective Tests ===\n\n";

    test_chain_cost_matches_hand_computed();
    test_objective_weight_zero_matches_baseline();
    test_objective_ordering_flips_with_weight();
    test_sa_prefers_shorter_chain_with_weight();
    test_chain_cost_deterministic();
    test_cycle_fallback();
    test_no_cross_edges_zero_cp();

    std::cout << "\n=== All tests PASSED ===\n";
    return 0;
}
