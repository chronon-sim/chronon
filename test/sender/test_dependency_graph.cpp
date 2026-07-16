// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_dependency_graph.cpp
//
// Unit tests for DependencyGraph and graph algorithms

#include <cassert>
#include <cstdint>
#include <iostream>
#include <limits>
#include <random>
#include <stdexcept>
#include <vector>

#include "../TestAssertions.hpp"
#include "sender/schedule/WeightedDependencyReduction.hpp"
#include "sender/util/Graph.hpp"

using namespace chronon::sender;

namespace reduction = chronon::sender::weighted_dependency_reduction;

void test_directed_graph_basic() {
    std::cout << "Testing DirectedGraph basic... ";

    DirectedGraph graph(4);

    graph.addEdge(0, 1, 5);
    graph.addEdge(1, 2, 3);
    graph.addEdge(0, 3, 10);
    graph.addEdge(3, 2, 2);

    assert(graph.numNodes() == 4);
    assert(graph.numEdges() == 4);

    assert(graph.hasEdge(0, 1));
    assert(graph.hasEdge(1, 2));
    assert(!graph.hasEdge(2, 0));

    assert(graph.edgeWeight(0, 1) == 5);
    assert(graph.edgeWeight(1, 2) == 3);
    assert(graph.edgeWeight(2, 0) == DirectedGraph::INF);

    // Test neighbors
    auto n0 = graph.neighbors(0);
    assert(n0.size() == 2);

    // Test predecessors
    auto p2 = graph.predecessors(2);
    assert(p2.size() == 2);

    std::cout << "PASSED\n";
}

void test_floyd_warshall() {
    std::cout << "Testing Floyd-Warshall... ";

    // Create a simple graph:
    // 0 --5--> 1 --3--> 2
    //          |        ^
    //          +--2-----+
    DirectedGraph graph(3);
    graph.addEdge(0, 1, 5);
    graph.addEdge(1, 2, 3);
    graph.addEdge(0, 2, 10);  // Direct but longer path

    auto dist = floydWarshall(graph);

    assert(dist[0][0] == 0);
    assert(dist[0][1] == 5);
    assert(dist[0][2] == 8);  // Through 1, not direct
    assert(dist[1][2] == 3);
    assert(dist[2][0] == DirectedGraph::INF);  // No path

    std::cout << "PASSED\n";
}

void test_tarjan_scc() {
    std::cout << "Testing Tarjan SCC... ";

    // Graph with two SCCs:
    // SCC1: 0 <--> 1 <--> 2
    // SCC2: 3 (single node)
    // 2 --> 3
    DirectedGraph graph(4);
    graph.addEdge(0, 1, 1);
    graph.addEdge(1, 2, 1);
    graph.addEdge(2, 0, 1);  // Creates SCC
    graph.addEdge(2, 3, 1);  // Edge to isolated node

    auto scc = tarjanSCC(graph);

    assert(scc.numComponents() == 2);

    // Node 3 should be in its own component
    assert(!scc.inCycle(3));

    // Nodes 0, 1, 2 should be in same component
    assert(scc.component[0] == scc.component[1]);
    assert(scc.component[1] == scc.component[2]);
    assert(scc.component[2] != scc.component[3]);

    std::cout << "PASSED\n";
}

void test_johnson_cycles() {
    std::cout << "Testing Johnson's cycle detection... ";

    // Graph with one cycle: 0 --> 1 --> 2 --> 0
    DirectedGraph graph(4);
    graph.addEdge(0, 1, 5);
    graph.addEdge(1, 2, 3);
    graph.addEdge(2, 0, 2);
    graph.addEdge(2, 3, 1);  // Not part of cycle

    auto cycles = johnsonAllCycles(graph);

    // Should find at least one cycle of length 3 with weight 10
    assert(!cycles.empty());
    bool found_expected = false;
    for (const auto& cycle : cycles) {
        if (cycle.nodes.size() == 3 && cycle.total_weight == 10) {
            found_expected = true;
            break;
        }
    }
    assert(found_expected);
    (void)found_expected;

    std::cout << "PASSED\n";
}

void test_johnson_tight_cycle() {
    std::cout << "Testing tight cycle detection... ";

    // Graph with tight cycle (zero delay): 0 --> 1 --> 0
    DirectedGraph graph(2);
    graph.addEdge(0, 1, 0);
    graph.addEdge(1, 0, 0);

    auto cycles = johnsonAllCycles(graph);

    // Note: Will find both cycles (0,1,0) and (1,0,1)
    assert(cycles.size() >= 1);
    assert(cycles[0].isTight());

    std::cout << "PASSED\n";
}

void test_topological_sort_acyclic() {
    std::cout << "Testing topological sort (acyclic)... ";

    // DAG: 0 --> 1 --> 2
    //      |         ^
    //      +---------+
    DirectedGraph graph(3);
    graph.addEdge(0, 1, 1);
    graph.addEdge(1, 2, 1);
    graph.addEdge(0, 2, 1);

    auto result = topologicalSort(graph);

    assert(!result.has_cycle);
    assert(result.order.size() == 3);
    assert(result.order[0] == 0);  // 0 must come first

    // 1 must come before 2
    size_t pos1 = 0, pos2 = 0;
    for (size_t i = 0; i < result.order.size(); ++i) {
        if (result.order[i] == 1) pos1 = i;
        if (result.order[i] == 2) pos2 = i;
    }
    assert(pos1 < pos2);
    (void)pos1;
    (void)pos2;

    std::cout << "PASSED\n";
}

void test_topological_sort_cyclic() {
    std::cout << "Testing topological sort (cyclic)... ";

    // Graph with cycle
    DirectedGraph graph(3);
    graph.addEdge(0, 1, 1);
    graph.addEdge(1, 2, 1);
    graph.addEdge(2, 0, 1);

    auto result = topologicalSort(graph);

    assert(result.has_cycle);
    assert(result.order.size() < 3);

    std::cout << "PASSED\n";
}

void test_independent_subgraphs() {
    std::cout << "Testing independent subgraph detection... ";

    // Two disconnected components: {0, 1} and {2, 3}
    DirectedGraph graph(4);
    graph.addEdge(0, 1, 1);
    graph.addEdge(2, 3, 1);

    auto groups = findIndependentSubgraphs(graph);

    assert(groups.size() == 2);

    // Each group should have 2 nodes
    assert(groups[0].size() == 2);
    assert(groups[1].size() == 2);

    std::cout << "PASSED\n";
}

void test_multiple_cycles() {
    std::cout << "Testing multiple cycle detection... ";

    // Graph with multiple cycles
    // 0 --> 1 --> 2 --> 0 (cycle 1)
    //       |     ^
    //       +-3---+ (cycle 2: 1 --> 3 --> 2 --> 1)
    DirectedGraph graph(4);
    graph.addEdge(0, 1, 1);
    graph.addEdge(1, 2, 1);
    graph.addEdge(2, 0, 1);  // Cycle 1
    graph.addEdge(1, 3, 1);
    graph.addEdge(3, 2, 1);  // Part of cycle 2

    auto cycles = johnsonAllCycles(graph);

    // Should find multiple cycles
    assert(cycles.size() >= 1);

    std::cout << "PASSED\n";
}

void test_self_loop() {
    std::cout << "Testing self-loop detection... ";

    DirectedGraph graph(2);
    graph.addEdge(0, 0, 5);  // Self loop
    graph.addEdge(0, 1, 1);

    auto cycles = johnsonAllCycles(graph);

    // Should detect self-loop
    bool found_self_loop = false;
    for (const auto& cycle : cycles) {
        if (cycle.nodes.size() == 1 && cycle.nodes[0] == 0) {
            found_self_loop = true;
            assert(cycle.total_weight == 5);
        }
    }
    assert(found_self_loop);
    (void)found_self_loop;

    std::cout << "PASSED\n";
}

void requireIrredundant(size_t clusters, const std::vector<reduction::Edge>& retained) {
    for (size_t removed_index = 0; removed_index < retained.size(); ++removed_index) {
        std::vector<reduction::Edge> without_candidate;
        without_candidate.reserve(retained.size() - 1);
        for (size_t i = 0; i < retained.size(); ++i) {
            if (i != removed_index) without_candidate.push_back(retained[i]);
        }

        const auto alternate = reduction::closure(clusters, without_candidate);
        const auto& candidate = retained[removed_index];
        REQUIRE(alternate[candidate.dependent][candidate.predecessor] > candidate.delay);
    }
}

void test_weighted_reduction_respects_delay_constraints() {
    std::cout << "Testing weighted dependency reduction constraints... ";

    const std::vector<reduction::Edge> edges = {
        {0, 1, 2}, {1, 2, 3}, {0, 2, 5},  // Equal-delay alternate path: removable.
        {0, 3, 4}, {3, 2, 2},             // Delay 6 alternate path: insufficient.
    };
    const auto result = reduction::reduce(4, edges);
    REQUIRE((result.removed == std::vector<reduction::Edge>{{0, 2, 5}}));
    REQUIRE(result.retained.size() == 4);
    REQUIRE(result.fan_in_before == std::vector<size_t>({3, 1, 0, 1}));
    REQUIRE(result.fan_in_after == std::vector<size_t>({2, 1, 0, 1}));
    REQUIRE(reduction::closure(4, edges) == reduction::closure(4, result.retained));

    std::cout << "PASSED\n";
}

void test_weighted_reduction_removes_same_source_zero_delay_shortcut() {
    std::cout << "Testing weighted dependency reduction zero-delay shortcut... ";

    const std::vector<reduction::Edge> edges = {
        {0, 1, 0},
        {0, 2, 0},
        {1, 2, 0},
    };
    const auto result = reduction::reduce(3, edges);
    REQUIRE(result.retained == std::vector<reduction::Edge>({{0, 1, 0}, {1, 2, 0}}));
    REQUIRE(result.removed == std::vector<reduction::Edge>({{0, 2, 0}}));
    requireIrredundant(3, result.retained);

    std::cout << "PASSED\n";
}

void test_weighted_reduction_does_not_batch_remove_mutual_witnesses() {
    std::cout << "Testing weighted dependency reduction mutual witnesses... ";

    // The first two constraints initially witness each other through the
    // zero-delay 1<->2 cycle. Sequential removal may remove one, never both.
    const std::vector<reduction::Edge> edges = {
        {0, 1, 1},
        {0, 2, 1},
        {1, 2, 0},
        {2, 1, 0},
    };
    const auto result = reduction::reduce(3, edges);
    REQUIRE((result.removed == std::vector<reduction::Edge>{{0, 1, 1}}));
    REQUIRE(result.retained == std::vector<reduction::Edge>({{0, 2, 1}, {1, 2, 0}, {2, 1, 0}}));
    REQUIRE(reduction::closure(3, edges) == reduction::closure(3, result.retained));

    std::cout << "PASSED\n";
}

void test_weighted_reduction_uses_saturating_distance() {
    std::cout << "Testing weighted dependency reduction overflow handling... ";

    constexpr uint64_t max = std::numeric_limits<uint64_t>::max();
    REQUIRE(reduction::saturatingDistanceAdd(max - 3, 4) == max);
    REQUIRE(reduction::saturatingDistanceAdd(max - 3, 3) == max);
    REQUIRE(reduction::saturatingDistanceAdd(7, 9) == 16);

    const std::vector<reduction::Edge> edges = {
        {0, 1, max - 4},
        {1, 2, 10},
        {0, 2, max - 1},
    };
    const auto result = reduction::reduce(3, edges);
    REQUIRE(result.removed.empty());
    REQUIRE(result.retained.size() == edges.size());

    std::cout << "PASSED\n";
}

void test_weighted_reduction_rejects_invalid_edges() {
    std::cout << "Testing weighted dependency reduction input validation... ";

    bool out_of_range_rejected = false;
    try {
        const std::vector<reduction::Edge> edges = {{0, 3, 1}};
        (void)reduction::reduce(3, edges);
    } catch (const std::out_of_range&) {
        out_of_range_rejected = true;
    }
    REQUIRE(out_of_range_rejected);

    bool self_edge_rejected = false;
    try {
        const std::vector<reduction::Edge> edges = {{1, 1, 0}};
        (void)reduction::reduce(3, edges);
    } catch (const std::invalid_argument&) {
        self_edge_rejected = true;
    }
    REQUIRE(self_edge_rejected);

    std::cout << "PASSED\n";
}

void test_weighted_reduction_is_deterministic_and_pair_minimal() {
    std::cout << "Testing weighted dependency reduction ordering/pair-min... ";

    const std::vector<reduction::Edge> first = {
        {0, 2, 7}, {2, 3, 1}, {0, 1, 2}, {1, 3, 2}, {0, 3, 9}, {0, 2, 3},
    };
    const std::vector<reduction::Edge> reversed(first.rbegin(), first.rend());
    const auto a = reduction::reduce(4, first);
    const auto b = reduction::reduce(4, reversed);
    REQUIRE(a.retained == b.retained);
    REQUIRE(a.removed == b.removed);
    REQUIRE(a.fan_in_before == b.fan_in_before);
    REQUIRE(a.fan_in_after == b.fan_in_after);
    REQUIRE(a.fan_in_before[0] == 3);
    REQUIRE(reduction::closure(4, first) == reduction::closure(4, a.retained));

    std::cout << "PASSED\n";
}

void test_weighted_reduction_random_closure_equivalence() {
    std::cout << "Testing weighted dependency reduction random closures... ";

    std::mt19937_64 rng(0x8d4e'13c2'79a5'6b01ULL);
    for (size_t iteration = 0; iteration < 500; ++iteration) {
        const size_t clusters = 2 + static_cast<size_t>(rng() % 7);
        std::vector<reduction::Edge> edges;
        for (size_t dependent = 0; dependent < clusters; ++dependent) {
            for (size_t predecessor = 0; predecessor < clusters; ++predecessor) {
                if (dependent == predecessor || rng() % 100 >= 45) continue;
                edges.push_back({dependent, predecessor, rng() % 21});
            }
        }

        const auto result = reduction::reduce(clusters, edges);
        REQUIRE(reduction::closure(clusters, edges) ==
                reduction::closure(clusters, result.retained));
        REQUIRE(result.retained.size() + result.removed.size() == edges.size());
        requireIrredundant(clusters, result.retained);
    }

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== DependencyGraph Tests ===\n\n";

    test_directed_graph_basic();
    test_floyd_warshall();
    test_tarjan_scc();
    test_johnson_cycles();
    test_johnson_tight_cycle();
    test_topological_sort_acyclic();
    test_topological_sort_cyclic();
    test_independent_subgraphs();
    test_multiple_cycles();
    test_self_loop();
    test_weighted_reduction_respects_delay_constraints();
    test_weighted_reduction_removes_same_source_zero_delay_shortcut();
    test_weighted_reduction_does_not_batch_remove_mutual_witnesses();
    test_weighted_reduction_uses_saturating_distance();
    test_weighted_reduction_rejects_invalid_edges();
    test_weighted_reduction_is_deterministic_and_pair_minimal();
    test_weighted_reduction_random_closure_equivalence();

    std::cout << "\n=== All DependencyGraph tests PASSED ===\n";
    return 0;
}
