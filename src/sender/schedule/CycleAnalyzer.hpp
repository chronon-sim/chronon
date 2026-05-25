// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <map>
#include <set>
#include <vector>

#include "../util/Graph.hpp"
#include "DependencyGraph.hpp"

namespace chronon::sender {

/** @brief A detected cycle with edge delays and total weight. */
struct CycleInfo {
    std::vector<Unit*> units;
    std::vector<uint32_t> delays;
    uint32_t total_delay;

    /// True for zero-delay cycles, which require delta-cycle execution (no lookahead buffer).
    bool isTight() const { return total_delay == 0; }

    uint32_t minEdgeDelay() const {
        if (delays.empty()) return 0;
        return *std::min_element(delays.begin(), delays.end());
    }

    bool contains(Unit* unit) const {
        return std::find(units.begin(), units.end(), unit) != units.end();
    }
};

/** @brief Full cycle/dependency analysis output for a DependencyGraph. */
struct AnalysisResult {
    std::vector<CycleInfo> all_cycles;
    std::vector<CycleInfo> tight_cycles;  ///< delay = 0; require delta cycles.
    std::vector<CycleInfo> loose_cycles;  ///< delay > 0; can use lookahead.

    std::set<Unit*> tight_cycle_units;

    std::vector<std::vector<Unit*>> independent_groups;
    std::vector<std::vector<Unit*>> sccs;

    /// All-pairs transitive shortest path (Floyd-Warshall closure). Diagnostic / reachability.
    std::map<std::pair<Unit*, Unit*>, uint32_t> lookahead;

    /**
     * Direct (uncomposed) edges from the underlying connection graph.
     *
     * Schedulers should prefer this over @ref lookahead when computing per-cycle safe
     * boundaries: transitive constraints propagate naturally through the direct
     * predecessor chain, so iterating only direct edges is strictly sufficient and
     * admits strictly more parallelism than iterating the Floyd-Warshall closure.
     */
    std::map<std::pair<Unit*, Unit*>, uint32_t> direct_edges;

    bool inTightCycle(Unit* unit) const { return tight_cycle_units.count(unit) > 0; }

    /// Returns DirectedGraph::INF if @p source has no path to @p dest.
    uint32_t safeLookahead(Unit* source, Unit* dest) const {
        auto it = lookahead.find({source, dest});
        if (it != lookahead.end()) {
            return it->second;
        }
        return DirectedGraph::INF;
    }

    /// False iff @p a and @p b share a tight cycle (or are the same unit).
    bool canParallelize(Unit* a, Unit* b) const {
        if (a == b) return false;

        for (const auto& cycle : tight_cycles) {
            if (cycle.contains(a) && cycle.contains(b)) {
                return false;
            }
        }

        return true;
    }
};

/** @brief Runs Tarjan SCC + Johnson cycles + Floyd-Warshall lookahead on a DependencyGraph. */
class CycleAnalyzer {
public:
    /// @p max_cycles caps Johnson enumeration (0 = all).
    static AnalysisResult analyze(const DependencyGraph& dep_graph, size_t max_cycles = 1000) {
        AnalysisResult result;

        const auto* graph = dep_graph.graph();
        if (!graph || graph->numNodes() == 0) {
            return result;
        }

        auto scc = tarjanSCC(*graph);
        for (const auto& component : scc.components) {
            std::vector<Unit*> units;
            for (size_t idx : component) {
                units.push_back(dep_graph.unitAt(idx));
            }
            result.sccs.push_back(std::move(units));
        }

        auto cycles = johnsonAllCycles(*graph, max_cycles);

        for (const auto& cycle : cycles) {
            CycleInfo info;
            for (size_t idx : cycle.nodes) {
                info.units.push_back(dep_graph.unitAt(idx));
            }

            for (size_t i = 0; i < cycle.nodes.size(); ++i) {
                size_t from = cycle.nodes[i];
                size_t to = cycle.nodes[(i + 1) % cycle.nodes.size()];
                uint32_t delay = graph->edgeWeight(from, to);
                info.delays.push_back(delay);
            }

            info.total_delay = cycle.total_weight;
            result.all_cycles.push_back(info);

            if (info.isTight()) {
                result.tight_cycles.push_back(info);
                for (Unit* unit : info.units) {
                    result.tight_cycle_units.insert(unit);
                }
            } else {
                result.loose_cycles.push_back(info);
            }
        }

        auto groups = findIndependentSubgraphs(*graph);
        for (const auto& group : groups) {
            std::vector<Unit*> units;
            for (size_t idx : group) {
                units.push_back(dep_graph.unitAt(idx));
            }
            result.independent_groups.push_back(std::move(units));
        }

        const auto& distances = dep_graph.distances();
        for (size_t i = 0; i < dep_graph.numUnits(); ++i) {
            for (size_t j = 0; j < dep_graph.numUnits(); ++j) {
                if (i != j && distances[i][j] < DirectedGraph::INF) {
                    result.lookahead[{dep_graph.unitAt(i), dep_graph.unitAt(j)}] = distances[i][j];
                }
            }
        }

        // Walk adjacency lists, not the distance matrix, so we capture only direct
        // connection delays — not transitive shortest-path combinations.
        for (size_t i = 0; i < dep_graph.numUnits(); ++i) {
            for (const auto& e : graph->neighbors(i)) {
                Unit* src = dep_graph.unitAt(i);
                Unit* dst = dep_graph.unitAt(e.to);
                if (src && dst) {
                    auto key = std::make_pair(src, dst);
                    auto it = result.direct_edges.find(key);
                    if (it == result.direct_edges.end() || e.weight < it->second) {
                        result.direct_edges[key] = e.weight;
                    }
                }
            }
        }

        return result;
    }

    /// True iff @p unit has a self-loop (feedback within itself).
    static bool hasSelfLoop(const DependencyGraph& dep_graph, Unit* unit) {
        size_t idx = dep_graph.unitIndex(unit);
        if (idx == SIZE_MAX) return false;

        const auto* graph = dep_graph.graph();
        return graph && graph->hasEdge(idx, idx);
    }

    /// Returns 0 if in a tight cycle, INF if not in any cycle.
    static uint32_t minCycleLength(const DependencyGraph& dep_graph, Unit* unit) {
        size_t idx = dep_graph.unitIndex(unit);
        if (idx == SIZE_MAX) return DirectedGraph::INF;

        const auto* graph = dep_graph.graph();
        if (!graph) return DirectedGraph::INF;

        uint32_t min_cycle = DirectedGraph::INF;

        const auto& distances = dep_graph.distances();
        for (const auto& e : graph->neighbors(idx)) {
            if (distances[e.to][idx] < DirectedGraph::INF) {
                uint32_t cycle_len = e.weight + distances[e.to][idx];
                min_cycle = std::min(min_cycle, cycle_len);
            }
        }

        return min_cycle;
    }
};

}  // namespace chronon::sender
