// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <map>
#include <string>
#include <vector>

#include "../port/Connection.hpp"
#include "../util/Graph.hpp"

namespace chronon::sender {

class Unit;

/** @brief Unit-level dependency graph derived from port connections; nodes are Units, edge weights
 * are delays. */
class DependencyGraph {
public:
    DependencyGraph() = default;

    void build(const std::vector<Unit*>& units, const std::vector<ConnectionBase*>& connections) {
        units_ = units;
        unit_to_index_.clear();

        for (size_t i = 0; i < units.size(); ++i) {
            unit_to_index_[units[i]] = i;
        }

        graph_ = std::make_unique<DirectedGraph>(units.size());

        for (const auto* conn : connections) {
            Unit* src = conn->source();
            Unit* dst = conn->destination();

            if (src && dst) {
                size_t src_idx = unit_to_index_[src];
                size_t dst_idx = unit_to_index_[dst];
                graph_->addEdge(src_idx, dst_idx, conn->delay());
            }
        }

        computeLookahead();
    }

    /// Call recomputeLookahead() after adding all connections.
    void addConnection(Unit* source, Unit* dest, uint32_t delay) {
        auto src_it = unit_to_index_.find(source);
        auto dst_it = unit_to_index_.find(dest);

        if (src_it == unit_to_index_.end() || dst_it == unit_to_index_.end()) {
            return;
        }

        graph_->addEdge(src_it->second, dst_it->second, delay);
    }

    void recomputeLookahead() { computeLookahead(); }

    /**
     * @brief Minimum delay on the shortest path from @p source to @p dest, or INF if no path.
     *
     * If lookahead(A, B) = 5, then B (the consumer) may advance up to A.current + 5,
     * i.e. B can run at most (5 - 1) cycles ahead of A before having to wait.
     */
    uint32_t lookahead(Unit* source, Unit* dest) const {
        auto src_it = unit_to_index_.find(source);
        auto dst_it = unit_to_index_.find(dest);

        if (src_it == unit_to_index_.end() || dst_it == unit_to_index_.end()) {
            return DirectedGraph::INF;
        }

        return distances_[src_it->second][dst_it->second];
    }

    uint32_t lookahead(size_t src_idx, size_t dst_idx) const {
        if (src_idx >= distances_.size() || dst_idx >= distances_.size()) {
            return DirectedGraph::INF;
        }
        return distances_[src_idx][dst_idx];
    }

    bool hasPath(Unit* source, Unit* dest) const {
        return lookahead(source, dest) < DirectedGraph::INF;
    }

    /// All units transitively reachable to @p unit (predecessors in any path).
    std::vector<Unit*> getDependencies(Unit* unit) const {
        std::vector<Unit*> result;
        auto it = unit_to_index_.find(unit);
        if (it == unit_to_index_.end()) return result;

        size_t idx = it->second;
        for (size_t i = 0; i < units_.size(); ++i) {
            if (i != idx && distances_[i][idx] < DirectedGraph::INF) {
                result.push_back(units_[i]);
            }
        }
        return result;
    }

    /// All units transitively reachable from @p unit (successors in any path).
    std::vector<Unit*> getDependents(Unit* unit) const {
        std::vector<Unit*> result;
        auto it = unit_to_index_.find(unit);
        if (it == unit_to_index_.end()) return result;

        size_t idx = it->second;
        for (size_t i = 0; i < units_.size(); ++i) {
            if (i != idx && distances_[idx][i] < DirectedGraph::INF) {
                result.push_back(units_[i]);
            }
        }
        return result;
    }

    /// Direct predecessors (units that send to @p unit) with edge delays.
    std::vector<std::pair<Unit*, uint32_t>> predecessors(Unit* unit) const {
        std::vector<std::pair<Unit*, uint32_t>> result;
        auto it = unit_to_index_.find(unit);
        if (it == unit_to_index_.end()) return result;

        for (const auto& e : graph_->predecessors(it->second)) {
            result.emplace_back(units_[e.to], e.weight);
        }
        return result;
    }

    /// Direct successors (units @p unit sends to) with edge delays.
    std::vector<std::pair<Unit*, uint32_t>> successors(Unit* unit) const {
        std::vector<std::pair<Unit*, uint32_t>> result;
        auto it = unit_to_index_.find(unit);
        if (it == unit_to_index_.end()) return result;

        for (const auto& e : graph_->neighbors(it->second)) {
            result.emplace_back(units_[e.to], e.weight);
        }
        return result;
    }

    /// Required VersionedRegister depth for a single-writer / multi-reader
    /// pattern.  The writer can advance at most distances[reader][writer]
    /// cycles ahead of each reader; the buffer must retain that many versions
    /// plus one baseline so the slowest reader always finds a correct value.
    ///
    /// When the graph has a finite path from reader to writer, the raw graph
    /// distance is used (not capped).  When no such path exists, the writer's
    /// advance is bounded only by @p max_lookahead_cycles — the global skew
    /// limit that the scheduler must enforce.
    uint32_t requiredVersionedRegisterDepth(Unit* writer, const std::vector<Unit*>& readers,
                                            uint32_t max_lookahead_cycles) const {
        auto w_it = unit_to_index_.find(writer);
        if (w_it == unit_to_index_.end()) return max_lookahead_cycles + 1;
        size_t w = w_it->second;

        uint32_t max_skew = 0;
        for (Unit* reader : readers) {
            if (reader == writer) continue;
            auto r_it = unit_to_index_.find(reader);
            if (r_it == unit_to_index_.end()) {
                max_skew = std::max(max_skew, max_lookahead_cycles);
                continue;
            }
            uint32_t d = distances_[r_it->second][w];
            uint32_t skew = (d < DirectedGraph::INF) ? d : max_lookahead_cycles;
            max_skew = std::max(max_skew, skew);
        }
        return std::max(max_skew + 1, uint32_t(2));
    }

    const DirectedGraph* graph() const { return graph_.get(); }
    const std::vector<Unit*>& units() const { return units_; }
    size_t numUnits() const { return units_.size(); }

    size_t unitIndex(Unit* unit) const {
        auto it = unit_to_index_.find(unit);
        return (it != unit_to_index_.end()) ? it->second : SIZE_MAX;
    }

    Unit* unitAt(size_t index) const { return (index < units_.size()) ? units_[index] : nullptr; }

    const std::vector<std::vector<uint32_t>>& distances() const { return distances_; }

private:
    void computeLookahead() {
        if (graph_) {
            distances_ = floydWarshall(*graph_);
        }
    }

    std::vector<Unit*> units_;
    std::map<Unit*, size_t> unit_to_index_;
    std::unique_ptr<DirectedGraph> graph_;
    std::vector<std::vector<uint32_t>> distances_;
};

}  // namespace chronon::sender
