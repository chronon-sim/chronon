// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// TickSimulation topology validation helpers.

#include <set>
#include <sstream>
#include <stdexcept>

#include "TickSimulation.hpp"

namespace chronon::sender {

void TickSimulation::validateNoZeroDelayCycles_() const {
    const auto* graph = dep_graph_.graph();
    if (!graph || graph->numNodes() <= 1) {
        if (graph && graph->numNodes() == 1) {
            for (const auto& e : graph->neighbors(0)) {
                if (e.to == 0 && e.weight == 0) {
                    throw std::invalid_argument(
                        "Chronon topology contains a zero-delay cycle: self-loop on unit '" +
                        unit_ptrs_[0]->name() +
                        "'. delay=0 feedback has no well-defined tick() causality; insert a "
                        "registered delay (delay>0) or collapse the combinational logic into one "
                        "unit.");
                }
            }
        }
        return;
    }

    const size_t n = graph->numNodes();
    DirectedGraph zero_delay_graph(n);
    for (size_t u = 0; u < n; ++u) {
        for (const auto& e : graph->neighbors(u)) {
            if (e.weight == 0) {
                zero_delay_graph.addEdge(u, e.to, 0);
            }
        }
    }

    auto sccs = tarjanSCC(zero_delay_graph);
    for (const auto& members : sccs.components) {
        bool has_zero_delay_cycle = members.size() > 1;
        if (!has_zero_delay_cycle && members.size() == 1) {
            const size_t u = members.front();
            for (const auto& e : zero_delay_graph.neighbors(u)) {
                if (e.to == u) {
                    has_zero_delay_cycle = true;
                    break;
                }
            }
        }

        if (!has_zero_delay_cycle) {
            continue;
        }

        std::set<size_t> member_set(members.begin(), members.end());
        std::ostringstream oss;
        oss << "Chronon topology contains a zero-delay cycle among units: ";
        for (size_t i = 0; i < members.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << unit_ptrs_[members[i]]->name();
        }
        oss << ". delay=0 feedback has no well-defined tick() causality; insert a registered "
               "delay (delay>0) on at least one feedback edge or collapse the combinational "
               "logic into one unit. Zero-delay edges in the cycle:";

        bool first_edge = true;
        for (size_t u : members) {
            for (const auto& e : zero_delay_graph.neighbors(u)) {
                if (!member_set.count(e.to)) continue;
                oss << (first_edge ? " " : ", ") << unit_ptrs_[u]->name() << " -> "
                    << unit_ptrs_[e.to]->name();
                first_edge = false;
            }
        }
        oss << '.';
        throw std::invalid_argument(oss.str());
    }
}

}  // namespace chronon::sender
