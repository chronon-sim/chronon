// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <cstdlib>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "TickSimulation.hpp"

namespace chronon::sender {

size_t TickSimulation::optimizeTransparentBroadcasts_() {
    const char* enabled = std::getenv("CHRONON_EXPERIMENTAL_TRANSPARENT_BROADCAST");
    if (enabled && enabled[0] == '0' && enabled[1] == '\0') return 0;
    if (connections_.empty()) return 0;

    // Optimize whole connected Port components, never isolated edges. This
    // guarantees that an InPort cannot end up merging a shared lane with a
    // conventional queue and that fallback remains all-or-nothing.
    std::unordered_map<void*, size_t> source_nodes;
    std::unordered_map<void*, size_t> destination_nodes;
    std::vector<size_t> parent;

    auto add_node = [&](auto& nodes, void* key) {
        auto [it, inserted] = nodes.try_emplace(key, parent.size());
        if (inserted) parent.push_back(it->second);
        return it->second;
    };
    auto find_root = [&](size_t node) {
        size_t root = node;
        while (parent[root] != root) root = parent[root];
        while (parent[node] != node) {
            const size_t next = parent[node];
            parent[node] = root;
            node = next;
        }
        return root;
    };
    auto unite = [&](size_t lhs, size_t rhs) {
        lhs = find_root(lhs);
        rhs = find_root(rhs);
        if (lhs != rhs) parent[rhs] = lhs;
    };

    for (auto* connection : connections_) {
        const size_t source = add_node(source_nodes, connection->sourcePortPtr());
        const size_t destination = add_node(destination_nodes, connection->destPortPtr());
        unite(source, destination);
    }

    std::unordered_map<size_t, std::vector<ConnectionBase*>> components;
    for (auto* connection : connections_) {
        const size_t node = source_nodes.at(connection->sourcePortPtr());
        components[find_root(node)].push_back(connection);
    }

    constexpr size_t kMinimumFanout = 4;
    constexpr size_t kHeadroomCycles = 512;
    size_t optimized_connections = 0;
    for (auto& [root, component] : components) {
        (void)root;
        std::unordered_map<void*, size_t> source_degrees;
        bool eligible = true;
        for (auto* connection : component) {
            ++source_degrees[connection->sourcePortPtr()];
            if (!connection->transparentBroadcastEligible(kHeadroomCycles)) eligible = false;
        }
        for (const auto& [source, degree] : source_degrees) {
            (void)source;
            if (degree < kMinimumFanout) eligible = false;
        }
        if (!eligible) continue;

        std::unordered_set<void*> enabled_sources;
        for (auto* connection : component) {
            if (!enabled_sources.insert(connection->sourcePortPtr()).second) continue;
            if (!connection->enableTransparentBroadcastForSource(kHeadroomCycles)) {
                throw std::logic_error("transparent broadcast component failed to initialize");
            }
        }
        optimized_connections += component.size();
    }
    return optimized_connections;
}

}  // namespace chronon::sender
