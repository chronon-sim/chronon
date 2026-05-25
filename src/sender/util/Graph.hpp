// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <stack>
#include <unordered_map>
#include <vector>

namespace chronon::sender {

/** @brief Directed graph with weighted edges; weights encode communication delay in cycles. */
class DirectedGraph {
public:
    static constexpr uint32_t INF = std::numeric_limits<uint32_t>::max() / 2;

    explicit DirectedGraph(size_t num_nodes)
        : num_nodes_(num_nodes), adj_(num_nodes), rev_adj_(num_nodes) {}

    void addEdge(size_t from, size_t to, uint32_t weight = 1) {
        adj_[from].push_back({to, weight});
        rev_adj_[to].push_back({from, weight});
    }

    struct Edge {
        size_t to;
        uint32_t weight;
    };

    const std::vector<Edge>& neighbors(size_t node) const { return adj_[node]; }
    const std::vector<Edge>& predecessors(size_t node) const { return rev_adj_[node]; }

    size_t numNodes() const noexcept { return num_nodes_; }
    size_t numEdges() const {
        size_t count = 0;
        for (const auto& edges : adj_) {
            count += edges.size();
        }
        return count;
    }

    bool hasEdge(size_t from, size_t to) const {
        for (const auto& e : adj_[from]) {
            if (e.to == to) return true;
        }
        return false;
    }

    /// Returns INF if no edge exists.
    uint32_t edgeWeight(size_t from, size_t to) const {
        for (const auto& e : adj_[from]) {
            if (e.to == to) return e.weight;
        }
        return INF;
    }

private:
    size_t num_nodes_;
    std::vector<std::vector<Edge>> adj_;
    std::vector<std::vector<Edge>> rev_adj_;
};

/// All-pairs shortest paths via Floyd-Warshall. Time O(V^3), space O(V^2).
inline std::vector<std::vector<uint32_t>> floydWarshall(const DirectedGraph& graph) {
    const size_t n = graph.numNodes();
    const uint32_t INF = DirectedGraph::INF;

    std::vector<std::vector<uint32_t>> dist(n, std::vector<uint32_t>(n, INF));

    for (size_t i = 0; i < n; ++i) {
        dist[i][i] = 0;
    }

    for (size_t u = 0; u < n; ++u) {
        for (const auto& e : graph.neighbors(u)) {
            dist[u][e.to] = std::min(dist[u][e.to], e.weight);
        }
    }

    for (size_t k = 0; k < n; ++k) {
        for (size_t i = 0; i < n; ++i) {
            if (dist[i][k] >= INF) continue;
            for (size_t j = 0; j < n; ++j) {
                if (dist[k][j] >= INF) continue;
                uint32_t through_k = dist[i][k] + dist[k][j];
                if (through_k < dist[i][j]) {
                    dist[i][j] = through_k;
                }
            }
        }
    }

    return dist;
}

/** @brief SCC decomposition produced by tarjanSCC(). */
struct SCCResult {
    std::vector<size_t> component;                ///< SCC index for each node.
    std::vector<std::vector<size_t>> components;  ///< Nodes in each SCC.

    size_t numComponents() const { return components.size(); }

    /// True if @p node belongs to a non-trivial SCC (size > 1).
    bool inCycle(size_t node) const { return components[component[node]].size() > 1; }
};

/// Strongly connected components via Tarjan. Time O(V + E), space O(V).
inline SCCResult tarjanSCC(const DirectedGraph& graph) {
    const size_t n = graph.numNodes();
    SCCResult result;
    result.component.resize(n, SIZE_MAX);

    std::vector<size_t> index(n, SIZE_MAX);
    std::vector<size_t> lowlink(n);
    std::vector<bool> on_stack(n, false);
    std::stack<size_t> stack;
    size_t current_index = 0;

    std::function<void(size_t)> strongconnect = [&](size_t v) {
        index[v] = lowlink[v] = current_index++;
        stack.push(v);
        on_stack[v] = true;

        for (const auto& e : graph.neighbors(v)) {
            size_t w = e.to;
            if (index[w] == SIZE_MAX) {
                strongconnect(w);
                lowlink[v] = std::min(lowlink[v], lowlink[w]);
            } else if (on_stack[w]) {
                lowlink[v] = std::min(lowlink[v], index[w]);
            }
        }

        if (lowlink[v] == index[v]) {
            std::vector<size_t> component;
            size_t w;
            do {
                w = stack.top();
                stack.pop();
                on_stack[w] = false;
                result.component[w] = result.components.size();
                component.push_back(w);
            } while (w != v);
            result.components.push_back(std::move(component));
        }
    };

    for (size_t v = 0; v < n; ++v) {
        if (index[v] == SIZE_MAX) {
            strongconnect(v);
        }
    }

    return result;
}

/** @brief A simple cycle in the graph with summed edge weight. */
struct SimpleCycle {
    std::vector<size_t> nodes;  ///< Nodes in cycle order.
    uint32_t total_weight;      ///< Sum of edge weights.

    bool operator<(const SimpleCycle& other) const { return total_weight < other.total_weight; }

    bool isTight() const { return total_weight == 0; }
};

/// All simple cycles via Johnson. @p max_cycles caps results (0 = unlimited).
inline std::vector<SimpleCycle> johnsonAllCycles(const DirectedGraph& graph,
                                                 size_t max_cycles = 0) {
    const size_t n = graph.numNodes();
    std::vector<SimpleCycle> cycles;

    std::vector<bool> blocked(n, false);
    std::vector<std::vector<size_t>> B(n);  // Blocked nodes to unblock
    std::vector<size_t> stack;
    std::vector<uint32_t> weights;  // Weights along current path

    std::function<bool(size_t, size_t)> circuit = [&](size_t v, size_t s) -> bool {
        bool found = false;
        stack.push_back(v);
        blocked[v] = true;

        for (const auto& e : graph.neighbors(v)) {
            size_t w = e.to;
            weights.push_back(e.weight);

            if (w == s) {
                SimpleCycle cycle;
                cycle.nodes = stack;
                cycle.total_weight = 0;
                for (uint32_t wt : weights) {
                    cycle.total_weight += wt;
                }
                cycles.push_back(std::move(cycle));
                found = true;

                if (max_cycles > 0 && cycles.size() >= max_cycles) {
                    weights.pop_back();
                    stack.pop_back();
                    return true;
                }
            } else if (!blocked[w]) {
                if (circuit(w, s)) {
                    weights.pop_back();
                    stack.pop_back();
                    return true;
                }
                found = found || !blocked[w];
            }

            weights.pop_back();
        }

        std::function<void(size_t)> unblock = [&](size_t u) {
            blocked[u] = false;
            for (size_t w : B[u]) {
                if (blocked[w]) {
                    unblock(w);
                }
            }
            B[u].clear();
        };

        if (found) {
            unblock(v);
        } else {
            for (const auto& e : graph.neighbors(v)) {
                B[e.to].push_back(v);
            }
        }

        stack.pop_back();
        return false;
    };

    for (size_t s = 0; s < n; ++s) {
        circuit(s, s);

        if (max_cycles > 0 && cycles.size() >= max_cycles) {
            break;
        }

        std::fill(blocked.begin(), blocked.end(), false);
        for (auto& b : B) {
            b.clear();
        }
    }

    return cycles;
}

/** @brief Result of topologicalSort(); order is partial if has_cycle is true. */
struct TopoSortResult {
    std::vector<size_t> order;
    bool has_cycle;
};

/// Topological sort via Kahn's algorithm. Time O(V + E), space O(V).
inline TopoSortResult topologicalSort(const DirectedGraph& graph) {
    const size_t n = graph.numNodes();
    TopoSortResult result;

    std::vector<size_t> in_degree(n, 0);
    for (size_t u = 0; u < n; ++u) {
        for (const auto& e : graph.neighbors(u)) {
            in_degree[e.to]++;
        }
    }

    std::vector<size_t> queue;
    for (size_t u = 0; u < n; ++u) {
        if (in_degree[u] == 0) {
            queue.push_back(u);
        }
    }

    size_t idx = 0;
    while (idx < queue.size()) {
        size_t u = queue[idx++];
        result.order.push_back(u);

        for (const auto& e : graph.neighbors(u)) {
            if (--in_degree[e.to] == 0) {
                queue.push_back(e.to);
            }
        }
    }

    result.has_cycle = result.order.size() != n;
    return result;
}

/// Group nodes by undirected connectivity into independent subgraphs.
inline std::vector<std::vector<size_t>> findIndependentSubgraphs(const DirectedGraph& graph) {
    const size_t n = graph.numNodes();
    std::vector<size_t> component(n, SIZE_MAX);
    std::vector<std::vector<size_t>> result;

    for (size_t start = 0; start < n; ++start) {
        if (component[start] != SIZE_MAX) continue;

        std::vector<size_t> group;
        std::vector<size_t> queue;
        queue.push_back(start);
        component[start] = result.size();

        size_t idx = 0;
        while (idx < queue.size()) {
            size_t u = queue[idx++];
            group.push_back(u);

            for (const auto& e : graph.neighbors(u)) {
                if (component[e.to] == SIZE_MAX) {
                    component[e.to] = result.size();
                    queue.push_back(e.to);
                }
            }

            for (const auto& e : graph.predecessors(u)) {
                if (component[e.to] == SIZE_MAX) {
                    component[e.to] = result.size();
                    queue.push_back(e.to);
                }
            }
        }

        result.push_back(std::move(group));
    }

    return result;
}

/**
 * @brief Tight-coupling cluster assignment from findTightCouplingClusters().
 *
 * Units connected by delay=0 edges share a cluster and must execute on the same thread.
 */
struct TightCouplingResult {
    std::vector<size_t> cluster_id;
    std::vector<std::vector<size_t>> clusters;

    size_t numClusters() const { return clusters.size(); }
    bool sameCluster(size_t a, size_t b) const { return cluster_id[a] == cluster_id[b]; }
    const std::vector<size_t>& getCluster(size_t node) const { return clusters[cluster_id[node]]; }
};

/// Union-Find clustering over zero-weight edges. Time O(E·α(V)), space O(V).
inline TightCouplingResult findTightCouplingClusters(const DirectedGraph& graph) {
    const size_t n = graph.numNodes();

    std::vector<size_t> parent(n);
    std::vector<size_t> rank(n, 0);

    for (size_t i = 0; i < n; ++i) {
        parent[i] = i;
    }

    std::function<size_t(size_t)> find = [&](size_t x) -> size_t {
        if (parent[x] != x) {
            parent[x] = find(parent[x]);
        }
        return parent[x];
    };

    auto unite = [&](size_t x, size_t y) {
        size_t px = find(x);
        size_t py = find(y);
        if (px == py) return;

        if (rank[px] < rank[py]) {
            parent[px] = py;
        } else if (rank[px] > rank[py]) {
            parent[py] = px;
        } else {
            parent[py] = px;
            rank[px]++;
        }
    };

    for (size_t u = 0; u < n; ++u) {
        for (const auto& e : graph.neighbors(u)) {
            if (e.weight == 0) {
                unite(u, e.to);
            }
        }
    }

    TightCouplingResult result;
    result.cluster_id.resize(n);

    std::unordered_map<size_t, size_t> root_to_cluster;

    for (size_t i = 0; i < n; ++i) {
        size_t root = find(i);
        auto it = root_to_cluster.find(root);
        if (it == root_to_cluster.end()) {
            size_t cluster_idx = result.clusters.size();
            root_to_cluster[root] = cluster_idx;
            result.clusters.push_back({i});
            result.cluster_id[i] = cluster_idx;
        } else {
            result.cluster_id[i] = it->second;
            result.clusters[it->second].push_back(i);
        }
    }

    return result;
}

}  // namespace chronon::sender
