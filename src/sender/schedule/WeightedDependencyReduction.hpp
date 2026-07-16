// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <queue>
#include <span>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace chronon::sender::weighted_dependency_reduction {

inline constexpr uint64_t kInfiniteDistance = std::numeric_limits<uint64_t>::max();

struct Edge {
    size_t dependent = 0;
    size_t predecessor = 0;
    uint64_t delay = 0;

    bool operator==(const Edge&) const = default;
};

struct Result {
    std::vector<Edge> retained;
    std::vector<Edge> removed;
    std::vector<size_t> fan_in_before;
    std::vector<size_t> fan_in_after;
};

inline uint64_t saturatingDistanceAdd(uint64_t lhs, uint64_t rhs) noexcept {
    if (lhs == kInfiniteDistance || rhs > kInfiniteDistance - lhs) {
        return kInfiniteDistance;
    }
    return lhs + rhs;
}

namespace detail {

inline bool edgeLess(const Edge& lhs, const Edge& rhs) noexcept {
    return std::tie(lhs.dependent, lhs.predecessor, lhs.delay) <
           std::tie(rhs.dependent, rhs.predecessor, rhs.delay);
}

inline std::vector<Edge> normalizePairMinimumEdges(size_t num_clusters,
                                                   std::span<const Edge> input) {
    std::vector<Edge> sorted(input.begin(), input.end());
    for (const Edge& edge : sorted) {
        if (edge.dependent >= num_clusters || edge.predecessor >= num_clusters) {
            throw std::out_of_range("weighted dependency edge endpoint out of range");
        }
        if (edge.dependent == edge.predecessor) {
            throw std::invalid_argument(
                "weighted dependency reduction requires cross-cluster edges");
        }
    }
    std::sort(sorted.begin(), sorted.end(), edgeLess);

    std::vector<Edge> normalized;
    normalized.reserve(sorted.size());
    for (const Edge& edge : sorted) {
        if (!normalized.empty() && normalized.back().dependent == edge.dependent &&
            normalized.back().predecessor == edge.predecessor) {
            continue;
        }
        normalized.push_back(edge);
    }
    return normalized;
}

inline std::vector<std::vector<size_t>> buildAdjacency(size_t num_clusters,
                                                       const std::vector<Edge>& edges) {
    std::vector<std::vector<size_t>> adjacency(num_clusters);
    for (size_t i = 0; i < edges.size(); ++i) {
        adjacency[edges[i].dependent].push_back(i);
    }
    return adjacency;
}

inline std::vector<uint64_t> shortestDistances(size_t num_clusters, const std::vector<Edge>& edges,
                                               const std::vector<std::vector<size_t>>& adjacency,
                                               const std::vector<bool>& active, size_t source) {
    using QueueEntry = std::pair<uint64_t, size_t>;
    std::priority_queue<QueueEntry, std::vector<QueueEntry>, std::greater<QueueEntry>> ready;
    std::vector<uint64_t> distance(num_clusters, kInfiniteDistance);
    distance[source] = 0;
    ready.emplace(0, source);

    while (!ready.empty()) {
        const auto [current_distance, cluster] = ready.top();
        ready.pop();
        if (current_distance != distance[cluster]) continue;

        for (size_t edge_index : adjacency[cluster]) {
            if (!active[edge_index]) continue;
            const Edge& edge = edges[edge_index];
            const uint64_t candidate = saturatingDistanceAdd(current_distance, edge.delay);
            if (candidate >= distance[edge.predecessor]) continue;
            distance[edge.predecessor] = candidate;
            ready.emplace(candidate, edge.predecessor);
        }
    }
    return distance;
}

inline uint64_t shortestDistance(size_t num_clusters, const std::vector<Edge>& edges,
                                 const std::vector<std::vector<size_t>>& adjacency,
                                 const std::vector<bool>& active, size_t source, size_t target) {
    using QueueEntry = std::pair<uint64_t, size_t>;
    std::priority_queue<QueueEntry, std::vector<QueueEntry>, std::greater<QueueEntry>> ready;
    std::vector<uint64_t> distance(num_clusters, kInfiniteDistance);
    distance[source] = 0;
    ready.emplace(0, source);

    while (!ready.empty()) {
        const auto [current_distance, cluster] = ready.top();
        ready.pop();
        if (current_distance != distance[cluster]) continue;
        if (cluster == target) return current_distance;

        for (size_t edge_index : adjacency[cluster]) {
            if (!active[edge_index]) continue;
            const Edge& edge = edges[edge_index];
            const uint64_t candidate = saturatingDistanceAdd(current_distance, edge.delay);
            if (candidate >= distance[edge.predecessor]) continue;
            distance[edge.predecessor] = candidate;
            ready.emplace(candidate, edge.predecessor);
        }
    }
    return kInfiniteDistance;
}

}  // namespace detail

/// Greedily remove progress constraints implied by an alternate path of equal
/// or smaller total delay. Candidates are processed in stable order, so an
/// edge removed earlier can never serve as the witness for a later removal.
inline Result reduce(size_t num_clusters, std::span<const Edge> input) {
    const std::vector<Edge> edges = detail::normalizePairMinimumEdges(num_clusters, input);
    const auto adjacency = detail::buildAdjacency(num_clusters, edges);
    std::vector<bool> active(edges.size(), true);

    Result result;
    result.fan_in_before.assign(num_clusters, 0);
    result.fan_in_after.assign(num_clusters, 0);
    result.retained.reserve(edges.size());
    result.removed.reserve(edges.size());
    for (const Edge& edge : edges) {
        ++result.fan_in_before[edge.dependent];
    }

    for (size_t i = 0; i < edges.size(); ++i) {
        const Edge& candidate = edges[i];
        active[i] = false;
        const uint64_t alternate = detail::shortestDistance(
            num_clusters, edges, adjacency, active, candidate.dependent, candidate.predecessor);
        if (alternate > candidate.delay) active[i] = true;
    }

    for (size_t i = 0; i < edges.size(); ++i) {
        if (active[i]) {
            result.retained.push_back(edges[i]);
            ++result.fan_in_after[edges[i].dependent];
        } else {
            result.removed.push_back(edges[i]);
        }
    }
    return result;
}

/// All-pairs weighted closure, exposed for verification and diagnostics.
inline std::vector<std::vector<uint64_t>> closure(size_t num_clusters,
                                                  std::span<const Edge> input) {
    const std::vector<Edge> edges = detail::normalizePairMinimumEdges(num_clusters, input);
    const auto adjacency = detail::buildAdjacency(num_clusters, edges);
    const std::vector<bool> active(edges.size(), true);

    std::vector<std::vector<uint64_t>> distances;
    distances.reserve(num_clusters);
    for (size_t source = 0; source < num_clusters; ++source) {
        distances.push_back(
            detail::shortestDistances(num_clusters, edges, adjacency, active, source));
    }
    return distances;
}

}  // namespace chronon::sender::weighted_dependency_reduction
