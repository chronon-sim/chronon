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

}  // namespace detail

/// Greedily remove progress constraints implied by an alternate path of equal
/// or smaller total delay. Each dependent cluster uses one incremental
/// shortest-path scan rather than restarting Dijkstra for every candidate.
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

    using QueueEntry = std::pair<uint64_t, size_t>;
    size_t group_begin = 0;
    while (group_begin < edges.size()) {
        const size_t source = edges[group_begin].dependent;
        size_t group_end = group_begin + 1;
        while (group_end < edges.size() && edges[group_end].dependent == source) {
            ++group_end;
        }

        // A path witnessing a direct source edge can be chosen without
        // revisiting source, so exclude the whole source group and seed only
        // the direct edges retained during this scan.
        for (size_t i = group_begin; i < group_end; ++i) active[i] = false;

        std::vector<size_t> candidates;
        candidates.reserve(group_end - group_begin);
        for (size_t i = group_begin; i < group_end; ++i) candidates.push_back(i);
        std::sort(candidates.begin(), candidates.end(), [&](size_t lhs, size_t rhs) {
            if (edges[lhs].delay != edges[rhs].delay) {
                return edges[lhs].delay < edges[rhs].delay;
            }
            // For equal delays, prefer the later normalized edge. This makes
            // zero-delay mutual witnesses deterministic without batch removal.
            return lhs > rhs;
        });

        std::vector<uint64_t> distance(num_clusters, kInfiniteDistance);
        std::priority_queue<QueueEntry, std::vector<QueueEntry>, std::greater<QueueEntry>> ready;
        distance[source] = 0;
        ready.emplace(0, source);

        auto settleThrough = [&](uint64_t limit) {
            while (!ready.empty() && ready.top().first <= limit) {
                const auto [current_distance, cluster] = ready.top();
                ready.pop();
                if (current_distance != distance[cluster]) continue;

                for (size_t edge_index : adjacency[cluster]) {
                    if (!active[edge_index]) continue;
                    const Edge& edge = edges[edge_index];
                    const uint64_t next = saturatingDistanceAdd(current_distance, edge.delay);
                    if (next >= distance[edge.predecessor]) continue;
                    distance[edge.predecessor] = next;
                    ready.emplace(next, edge.predecessor);
                }
            }
        };

        for (size_t edge_index : candidates) {
            const Edge& candidate = edges[edge_index];
            settleThrough(candidate.delay);
            if (distance[candidate.predecessor] <= candidate.delay) continue;

            active[edge_index] = true;
            distance[candidate.predecessor] = candidate.delay;
            ready.emplace(candidate.delay, candidate.predecessor);
        }

        group_begin = group_end;
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
