// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

// Compares the incremental per-dependent scan against the previous per-edge
// Dijkstra implementation. The legacy reference intentionally returns only a
// retained-edge count, giving it a small advantage over the production reducer.
//
// Usage: chronon_weighted_dependency_reduction_benchmark [clusters] [repeats]

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <queue>
#include <span>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "sender/schedule/WeightedDependencyReduction.hpp"

namespace reduction = chronon::sender::weighted_dependency_reduction;

namespace {

using Edge = reduction::Edge;
using Clock = std::chrono::steady_clock;

std::vector<Edge> makeRingGraph(size_t clusters, size_t fanout) {
    fanout = std::min(fanout, clusters - 1);
    std::vector<Edge> edges;
    edges.reserve(clusters * fanout);
    for (size_t source = 0; source < clusters; ++source) {
        for (size_t step = 1; step <= fanout; ++step) {
            edges.push_back({source, (source + step) % clusters, step});
        }
    }
    return edges;
}

size_t legacyRetainedCount(size_t clusters, std::span<const Edge> input) {
    std::vector<Edge> edges(input.begin(), input.end());
    std::sort(edges.begin(), edges.end(), [](const Edge& lhs, const Edge& rhs) {
        return std::tie(lhs.dependent, lhs.predecessor, lhs.delay) <
               std::tie(rhs.dependent, rhs.predecessor, rhs.delay);
    });

    std::vector<std::vector<size_t>> adjacency(clusters);
    for (size_t i = 0; i < edges.size(); ++i) adjacency[edges[i].dependent].push_back(i);
    std::vector<bool> active(edges.size(), true);

    using QueueEntry = std::pair<uint64_t, size_t>;
    for (size_t candidate_index = 0; candidate_index < edges.size(); ++candidate_index) {
        const Edge& candidate = edges[candidate_index];
        active[candidate_index] = false;

        std::vector<uint64_t> distance(clusters, reduction::kInfiniteDistance);
        std::priority_queue<QueueEntry, std::vector<QueueEntry>, std::greater<QueueEntry>> ready;
        distance[candidate.dependent] = 0;
        ready.emplace(0, candidate.dependent);

        uint64_t alternate = reduction::kInfiniteDistance;
        while (!ready.empty()) {
            const auto [current_distance, cluster] = ready.top();
            ready.pop();
            if (current_distance != distance[cluster]) continue;
            if (cluster == candidate.predecessor) {
                alternate = current_distance;
                break;
            }

            for (size_t edge_index : adjacency[cluster]) {
                if (!active[edge_index]) continue;
                const Edge& edge = edges[edge_index];
                const uint64_t next =
                    reduction::saturatingDistanceAdd(current_distance, edge.delay);
                if (next >= distance[edge.predecessor]) continue;
                distance[edge.predecessor] = next;
                ready.emplace(next, edge.predecessor);
            }
        }

        if (alternate > candidate.delay) active[candidate_index] = true;
    }

    return static_cast<size_t>(std::count(active.begin(), active.end(), true));
}

struct Timing {
    double milliseconds = 0.0;
    size_t retained = 0;
};

template <class Function>
Timing measure(size_t repeats, Function&& function) {
    Timing timing{std::numeric_limits<double>::max(), 0};
    for (size_t repeat = 0; repeat < repeats; ++repeat) {
        const auto start = Clock::now();
        const size_t retained = function();
        const auto stop = Clock::now();
        const double milliseconds = std::chrono::duration<double, std::milli>(stop - start).count();
        timing.milliseconds = std::min(timing.milliseconds, milliseconds);
        timing.retained ^= retained;
    }
    if (repeats % 2 == 0) timing.retained ^= function();
    return timing;
}

void runScenario(std::string_view name, size_t clusters, size_t fanout, size_t repeats) {
    const std::vector<Edge> edges = makeRingGraph(clusters, fanout);

    (void)reduction::reduce(clusters, edges);
    (void)legacyRetainedCount(clusters, edges);

    const Timing incremental =
        measure(repeats, [&] { return reduction::reduce(clusters, edges).retained.size(); });
    const Timing legacy = measure(repeats, [&] { return legacyRetainedCount(clusters, edges); });

    const double speedup = legacy.milliseconds / incremental.milliseconds;
    std::cout << std::left << std::setw(14) << name << std::right << std::setw(10) << clusters
              << std::setw(12) << edges.size() << std::setw(14) << std::fixed
              << std::setprecision(3) << incremental.milliseconds << std::setw(14)
              << legacy.milliseconds << std::setw(11) << std::setprecision(2) << speedup << 'x'
              << std::setw(12) << incremental.retained << std::setw(12) << legacy.retained << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    const size_t clusters = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 256;
    const size_t repeats = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 3;
    if (clusters < 2 || repeats == 0) {
        std::cerr << "clusters must be at least 2 and repeats must be non-zero\n";
        return 1;
    }

    std::cout << "Weighted dependency reduction microbenchmark (best of " << repeats << ")\n";
    std::cout << std::left << std::setw(14) << "graph" << std::right << std::setw(10) << "clusters"
              << std::setw(12) << "edges" << std::setw(14) << "increment(ms)" << std::setw(14)
              << "legacy(ms)" << std::setw(11) << "speedup" << std::setw(12) << "new keep"
              << std::setw(12) << "old keep" << '\n';
    std::cout << std::string(99, '-') << '\n';

    runScenario("sparse-4", clusters, 4, repeats);
    runScenario("medium-16", clusters, 16, repeats);
    runScenario("dense-64", clusters, 64, repeats);
    return 0;
}
