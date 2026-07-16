// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

// Measures reducer construction cost and, more importantly, the resulting hot-
// path dependency count and fan-in. Treat construction cost as a guardrail after
// closure preservation and pruning strength.
//
// Usage: chronon_weighted_dependency_reduction_benchmark [clusters] [repeats]

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string_view>
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

// Synthetic scheduler-style graph with uneven fan-in and many same-source
// zero-delay shortcuts. The 63-edge chain is necessary; every other edge is
// transitively implied by that chain. No application topology is encoded here.
std::vector<Edge> makeHeterogeneousZeroDelayGraph() {
    constexpr size_t clusters = 64;
    std::vector<Edge> edges;
    edges.reserve(168);

    for (size_t source = 0; source + 1 < clusters; ++source) {
        edges.push_back({source, source + 1, 0});
    }

    auto addShortcuts = [&](size_t source, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            const size_t hop = 2 + i * 2 + (source % 2);
            edges.push_back({source, source + hop, 0});
        }
    };

    for (size_t source = 0; source < 15; ++source) addShortcuts(source, 5);
    for (size_t source = 15; source < 25; ++source) addShortcuts(source, 2);
    for (size_t source = 25; source < 35; ++source) addShortcuts(source, 1);
    return edges;
}

struct Timing {
    double milliseconds = 0.0;
    size_t retained = 0;
    double mean_fan_in = 0.0;
    size_t max_fan_in = 0;
};

Timing measure(size_t repeats, size_t clusters, const std::vector<Edge>& edges) {
    Timing timing{std::numeric_limits<double>::max(), 0, 0.0, 0};
    for (size_t repeat = 0; repeat < repeats; ++repeat) {
        const auto start = Clock::now();
        const auto result = reduction::reduce(clusters, edges);
        const auto stop = Clock::now();
        const double milliseconds = std::chrono::duration<double, std::milli>(stop - start).count();
        timing.milliseconds = std::min(timing.milliseconds, milliseconds);
        timing.retained = result.retained.size();
        size_t total_fan_in = 0;
        timing.max_fan_in = 0;
        for (size_t fan_in : result.fan_in_after) {
            total_fan_in += fan_in;
            timing.max_fan_in = std::max(timing.max_fan_in, fan_in);
        }
        timing.mean_fan_in = static_cast<double>(total_fan_in) / clusters;
    }
    return timing;
}

bool runScenario(std::string_view name, size_t clusters, const std::vector<Edge>& edges,
                 size_t repeats, size_t expected_retained) {
    (void)reduction::reduce(clusters, edges);
    const Timing timing = measure(repeats, clusters, edges);

    std::cout << std::left << std::setw(14) << name << std::right << std::setw(10) << clusters
              << std::setw(12) << edges.size() << std::setw(14) << std::fixed
              << std::setprecision(3) << timing.milliseconds << std::setw(12) << timing.retained
              << std::setw(12) << (edges.size() - timing.retained) << std::setw(14)
              << std::setprecision(2) << timing.mean_fan_in << std::setw(12) << timing.max_fan_in
              << '\n';

    if (timing.retained != expected_retained) {
        std::cerr << name << ": expected " << expected_retained << " retained edges, got "
                  << timing.retained << '\n';
        return false;
    }
    return true;
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
              << std::setw(12) << "edges" << std::setw(14) << "build(ms)" << std::setw(12)
              << "retained" << std::setw(12) << "removed" << std::setw(14) << "mean fan-in"
              << std::setw(12) << "max fan-in" << '\n';
    std::cout << std::string(96, '-') << '\n';

    bool valid = true;
    valid &= runScenario("sparse-4", clusters, makeRingGraph(clusters, 4), repeats, clusters);
    valid &= runScenario("medium-16", clusters, makeRingGraph(clusters, 16), repeats, clusters);
    valid &= runScenario("dense-64", clusters, makeRingGraph(clusters, 64), repeats, clusters);

    constexpr size_t heterogeneous_clusters = 64;
    valid &= runScenario("hetero-zero", heterogeneous_clusters, makeHeterogeneousZeroDelayGraph(),
                         repeats, heterogeneous_clusters - 1);
    return valid ? 0 : 2;
}
