// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_locality_partition.cpp
//
// Issue #36: the deterministic initial partition is locality-aware. A fixed,
// wall-clock-free sync cost (config.initial_partition_sync_cost_ns) makes the
// solver minimize the cross-thread edge cut, so topologically-connected units
// (a "core" pipeline) are co-located on one worker thread instead of being
// scattered across all threads.
//
// The change must NOT alter simulation semantics: results stay invariant across
// worker counts (the scheduler is deterministic regardless of placement), and no
// public API changes (existing code compiles unchanged, gets the win for free).

#include <cstdint>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"

using namespace chronon::sender;

namespace {

int g_pass = 0;
int g_fail = 0;

void check(bool cond, const std::string& msg) {
    if (cond) {
        ++g_pass;
        std::cout << "  [PASS] " << msg << "\n";
    } else {
        ++g_fail;
        std::cout << "  [FAIL] " << msg << "\n";
    }
}

// Deterministic stateful node: folds received values + arrival cycle into an
// accumulator, burns a little non-elidable work, and forwards the result.
class Node : public TickableUnit {
public:
    OutPort<uint64_t> out{this, "out"};
    InPort<uint64_t> in{this, "in"};

    Node(std::string name, uint64_t seed, uint32_t work)
        : TickableUnit(std::move(name)), acc_(seed), work_(work) {}

    void tick() override {
        for (uint64_t v : in.receiveAll(localCycle())) {
            acc_ ^= (v * 1000003ULL) ^ (localCycle() * 2654435761ULL);
        }
        uint64_t a = acc_;
        for (uint32_t i = 0; i < work_; ++i)
            a = a * 6364136223846793005ULL + 1442695040888963407ULL;
        acc_ = a;
        if (out.canSend()) (void)out.send(acc_);
    }

    uint64_t checksum() const { return acc_; }

private:
    uint64_t acc_;
    uint32_t work_;
};

struct Edge {
    Node* src;
    Node* dst;
};

// Build `cores` independent pipelines of `depth` delay-1 units, plus one shared
// node fed by every core's tail (an LLC-like serialization point). Connected
// units within a core form a topological component; the shared node is the only
// genuinely cross-component traffic.
struct Topology {
    std::vector<std::vector<Node*>> cores;
    Node* shared = nullptr;  // null when with_shared == false
    std::vector<Edge> edges;
};

// `cores` independent pipelines of `depth` delay-1 units. With `with_shared`,
// every core tail also feeds one shared node (an LLC-like fan-in serialization
// point) — that traffic is irreducibly cross-thread, so it is excluded from the
// strong co-location assertion and used only for the determinism sweep.
Topology buildCores(TickSimulation& sim, size_t cores, size_t depth, uint32_t work,
                    bool with_shared) {
    Topology topo;
    topo.cores.resize(cores);
    for (size_t c = 0; c < cores; ++c) {
        for (size_t s = 0; s < depth; ++s) {
            std::string name = "c";
            name += std::to_string(c);
            name += "_s";
            name += std::to_string(s);
            topo.cores[c].push_back(sim.createUnit<Node>(name, (c + 1) * 100 + s + 1, work));
        }
    }
    if (with_shared) topo.shared = sim.createUnit<Node>("shared", 7777, work);

    for (size_t c = 0; c < cores; ++c) {
        for (size_t s = 0; s + 1 < depth; ++s) {
            sim.connect(topo.cores[c][s]->out, topo.cores[c][s + 1]->in, 1);
            topo.edges.push_back({topo.cores[c][s], topo.cores[c][s + 1]});
        }
        if (with_shared) {
            sim.connect(topo.cores[c][depth - 1]->out, topo.shared->in, 1);
            topo.edges.push_back({topo.cores[c][depth - 1], topo.shared});
        }
    }
    return topo;
}

size_t countCrossThreadEdges(const TickSimulation& sim, const Topology& topo) {
    size_t cross = 0;
    for (const auto& e : topo.edges) {
        if (sim.assignedThread(e.src) != sim.assignedThread(e.dst)) ++cross;
    }
    return cross;
}

TickSimulationConfig baseConfig(size_t num_threads, double sync_cost) {
    TickSimulationConfig cfg;
    cfg.num_threads = num_threads;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.enable_weighted_partitioning = true;
    cfg.enable_dynamic_rebalance = false;  // isolate the initial partition
    cfg.epoch_size = 64;
    cfg.initial_partition_sync_cost_ns = sync_cost;
    return cfg;
}

// Count how many cores are fully co-located on a single thread.
size_t countColocatedCores(const TickSimulation& sim, const Topology& topo) {
    size_t colocated = 0;
    for (const auto& core : topo.cores) {
        std::set<size_t> threads;
        for (Node* n : core) threads.insert(sim.assignedThread(n));
        if (threads.size() == 1) ++colocated;
    }
    return colocated;
}

// With `cores` disjoint pipelines and `cores` threads, the locality-aware
// partition must place each pipeline entirely on its own thread (zero
// cross-thread edges) — whereas the old load-balance-only pass (sync_cost = 0)
// round-robins connected units across threads and cuts many edges.
void test_locality_colocation() {
    std::cout << "Testing locality-aware co-location (disjoint cores)...\n";

    constexpr size_t kCores = 4;
    constexpr size_t kDepth = 5;
    constexpr uint32_t kWork = 0;  // uniform unit cost; only topology matters

    // Locality on (new default sync cost).
    TickSimulation sim(baseConfig(kCores, /*sync_cost=*/8.0));
    Topology topo = buildCores(sim, kCores, kDepth, kWork, /*with_shared=*/false);
    sim.initialize();
    size_t cross_locality = countCrossThreadEdges(sim, topo);
    size_t colocated = countColocatedCores(sim, topo);

    // Baseline: pure load balance (sync cost = 0) scatters connected units.
    TickSimulation sim0(baseConfig(kCores, /*sync_cost=*/0.0));
    Topology topo0 = buildCores(sim0, kCores, kDepth, kWork, /*with_shared=*/false);
    sim0.initialize();
    size_t cross_baseline = countCrossThreadEdges(sim0, topo0);

    std::cout << "    co-located cores: " << colocated << "/" << kCores
              << "; cross-thread edges: locality=" << cross_locality
              << " baseline(sync=0)=" << cross_baseline << "\n";
    check(colocated == kCores, "every disjoint core pipeline co-located on one thread");
    check(cross_locality == 0, "locality partition cuts zero cross-thread edges");
    check(cross_locality < cross_baseline,
          "locality partition cuts fewer cross-thread edges than load-balance-only");
}

// Worker-count invariance: the locality-aware default must not change results.
// Same topology + feedback loop, run across worker counts and scheduler modes;
// every checksum must match the single-thread sequential reference.
uint64_t runSweep(size_t num_threads, bool lookahead, uint64_t cycles) {
    TickSimulationConfig cfg = baseConfig(num_threads, /*sync_cost=*/8.0);
    cfg.enable_parallel = (num_threads > 1);
    cfg.enable_lookahead = lookahead;

    TickSimulation sim(cfg);
    // Enough per-tick work that parallel placement is actually exercised.
    constexpr uint32_t kWork = 800;
    Topology topo = buildCores(sim, /*cores=*/4, /*depth=*/4, kWork, /*with_shared=*/true);
    // Feedback: shared node drives each core head, coupling rates across threads.
    for (auto& core : topo.cores) {
        sim.connect(topo.shared->out, core.front()->in, 1);
    }
    sim.initialize();
    sim.run(cycles);

    uint64_t checksum = topo.shared->checksum();
    for (auto& core : topo.cores)
        for (Node* n : core) checksum ^= n->checksum();
    return checksum;
}

void test_locality_determinism_sweep() {
    std::cout << "Testing worker-count invariance under locality default...\n";

    constexpr uint64_t kCycles = 1200;
    const uint64_t ref = runSweep(/*num_threads=*/1, /*lookahead=*/false, kCycles);

    for (size_t nw : {1u, 2u, 3u, 4u, 6u, 8u}) {
        for (bool lookahead : {false, true}) {
            if (nw == 1 && !lookahead) continue;  // that is the reference
            uint64_t got = runSweep(nw, lookahead, kCycles);
            check(got == ref, "nw=" + std::to_string(nw) + (lookahead ? " lookahead" : " barrier") +
                                  " matches sequential reference");
        }
    }
}

// The same TickSimulation must partition identically every time (no wall-clock).
void test_partition_repeatable() {
    std::cout << "Testing deterministic (repeatable) partition...\n";

    auto assignmentOf = []() {
        TickSimulation sim(baseConfig(4, 8.0));
        Topology topo = buildCores(sim, 4, 4, 0, /*with_shared=*/true);
        sim.initialize();
        std::vector<size_t> a;
        for (auto& core : topo.cores)
            for (Node* n : core) a.push_back(sim.assignedThread(n));
        a.push_back(sim.assignedThread(topo.shared));
        return a;
    };

    check(assignmentOf() == assignmentOf(), "partition is identical across runs");
}

void test_deterministic_parallel_benefit_ignores_locality_weight() {
    std::cout << "Testing deterministic parallel-benefit ignores locality weight...\n";

    TickSimulation sim(baseConfig(/*num_threads=*/4, /*sync_cost=*/8.0));
    Topology topo = buildCores(sim, /*cores=*/4, /*depth=*/4, /*work=*/0, /*with_shared=*/true);
    sim.initialize();

    check(countCrossThreadEdges(sim, topo) > 0, "topology has unavoidable cross-thread fan-in");
    check(sim.isParallelBeneficial(), "placement-only locality weight does not force sequential");
    check(sim.useParallelExecution(), "automatic execution mode keeps the parallel path");
}

void test_precomputed_parallel_benefit_includes_sync_cost() {
    std::cout << "Testing precomputed parallel-benefit includes sync cost...\n";

    TickSimulationConfig cfg = baseConfig(/*num_threads=*/2, /*sync_cost=*/0.0);
    cfg.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    TickSimulation sim(cfg);
    Topology topo = buildCores(sim, /*cores=*/1, /*depth=*/4, /*work=*/0, /*with_shared=*/false);

    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 2.8;
    sim.setPrecomputedUnitCosts({1.0, 1.0, 1.0, 1.0}, metrics);
    sim.initialize();

    size_t cross_edges = countCrossThreadEdges(sim, topo);
    std::set<size_t> used_threads;
    for (Node* n : topo.cores[0]) {
        used_threads.insert(sim.assignedThread(n));
    }

    check(used_threads.size() == 2, "partition keeps the cheap split across two threads");
    check(cross_edges > 0, "partition still has a cross-thread delay-1 edge");
    check(!sim.isParallelBeneficial(), "measured sync-dominated split falls back to sequential");
    check(!sim.useParallelExecution(), "automatic execution mode rejects the parallel path");
}

}  // namespace

int main() {
    std::cout << "=== Locality-Aware Partition Tests (issue #36) ===\n\n";

    test_locality_colocation();
    test_locality_determinism_sweep();
    test_partition_repeatable();
    test_deterministic_parallel_benefit_ignores_locality_weight();
    test_precomputed_parallel_benefit_includes_sync_cost();

    std::cout << "\n" << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
