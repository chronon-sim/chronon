// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

// test_rebalance_calibration.cpp
//
// Verifies that epoch-free dynamic rebalance fires and produces meaningful
// unit costs. Topology: 5 units with deliberately imbalanced tick costs;
// rebalance_check_interval_cycles is low enough to migrate within the budget.

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/Port.hpp"

using namespace chronon::sender;

namespace {

class HeavyUnit : public TickableUnit {
public:
    explicit HeavyUnit(const std::string& name, int iterations = 5000)
        : TickableUnit(name), iterations_(iterations) {}
    OutPort<uint64_t> out{this, "out", 1};

    void tick() override {
        uint64_t v = localCycle();
        for (int i = 0; i < iterations_; ++i) {
            v = v * 6364136223846793005ULL + 1442695040888963407ULL;
        }
        sink_ += v;
        (void)out.send(v);
    }

    uint64_t sink_ = 0;

private:
    int iterations_ = 5000;
};

class LightUnit : public TickableUnit {
public:
    explicit LightUnit(const std::string& name) : TickableUnit(name) {}
    InPort<uint64_t> in{this, "in"};
    OutPort<uint64_t> out{this, "out"};

    void tick() override {
        if (auto val = in.tryReceive(localCycle())) {
            (void)out.send(*val);
        }
    }
};

class Sink : public TickableUnit {
public:
    Sink() : TickableUnit("sink") {}
    InPort<uint64_t> in{this, "in"};

    void tick() override {
        while (in.tryReceive(localCycle())) {
        }
    }
};

class TightProducer : public TickableUnit {
public:
    TightProducer() : TickableUnit("tight_producer") {}
    OutPort<uint64_t> out{this, "out", 1};

    void tick() override { (void)out.send(localCycle()); }
};

class TightConsumer : public TickableUnit {
public:
    TightConsumer() : TickableUnit("tight_consumer") {}
    InPort<uint64_t> in{this, "in"};

    void tick() override {
        if (auto v = in.tryReceive(localCycle())) {
            checksum_ ^= *v + localCycle();
        }
    }

    uint64_t checksum() const { return checksum_; }

private:
    uint64_t checksum_ = 0;
};

#ifdef CHRONON_SANITIZER_BUILD
constexpr int kPrimaryHeavy0Iterations = 6000;
constexpr int kPrimaryHeavy3Iterations = 4000;
constexpr int kGuardHeavy0Iterations = 6000;
constexpr int kGuardHeavy2Iterations = 4000;
constexpr uint64_t kRunUntilCycles = 8192;
constexpr uint64_t kLongRunCycles = 8192;
constexpr uint64_t kChunkedCooldownCycles = 4096;
constexpr uint64_t kChunkBoundaryCycles = 1024;
constexpr uint64_t kGuardCycles = 2048;
#else
constexpr int kPrimaryHeavy0Iterations = 12000;
constexpr int kPrimaryHeavy3Iterations = 8000;
constexpr int kGuardHeavy0Iterations = 12000;
constexpr int kGuardHeavy2Iterations = 8000;
constexpr uint64_t kRunUntilCycles = 16384;
constexpr uint64_t kLongRunCycles = 65536;
constexpr uint64_t kChunkedCooldownCycles = 8192;
constexpr uint64_t kChunkBoundaryCycles = 2048;
constexpr uint64_t kGuardCycles = 4096;
#endif

}  // namespace

int run_rebalance_calibration(bool use_run_until_termination, uint64_t chunk_cycles = 0,
                              uint64_t check_interval = 64, uint64_t total_cycles = 16384) {
    std::cout << (use_run_until_termination ? "Testing runUntilTermination rebalance"
                                            : "Testing run() rebalance")
              << " (epoch-free)... ";

    TickSimulationConfig cfg;
    cfg.num_threads = 3;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.enable_epoch_free_lookahead = true;
    cfg.epoch_size = 64;
    cfg.enable_dynamic_rebalance = true;
    cfg.rebalance_check_interval_cycles = check_interval;
    cfg.rebalance_imbalance_threshold = 1.05;
    cfg.rebalance_min_gain = 0.0;
    cfg.rebalance_cooldown_cycles = 0;
    // This test isolates runtime sampling and migration. Disable the initial
    // locality heuristic so the fan-in topology still starts in parallel mode.
    cfg.initial_partition_sync_cost_ns = 0.0;

    TickSimulation sim(cfg);

    // 5 units: 4 heavy + 1 sink. With 3 threads and uniform cost LPT,
    // one thread starts with fewer units than the others. Real tick costs are
    // still imbalanced (heavy >> sink), so sampled costs should replace the
    // initial 1.0 placeholders on the first successful rebalance.
    // Uniform initial costs place unit indices 0 and 3 on the same thread.
    // Make those two runtime-heavy so dynamic rebalance has a deterministic
    // migration that lowers max active cost instead of relying on timing noise
    // between otherwise identical units.
    auto* h0 = sim.createUnit<HeavyUnit>("heavy0", kPrimaryHeavy0Iterations);
    auto* h1 = sim.createUnit<HeavyUnit>("heavy1", 300);
    auto* h2 = sim.createUnit<HeavyUnit>("heavy2", 300);
    auto* h3 = sim.createUnit<HeavyUnit>("heavy3", kPrimaryHeavy3Iterations);
    auto* sink = sim.createUnit<Sink>();

    sim.connect(h0->out, sink->in, 1);
    sim.connect(h1->out, sink->in, 1);
    sim.connect(h2->out, sink->in, 1);
    sim.connect(h3->out, sink->in, 1);

    sim.initialize();

    if (use_run_until_termination) {
        sim.runUntilTermination(total_cycles);
    } else if (chunk_cycles > 0) {
        for (uint64_t done = 0; done < total_cycles; done += chunk_cycles) {
            sim.run(std::min<uint64_t>(chunk_cycles, total_cycles - done));
        }
    } else {
        sim.run(total_cycles);
    }

    const auto& post = sim.getPlatformMetrics();
    std::cout << "\nPost-run atomic_roundtrip_ns: " << post.atomic_roundtrip_ns << "\n";
    std::cout << "Rebalance count: " << sim.rebalanceCount() << "\n";
    std::cout << "Epoch-free runs: " << sim.epochFreeRunCount() << "\n";

    const auto& costs = sim.getUnitCosts();
    std::cout << "Unit costs:";
    for (size_t i = 0; i < costs.size(); ++i) {
        std::cout << " " << costs[i];
    }
    std::cout << "\n";

    if (sim.rebalanceCount() == 0) {
        std::cerr << "FAIL: no rebalance occurred (imbalance may not have been detected)\n";
        return 1;
    }

    if (sim.epochFreeRunCount() == 0) {
        std::cerr << "FAIL: dynamic rebalance did not use epoch-free lookahead\n";
        return 1;
    }

    if (costs.size() != 5) {
        std::cerr << "FAIL: expected 5 unit costs, got " << costs.size() << "\n";
        return 1;
    }

    bool any_measured_cost = false;
    for (double cost : costs) {
        if (cost <= 0.0) {
            std::cerr << "FAIL: non-positive unit cost after rebalance: " << cost << "\n";
            return 1;
        }
        if (cost != 1.0) {
            any_measured_cost = true;
        }
    }

    if (!any_measured_cost) {
        std::cerr << "FAIL: unit costs remained at initial uniform placeholder values\n";
        return 1;
    }

    std::cout << "\n=== Rebalance calibration: PASSED ===\n";
    return 0;
}

int run_tight_cluster_migration_guard() {
    std::cout << "Testing delay=0 cluster migration guard... ";

    TickSimulationConfig cfg;
    cfg.num_threads = 3;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.epoch_size = 64;
    cfg.enable_dynamic_rebalance = true;
    cfg.rebalance_check_interval_cycles = 64;
    cfg.rebalance_imbalance_threshold = 1.05;
    cfg.rebalance_min_gain = 0.0;
    cfg.initial_partition_sync_cost_ns = 0.0;

    TickSimulation sim(cfg);
    auto* tight_src = sim.createUnit<TightProducer>();
    auto* tight_dst = sim.createUnit<TightConsumer>();
    // With the tight producer/consumer forming one delay=0 cluster, uniform
    // initial cluster costs place guard_heavy0 and guard_heavy2 together.
    // Make that pair runtime-heavy so the guard exercises a real migration
    // while still verifying the tight cluster remains atomic.
    auto* h0 = sim.createUnit<HeavyUnit>("guard_heavy0", kGuardHeavy0Iterations);
    auto* h1 = sim.createUnit<HeavyUnit>("guard_heavy1", 300);
    auto* h2 = sim.createUnit<HeavyUnit>("guard_heavy2", kGuardHeavy2Iterations);
    auto* h3 = sim.createUnit<HeavyUnit>("guard_heavy3", 300);
    auto* sink = sim.createUnit<Sink>();

    sim.connect(tight_src->out, tight_dst->in, 0);
    sim.connect(h0->out, sink->in, 1);
    sim.connect(h1->out, sink->in, 1);
    sim.connect(h2->out, sink->in, 1);
    sim.connect(h3->out, sink->in, 1);

    sim.initialize();
    sim.run(kGuardCycles);

    if (sim.assignedThread(tight_src) != sim.assignedThread(tight_dst)) {
        std::cerr << "FAIL: delay=0 tight cluster was split by migration\n";
        return 1;
    }
    if (sim.rebalanceCount() == 0 || sim.epochFreeRunCount() == 0) {
        std::cerr << "FAIL: guard did not exercise epoch-free dynamic migration\n";
        return 1;
    }

    std::cout << "PASSED (checksum=" << tight_dst->checksum()
              << ", rebalances=" << sim.rebalanceCount() << ")\n";
    return 0;
}

int main() {
    std::cout << "=== Rebalance Calibration Test ===\n";

    if (run_rebalance_calibration(true, 0, 64, kRunUntilCycles) != 0) {
        return 1;
    }
    if (run_rebalance_calibration(false, 0, 64, kLongRunCycles) != 0) {
        return 1;
    }
    if (run_rebalance_calibration(false, 1024, 2048, kChunkedCooldownCycles) != 0) {
        return 1;
    }
    if (run_rebalance_calibration(false, 64, 64, kChunkBoundaryCycles) != 0) {
        return 1;
    }
    if (run_tight_cluster_migration_guard() != 0) {
        return 1;
    }

    return 0;
}
