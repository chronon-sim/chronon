// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

// test_rebalance_calibration.cpp
//
// Verifies that the deferred PlatformBenchmark calibration fires on the
// first dynamic rebalance. After assignThreadsDeterministic_(), rdtsc_to_ns
// is left at 0 (uncalibrated sentinel). performRebalance_() must call
// PlatformBenchmark::measure() before converting sampled ticks to
// nanoseconds, setting rdtsc_to_ns > 0.
//
// Topology: 5 units with deliberately imbalanced tick costs so that
// shouldRebalance_() triggers. rebalance_check_interval_cycles is set
// low (64) to ensure a rebalance occurs within the test's run budget.

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
    explicit HeavyUnit(const std::string& name) : TickableUnit(name) {}
    OutPort<uint64_t> out{this, "out"};

    void tick() override {
        uint64_t v = localCycle();
        for (int i = 0; i < 5000; ++i) {
            v = v * 6364136223846793005ULL + 1442695040888963407ULL;
        }
        sink_ += v;
        (void)out.send(v);
    }

    uint64_t sink_ = 0;
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

}  // namespace

int main() {
    std::cout << "=== Rebalance Calibration Test ===\n";

    TickSimulationConfig cfg;
    cfg.num_threads = 3;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.epoch_size = 64;
    cfg.enable_dynamic_rebalance = true;
    cfg.rebalance_check_interval_cycles = 64;
    cfg.rebalance_imbalance_threshold = 1.05;
    cfg.rebalance_min_gain = 0.0;
    cfg.rebalance_cooldown_cycles = 0;

    TickSimulation sim(cfg);

    // 5 units: 4 heavy + 1 sink. With 3 threads and uniform cost LPT,
    // one thread starts with fewer units than the others. Real tick costs are
    // still imbalanced (heavy >> sink), so sampled costs should replace the
    // initial 1.0 placeholders on the first successful rebalance.
    auto* h0 = sim.createUnit<HeavyUnit>("heavy0");
    auto* h1 = sim.createUnit<HeavyUnit>("heavy1");
    auto* h2 = sim.createUnit<HeavyUnit>("heavy2");
    auto* h3 = sim.createUnit<HeavyUnit>("heavy3");
    auto* sink = sim.createUnit<Sink>();

    sim.connect(h0->out, sink->in, 1);
    sim.connect(h1->out, sink->in, 1);
    sim.connect(h2->out, sink->in, 1);
    sim.connect(h3->out, sink->in, 1);

    sim.initialize();

    const auto& pre = sim.getPlatformMetrics();
    std::cout << "Pre-rebalance rdtsc_to_ns: " << pre.rdtsc_to_ns << "\n";
    assert(pre.rdtsc_to_ns == 0.0 && "rdtsc_to_ns should be uncalibrated before first rebalance");

    sim.runUntilTermination(16384);

    const auto& post = sim.getPlatformMetrics();
    std::cout << "Post-run rdtsc_to_ns: " << post.rdtsc_to_ns << "\n";
    std::cout << "Post-run atomic_roundtrip_ns: " << post.atomic_roundtrip_ns << "\n";
    std::cout << "Rebalance count: " << sim.rebalanceCount() << "\n";

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

    if (post.rdtsc_to_ns == 0.0) {
        std::cerr << "FAIL: rdtsc_to_ns still 0 after rebalance (calibration not triggered)\n";
        return 1;
    }

    if (post.rdtsc_to_ns < 0.0) {
        std::cerr << "FAIL: rdtsc_to_ns is negative: " << post.rdtsc_to_ns << "\n";
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
