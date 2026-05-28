// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_sa_integration.cpp
//
// Integration test: TickSimulation with PartitionSolverType::SA.
// Verifies the full path from config → resolveSolver_() → SA initial
// partitioning → parallel run completes without crash.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/schedule/SimulatedAnnealingPartitioner.hpp"

using namespace chronon::sender;

#define REQUIRE(cond)                                                              \
    do {                                                                           \
        if (!(cond)) {                                                             \
            std::cerr << "FAILED: " #cond " at " __FILE__ ":" << __LINE__ << "\n"; \
            std::abort();                                                          \
        }                                                                          \
    } while (0)

// Simple unit with configurable work per tick.
class WorkUnit : public TickableUnit {
public:
    WorkUnit(std::string name, uint64_t work) : TickableUnit(std::move(name)), work_(work) {}

    OutPort<int> out{this, "out"};
    InPort<int> in{this, "in", 4096};

    void tick() override {
        while (in.tryReceive(localCycle()).has_value()) {
        }
        uint64_t x = 0;
        for (uint64_t i = 0; i < work_; ++i) {
            x += i;
            asm volatile("" : "+r"(x) : :);
        }
        asm volatile("" : : "r"(x) : "memory");
    }

private:
    uint64_t work_;
};

void test_sa_simulation_completes() {
    std::cout << "Testing SA-partitioned simulation completes... ";

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = true;
    config.enable_lookahead = true;

    config.enable_weighted_partitioning = true;
    config.epoch_size = 64;

    config.partition_solver = TickSimulationConfig::PartitionSolverType::SA;

    TickSimulation sim(config);

    auto* a = sim.createUnit<WorkUnit>("a", 5000);
    auto* b = sim.createUnit<WorkUnit>("b", 3000);
    auto* c = sim.createUnit<WorkUnit>("c", 4000);
    auto* d = sim.createUnit<WorkUnit>("d", 2000);
    auto* e = sim.createUnit<WorkUnit>("e", 6000);
    auto* f = sim.createUnit<WorkUnit>("f", 1000);

    sim.connect(a->out, b->in, 1);
    sim.connect(b->out, c->in, 1);
    sim.connect(c->out, d->in, 1);
    sim.connect(d->out, e->in, 1);
    sim.connect(e->out, f->in, 1);

    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 1.0;
    sim.setPrecomputedUnitCosts({500, 300, 400, 200, 600, 100}, metrics);
    sim.initialize();

    REQUIRE(sim.isParallelBeneficial());

    // Verify all units got a valid thread assignment
    for (auto* u : {a, b, c, d, e, f}) {
        size_t tid = sim.assignedThread(u);
        REQUIRE(tid != SIZE_MAX);
        REQUIRE(tid < 2);
    }

    // Run 5000 cycles — enough to exercise parallel tick loop
    sim.run(5000);

    std::cout << "PASSED\n";
}

void test_sa_simulation_delay_zero() {
    std::cout << "Testing SA simulation with delay=0 edges... ";

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = true;
    config.enable_lookahead = true;

    config.enable_weighted_partitioning = true;
    config.epoch_size = 64;

    config.partition_solver = TickSimulationConfig::PartitionSolverType::SA;

    TickSimulation sim(config);

    auto* a = sim.createUnit<WorkUnit>("a", 3000);
    auto* b = sim.createUnit<WorkUnit>("b", 3000);
    auto* c = sim.createUnit<WorkUnit>("c", 3000);
    auto* d = sim.createUnit<WorkUnit>("d", 3000);

    // delay=0 forces a+b onto same thread, c+d onto same thread
    sim.connect(a->out, b->in, 0);
    sim.connect(c->out, d->in, 0);
    // Cross-cluster with delay=2
    sim.connect(b->out, c->in, 2);

    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 1.0;
    sim.setPrecomputedUnitCosts({300, 300, 300, 300}, metrics);
    sim.initialize();

    // delay=0 pairs must be co-located
    REQUIRE(sim.assignedThread(a) == sim.assignedThread(b));
    REQUIRE(sim.assignedThread(c) == sim.assignedThread(d));

    sim.run(2000);

    std::cout << "PASSED\n";
}

void test_sa_no_sequential_fallback_t2() {
    std::cout << "Testing SA no sequential fallback (t=2)... ";

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = true;
    config.enable_lookahead = true;

    config.enable_weighted_partitioning = true;
    config.epoch_size = 64;

    config.partition_solver = TickSimulationConfig::PartitionSolverType::SA;

    TickSimulation sim(config);

    auto* a = sim.createUnit<WorkUnit>("fetch", 5000);
    auto* b = sim.createUnit<WorkUnit>("decode", 3000);
    auto* c = sim.createUnit<WorkUnit>("rename", 4000);
    auto* d = sim.createUnit<WorkUnit>("dispatch", 2000);
    auto* e = sim.createUnit<WorkUnit>("execute", 6000);
    auto* f = sim.createUnit<WorkUnit>("rob", 1000);

    // Pipeline chain with delay=1
    sim.connect(a->out, b->in, 1);
    sim.connect(b->out, c->in, 1);
    sim.connect(c->out, d->in, 1);
    sim.connect(d->out, e->in, 1);
    sim.connect(e->out, f->in, 1);

    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 1.0;
    sim.setPrecomputedUnitCosts({500, 300, 400, 200, 600, 100}, metrics);
    sim.initialize();

    // The critical assertion: parallelBeneficialWeighted_() must return true.
    // If SA degenerates to one thread, active_threads < 2 → sequential fallback.
    REQUIRE(sim.isParallelBeneficial());

    // Both threads must have units
    std::set<size_t> used_threads;
    for (auto* u : {a, b, c, d, e, f}) {
        used_threads.insert(sim.assignedThread(u));
    }
    REQUIRE(used_threads.size() == 2);

    sim.run(2000);

    std::cout << "PASSED\n";
}

void test_sa_no_sequential_fallback_t4() {
    std::cout << "Testing SA no sequential fallback (t=4)... ";

    TickSimulationConfig config;
    config.num_threads = 4;
    config.enable_parallel = true;
    config.enable_lookahead = true;

    config.enable_weighted_partitioning = true;
    config.epoch_size = 64;

    config.partition_solver = TickSimulationConfig::PartitionSolverType::SA;

    TickSimulation sim(config);

    auto* u0 = sim.createUnit<WorkUnit>("fetch", 5000);
    auto* u1 = sim.createUnit<WorkUnit>("decode", 3000);
    auto* u2 = sim.createUnit<WorkUnit>("rename", 4000);
    auto* u3 = sim.createUnit<WorkUnit>("dispatch", 2000);
    auto* u4 = sim.createUnit<WorkUnit>("iq0", 6000);
    auto* u5 = sim.createUnit<WorkUnit>("iq1", 1000);
    auto* u6 = sim.createUnit<WorkUnit>("execute", 3500);
    auto* u7 = sim.createUnit<WorkUnit>("lsu", 1500);
    auto* u8 = sim.createUnit<WorkUnit>("rob", 2500);
    auto* u9 = sim.createUnit<WorkUnit>("flush", 800);

    // Pipeline chain + branches
    sim.connect(u0->out, u1->in, 1);
    sim.connect(u1->out, u2->in, 1);
    sim.connect(u2->out, u3->in, 1);
    sim.connect(u3->out, u4->in, 1);
    sim.connect(u3->out, u5->in, 1);
    sim.connect(u4->out, u6->in, 1);
    sim.connect(u5->out, u7->in, 1);
    sim.connect(u6->out, u8->in, 1);
    sim.connect(u7->out, u8->in, 1);
    sim.connect(u8->out, u9->in, 1);

    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 1.0;
    sim.setPrecomputedUnitCosts({500, 300, 400, 200, 600, 100, 350, 150, 250, 80}, metrics);
    sim.initialize();

    REQUIRE(sim.isParallelBeneficial());

    // All 4 threads must have units
    std::set<size_t> used_threads;
    for (auto* u : {u0, u1, u2, u3, u4, u5, u6, u7, u8, u9}) {
        used_threads.insert(sim.assignedThread(u));
    }
    REQUIRE(used_threads.size() == 4);

    sim.run(2000);

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== SA Integration Tests ===\n\n";

    test_sa_simulation_completes();
    test_sa_simulation_delay_zero();
    test_sa_no_sequential_fallback_t2();
    test_sa_no_sequential_fallback_t4();

    std::cout << "\n=== All SA integration tests PASSED ===\n";
    return 0;
}
