// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_versioned_register.cpp
//
// Verifies that VersionedRegister depth can be derived from the simulation's
// dependency graph via DependencyGraph::requiredVersionedRegisterDepth(),
// and that the derived depth is sufficient for temporal correctness under
// parallel lookahead execution.

#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/util/VersionedRegister.hpp"

using namespace chronon::sender;

template <typename Derived>
std::vector<Unit*> asUnits(const std::vector<Derived*>& v) {
    return {v.begin(), v.end()};
}

// ---------------------------------------------------------------------------
// Pipeline unit that reads/writes a shared VersionedRegister
// ---------------------------------------------------------------------------

class PipelineStage : public TickableUnit {
public:
    OutPort<uint64_t> out{this, "out"};
    InPort<uint64_t> in{this, "in"};

    enum class Role { None, Writer, Reader };

    PipelineStage(std::string name, Role role, VersionedRegister<uint64_t>* reg,
                  std::atomic<uint64_t>* violations)
        : TickableUnit(std::move(name)), role_(role), reg_(reg), violations_(violations) {}

    void tick() override {
        if (role_ == Role::Writer) {
            reg_->write(localCycle(), localCycle());
        } else if (role_ == Role::Reader) {
            uint64_t val = reg_->read(localCycle());
            if (val > localCycle()) {
                violations_->fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    void setRegister(VersionedRegister<uint64_t>* reg) { reg_ = reg; }
    void setViolations(std::atomic<uint64_t>* v) { violations_ = v; }

private:
    Role role_;
    VersionedRegister<uint64_t>* reg_;
    std::atomic<uint64_t>* violations_;
};

// ---------------------------------------------------------------------------
// Helper: build a simulation, initialize it, query the framework for depth
// ---------------------------------------------------------------------------

struct RingSimSetup {
    TickSimulation sim;
    std::vector<PipelineStage*> stages;
    PipelineStage* writer;
    std::vector<PipelineStage*> readers;

    RingSimSetup(size_t n, const std::vector<uint32_t>& delays, size_t writer_idx,
                 const std::set<size_t>& reader_indices, const TickSimulationConfig& cfg)
        : sim(cfg), writer(nullptr) {
        for (size_t i = 0; i < n; ++i) {
            PipelineStage::Role role = PipelineStage::Role::None;
            if (i == writer_idx)
                role = PipelineStage::Role::Writer;
            else if (reader_indices.count(i))
                role = PipelineStage::Role::Reader;
            stages.push_back(
                sim.createUnit<PipelineStage>("s" + std::to_string(i), role, nullptr, nullptr));
        }
        for (size_t i = 0; i < n; ++i) {
            sim.connect(stages[i]->out, stages[(i + 1) % n]->in, delays[i]);
        }
        writer = stages[writer_idx];
        for (size_t i : reader_indices) readers.push_back(stages[i]);
    }
};

// ---------------------------------------------------------------------------
// Depth derivation tests — call the framework API, not hand-rolled math
// ---------------------------------------------------------------------------

void test_depth_ring_uniform() {
    std::cout << "Testing depth derivation (ring, uniform delay=1)... ";

    const size_t N = 8;
    std::vector<uint32_t> delays(N, 1);
    std::set<size_t> reader_indices;
    for (size_t i = 1; i < N; ++i) reader_indices.insert(i);

    TickSimulationConfig cfg;
    cfg.enable_parallel = false;
    RingSimSetup setup(N, delays, 0, reader_indices, cfg);
    setup.sim.initialize();

    auto& graph = setup.sim.dependencyGraph();
    uint32_t depth = graph.requiredVersionedRegisterDepth(setup.writer, asUnits(setup.readers),
                                                          cfg.max_lookahead_cycles);

    // Ring of 8 with delay=1: max dist from reader 1 to writer 0 = 7. depth = 8.
    assert(depth == N);

    // Subset: only nearby readers {6, 7}, distances 2 and 1.
    std::vector<Unit*> close = {setup.stages[6], setup.stages[7]};
    depth = graph.requiredVersionedRegisterDepth(setup.writer, close, cfg.max_lookahead_cycles);
    assert(depth == 3);

    std::cout << "PASSED\n";
}

void test_depth_ring_varying_delay() {
    std::cout << "Testing depth derivation (ring, varying delays)... ";

    // 0->1:3, 1->2:2, 2->3:1, 3->0:4   total=10
    std::vector<uint32_t> delays = {3, 2, 1, 4};
    std::set<size_t> reader_indices = {1, 2, 3};

    TickSimulationConfig cfg;
    cfg.enable_parallel = false;
    RingSimSetup setup(4, delays, 0, reader_indices, cfg);
    setup.sim.initialize();

    auto& graph = setup.sim.dependencyGraph();
    uint32_t depth = graph.requiredVersionedRegisterDepth(setup.writer, asUnits(setup.readers),
                                                          cfg.max_lookahead_cycles);

    // dist[1][0]=7, dist[2][0]=5, dist[3][0]=4.  max=7, depth=8.
    assert(depth == 8);

    // Single reader: unit 3 only.  dist[3][0]=4, depth=5.
    std::vector<Unit*> one = {setup.stages[3]};
    depth = graph.requiredVersionedRegisterDepth(setup.writer, one, cfg.max_lookahead_cycles);
    assert(depth == 5);

    std::cout << "PASSED\n";
}

void test_depth_chain() {
    std::cout << "Testing depth derivation (chain, no back-edge)... ";

    // Chain 0->1->2->3, no cycle.  dist[3][0] = INF.
    const size_t N = 4;
    std::vector<uint32_t> delays = {1, 1, 1};

    TickSimulationConfig cfg;
    cfg.enable_parallel = false;
    cfg.max_lookahead_cycles = 100;

    TickSimulation sim(cfg);
    std::vector<PipelineStage*> stages;
    for (size_t i = 0; i < N; ++i) {
        PipelineStage::Role role = PipelineStage::Role::None;
        if (i == 0) role = PipelineStage::Role::Writer;
        if (i == N - 1) role = PipelineStage::Role::Reader;
        stages.push_back(
            sim.createUnit<PipelineStage>("s" + std::to_string(i), role, nullptr, nullptr));
    }
    for (size_t i = 0; i + 1 < N; ++i) {
        sim.connect(stages[i]->out, stages[i + 1]->in, delays[i]);
    }
    sim.initialize();

    std::vector<Unit*> readers = {stages[N - 1]};
    uint32_t depth = sim.dependencyGraph().requiredVersionedRegisterDepth(stages[0], readers,
                                                                          cfg.max_lookahead_cycles);

    assert(depth == cfg.max_lookahead_cycles + 1);

    std::cout << "PASSED\n";
}

void test_depth_random_ring() {
    std::cout << "Testing depth derivation (random ring, random readers)... ";

    std::mt19937 rng(42);
    const size_t N = 12;
    const size_t K = 5;

    std::vector<uint32_t> delays(N);
    uint32_t total_delay = 0;
    for (size_t i = 0; i < N; ++i) {
        delays[i] = 1 + rng() % 5;
        total_delay += delays[i];
    }

    std::set<size_t> reader_indices;
    while (reader_indices.size() < K) {
        size_t r = 1 + rng() % (N - 1);
        reader_indices.insert(r);
    }

    TickSimulationConfig cfg;
    cfg.enable_parallel = false;
    RingSimSetup setup(N, delays, 0, reader_indices, cfg);
    setup.sim.initialize();

    uint32_t depth = setup.sim.dependencyGraph().requiredVersionedRegisterDepth(
        setup.writer, asUnits(setup.readers), cfg.max_lookahead_cycles);

    assert(depth >= 2);
    assert(depth <= total_delay + 1);

    std::cout << "PASSED (depth=" << depth << ")\n";
}

// ---------------------------------------------------------------------------
// End-to-end simulation: derive depth from framework, run, verify correctness
// ---------------------------------------------------------------------------

void test_simulation_ring() {
    std::cout << "Testing simulation ring (parallel, framework-derived depth)... ";

    const size_t N = 8;
    const uint32_t DELAY = 2;
    const uint32_t MAX_LOOKAHEAD = 100;
    const uint64_t CYCLES = 2000;

    std::atomic<uint64_t> violations{0};
    std::vector<uint32_t> delays(N, DELAY);
    std::set<size_t> reader_indices;
    for (size_t i = 1; i < N; ++i) reader_indices.insert(i);

    TickSimulationConfig cfg;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.max_lookahead_cycles = MAX_LOOKAHEAD;
    cfg.num_threads = 4;
    cfg.epoch_size = 64;

    RingSimSetup setup(N, delays, 0, reader_indices, cfg);
    setup.sim.initialize();

    uint32_t depth = setup.sim.dependencyGraph().requiredVersionedRegisterDepth(
        setup.writer, asUnits(setup.readers), MAX_LOOKAHEAD);
    assert(depth == (N - 1) * DELAY + 1);

    VersionedRegister<uint64_t> reg(uint64_t{0}, depth);
    for (auto* s : setup.stages) {
        s->setRegister(&reg);
        s->setViolations(&violations);
    }

    setup.sim.run(CYCLES);
    assert(violations.load() == 0);

    std::cout << "PASSED (depth=" << depth << ", cycles=" << CYCLES << ")\n";
}

void test_simulation_chain() {
    std::cout << "Testing simulation chain (framework-derived depth)... ";

    const size_t N = 6;
    const uint32_t MAX_LOOKAHEAD = 50;
    const uint64_t CYCLES = 500;

    std::atomic<uint64_t> violations{0};

    TickSimulationConfig cfg;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.max_lookahead_cycles = MAX_LOOKAHEAD;
    cfg.num_threads = 2;
    cfg.epoch_size = 64;

    TickSimulation sim(cfg);
    std::vector<PipelineStage*> stages;
    for (size_t i = 0; i < N; ++i) {
        PipelineStage::Role role = PipelineStage::Role::None;
        if (i == 0) role = PipelineStage::Role::Writer;
        if (i == N - 1) role = PipelineStage::Role::Reader;
        stages.push_back(
            sim.createUnit<PipelineStage>("s" + std::to_string(i), role, nullptr, &violations));
    }
    for (size_t i = 0; i + 1 < N; ++i) {
        sim.connect(stages[i]->out, stages[i + 1]->in, 1);
    }
    sim.initialize();

    std::vector<Unit*> readers = {stages[N - 1]};
    uint32_t depth =
        sim.dependencyGraph().requiredVersionedRegisterDepth(stages[0], readers, MAX_LOOKAHEAD);
    assert(depth == MAX_LOOKAHEAD + 1);

    VersionedRegister<uint64_t> reg(uint64_t{0}, depth);
    for (auto* s : stages) s->setRegister(&reg);

    sim.run(CYCLES);
    assert(violations.load() == 0);

    std::cout << "PASSED (depth=" << depth << ")\n";
}

void test_simulation_random_ring() {
    std::cout << "Testing simulation random ring (framework-derived depth)... ";

    std::mt19937 rng(123);
    const size_t N = 10;
    const size_t K = 4;
    const uint32_t MAX_LOOKAHEAD = 200;
    const uint64_t CYCLES = 3000;

    std::atomic<uint64_t> violations{0};

    std::vector<uint32_t> delays(N);
    for (size_t i = 0; i < N; ++i) delays[i] = 1 + rng() % 5;

    std::set<size_t> reader_indices;
    while (reader_indices.size() < K) {
        size_t r = 1 + rng() % (N - 1);
        reader_indices.insert(r);
    }

    TickSimulationConfig cfg;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.max_lookahead_cycles = MAX_LOOKAHEAD;
    cfg.num_threads = 4;
    cfg.epoch_size = 64;

    RingSimSetup setup(N, delays, 0, reader_indices, cfg);
    setup.sim.initialize();

    uint32_t depth = setup.sim.dependencyGraph().requiredVersionedRegisterDepth(
        setup.writer, asUnits(setup.readers), MAX_LOOKAHEAD);

    VersionedRegister<uint64_t> reg(uint64_t{0}, depth);
    for (auto* s : setup.stages) {
        s->setRegister(&reg);
        s->setViolations(&violations);
    }

    setup.sim.run(CYCLES);
    assert(violations.load() == 0);

    std::cout << "PASSED (N=" << N << ", K=" << K << ", depth=" << depth << ", cycles=" << CYCLES
              << ")\n";
}

// ---------------------------------------------------------------------------

int main() {
    std::cout << "=== VersionedRegister Depth Derivation Tests ===\n\n";

    test_depth_ring_uniform();
    test_depth_ring_varying_delay();
    test_depth_chain();
    test_depth_random_ring();

    std::cout << "\n";

    test_simulation_ring();
    test_simulation_chain();
    test_simulation_random_ring();

    std::cout << "\n=== All VersionedRegister Tests PASSED ===\n";
    return 0;
}
