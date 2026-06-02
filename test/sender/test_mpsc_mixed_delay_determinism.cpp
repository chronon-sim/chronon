// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_mpsc_mixed_delay_determinism.cpp
//
// Regression test for heterogeneous-delay MPSC arbitration. A consumer with
// fan-in from multiple producers at DIFFERENT edge delays, inside a feedback
// loop, must produce a result that is identical across worker counts and
// matches barrier/sequential (i.e. the lookahead scheduler stays cycle-accurate).
//
// Before the per-connection arbitration fix (draining each MPSC connection up to
// its OWN producer's completed cycle rather than the min across producers), the
// low-delay producer's message was held back by a lagging high-delay producer on
// the same InPort, arrived a cycle late, and the feedback loop amplified it into
// a worker-count-dependent divergence.

#include <cstdint>
#include <iostream>
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

// A deterministic node: folds every received value (and the cycle it arrived at)
// into its accumulator, does a little non-elidable work, and emits the result.
// Sensitive to WHICH simulated cycle each message arrives at.
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

// Topology: A and B fan in to C with delays (dA, dB); C -> D -> {A, B} closes a
// feedback loop so producer rates are coupled. `work` spreads units across
// threads so producers/consumer actually land on different workers.
uint64_t runOnce(uint32_t dA, uint32_t dB, size_t num_threads, bool lookahead, uint64_t cycles) {
    TickSimulationConfig cfg;
    cfg.num_threads = num_threads;
    cfg.enable_parallel = (num_threads > 1);
    cfg.enable_lookahead = lookahead;
    cfg.enable_dynamic_rebalance = false;
    cfg.max_lookahead_cycles = 100;
    cfg.epoch_size = 64;

    TickSimulation sim(cfg);
    constexpr uint32_t kWork = 3000;
    auto* A = sim.createUnit<Node>("A", 11, kWork);
    auto* B = sim.createUnit<Node>("B", 22, kWork);
    auto* C = sim.createUnit<Node>("C", 33, kWork);
    auto* D = sim.createUnit<Node>("D", 44, kWork);
    sim.connect(A->out, C->in, dA);  // mixed-delay MPSC fan-in into C
    sim.connect(B->out, C->in, dB);
    sim.connect(C->out, D->in, 1);
    sim.connect(D->out, A->in, 1);  // feedback couples A, B, C rates
    sim.connect(D->out, B->in, 1);
    sim.initialize();
    sim.run(cycles);
    return A->checksum() ^ B->checksum() ^ C->checksum() ^ D->checksum();
}

void verify(uint32_t dA, uint32_t dB, uint64_t cycles) {
    const std::string label = "dA=" + std::to_string(dA) + " dB=" + std::to_string(dB);
    const uint64_t ref = runOnce(dA, dB, /*threads=*/1, /*lookahead=*/false, cycles);
    for (size_t nw : {1u, 2u, 4u, 8u}) {
        for (bool lookahead : {false, true}) {
            if (nw == 1 && !lookahead) continue;  // that is the reference
            uint64_t got = runOnce(dA, dB, nw, lookahead, cycles);
            check(got == ref,
                  label + " nw=" + std::to_string(nw) + (lookahead ? " lookahead" : " barrier"));
        }
    }
}

}  // namespace

int main() {
    std::cout << "=== MPSC mixed-delay determinism test ===\n";
    const uint64_t cycles = 8000;

    // Uniform delays (the case that always worked) plus several heterogeneous
    // combinations that previously diverged under lookahead.
    verify(1, 1, cycles);
    verify(1, 3, cycles);
    verify(3, 1, cycles);
    verify(1, 8, cycles);
    verify(2, 5, cycles);
    verify(8, 1, cycles);

    std::cout << "\n=== Results: " << g_pass << " passed, " << g_fail << " failed ===\n";
    return g_fail == 0 ? 0 : 1;
}
