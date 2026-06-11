// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_epoch_free_lookahead_determinism.cpp
//
// Correctness gate for the epoch-free lookahead scheduler
// (TickSimulationConfig::enable_epoch_free_lookahead). Removing the per-epoch
// std::barrier must NOT change results: the run-ahead bound moves from the
// epoch boundary to lookahead_floor_ + max_lookahead_cycles, and MPSC delivery
// relies on per-connection consumer-driven draining plus a single end-of-run
// flush instead of a per-epoch central flush.
//
// The topology is the heterogeneous-delay feedback loop from
// test_mpsc_mixed_delay_determinism.cpp — the case most sensitive to a message
// arriving one cycle early/late. We assert the epoch-free path reproduces the
// sequential reference bit-for-bit across worker counts and across a
// max_lookahead sweep (including 1, the tightest gate), AND that it actually
// engaged (epochFreeRunCount() > 0) rather than silently falling back to the
// barrier path.

#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
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

// Same node as the mixed-delay test: folds (value, arrival cycle) into an
// accumulator with non-elidable work, so the checksum is sensitive to WHICH
// simulated cycle each message arrives at.
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

struct RunResult {
    uint64_t checksum;
    uint64_t epoch_free_runs;
};

// A and B fan in to C with delays (dA, dB); C -> D -> {A, B} closes a feedback
// loop so producer rates are coupled.
RunResult runOnce(uint32_t dA, uint32_t dB, size_t num_threads, bool lookahead, bool epoch_free,
                  uint32_t max_lookahead, uint64_t cycles) {
    TickSimulationConfig cfg;
    cfg.num_threads = num_threads;
    cfg.enable_parallel = (num_threads > 1);
    cfg.enable_lookahead = lookahead;
    cfg.enable_dynamic_rebalance = false;  // required for the persistent path
    cfg.enable_epoch_free_lookahead = epoch_free;
    cfg.max_lookahead_cycles = max_lookahead;
    cfg.epoch_size = 64;

    TickSimulation sim(cfg);
    constexpr uint32_t kWork = 1500;  // spread the 4 units across workers
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
    return {A->checksum() ^ B->checksum() ^ C->checksum() ^ D->checksum(), sim.epochFreeRunCount()};
}

void verify(uint32_t dA, uint32_t dB, uint64_t cycles, unsigned hw) {
    const std::string base = "dA=" + std::to_string(dA) + " dB=" + std::to_string(dB);
    const uint64_t ref = runOnce(dA, dB, /*threads=*/1, /*lookahead=*/false, /*epoch_free=*/false,
                                 /*max_lookahead=*/100, cycles)
                             .checksum;

    for (uint32_t la : {1u, 64u, 100u}) {
        for (size_t nw : {2u, 4u}) {
            if (nw > hw) continue;  // can't engage the persistent path past core count
            const std::string label =
                base + " nw=" + std::to_string(nw) + " la=" + std::to_string(la);

            // Sanity: barrier lookahead still matches (our edits must not perturb it).
            RunResult barrier =
                runOnce(dA, dB, nw, /*lookahead=*/true, /*epoch_free=*/false, la, cycles);
            check(barrier.checksum == ref, label + " barrier-lookahead == ref");

            // The actual gate: epoch-free must match AND must have engaged.
            RunResult ef = runOnce(dA, dB, nw, /*lookahead=*/true, /*epoch_free=*/true, la, cycles);
            check(ef.checksum == ref, label + " epoch-free == ref");
            check(ef.epoch_free_runs > 0, label + " epoch-free actually engaged");
        }
    }
}

// The fan-in ports into C have unlimited capacity, so their MPSC staging rings
// (4096) never back-pressure. If max_lookahead_cycles could exceed that ring,
// the epoch-free path (no per-epoch drain) might silently drop staged sends, so
// the gate must veto and fall back to the barrier path. Verify it does, and that
// the result still matches the single-thread reference.
void verify_staging_veto(uint64_t cycles, unsigned hw) {
    if (hw < 2) return;
    const uint64_t ref = runOnce(2, 5, /*threads=*/1, /*lookahead=*/false, /*epoch_free=*/false,
                                 /*max_lookahead=*/100, cycles)
                             .checksum;
    // 5000 > 4095 usable staging slots on the unlimited fan-in ports -> veto.
    RunResult ef = runOnce(2, 5, /*threads=*/2, /*lookahead=*/true, /*epoch_free=*/true,
                           /*max_lookahead=*/5000, cycles);
    check(ef.checksum == ref, "staging-veto epoch-free == ref (barrier fallback)");
    check(ef.epoch_free_runs == 0, "staging-veto vetoes epoch-free past ring capacity");
}

}  // namespace

int main() {
    std::cout << "=== Epoch-free lookahead determinism test ===\n";
    const uint64_t cycles = 1500;
    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 2;
    std::cout << "  (hardware_concurrency = " << hw << ")\n";

    verify(1, 1, cycles, hw);
    verify(1, 3, cycles, hw);
    verify(3, 1, cycles, hw);
    verify(1, 8, cycles, hw);
    verify(2, 5, cycles, hw);

    verify_staging_veto(cycles, hw);

    std::cout << "\n=== Results: " << g_pass << " passed, " << g_fail << " failed ===\n";
    return g_fail == 0 ? 0 : 1;
}
