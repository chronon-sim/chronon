// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_synthetic_determinism.cpp
//
// Guards the "same parameters -> consistent results" contract of the synthetic
// benchmark: the aggregate checksum over all SyntheticUnits must be identical
// regardless of worker count or lookahead/barrier mode. This exercises the
// framework's determinism guarantees (conn_id-ordered MPSC arbitration,
// topological unit ordering, fixed simulated-cycle delivery) across a range of
// topologies.

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "synthetic_workload.hpp"

using namespace chronon::sender;
using namespace synthetic;

namespace {

int g_pass = 0;
int g_fail = 0;

struct RunResult {
    uint64_t chk;
    uint64_t fpchk;
};

RunResult runConfig(const TopologySpec& spec, const WorkloadKnobs& knobs, uint64_t cycles,
                    size_t num_threads, bool lookahead) {
    TickSimulationConfig cfg;
    cfg.num_threads = num_threads;
    cfg.enable_parallel = (num_threads > 1);
    cfg.enable_lookahead = lookahead;
    cfg.enable_dynamic_rebalance = false;
    cfg.max_lookahead_cycles = 100;
    cfg.epoch_size = 64;

    TickSimulation sim(cfg);
    auto units = createTopology(sim, spec, knobs);
    sim.initialize();
    sim.run(cycles);
    return {aggregateChecksum(units), aggregateFpChecksum(units)};
}

void check(bool cond, const std::string& msg) {
    if (cond) {
        ++g_pass;
        std::cout << "  [PASS] " << msg << "\n";
    } else {
        ++g_fail;
        std::cout << "  [FAIL] " << msg << "\n";
    }
}

// For one topology: compute the sequential single-thread reference, then assert
// every (worker-count, lookahead) combination reproduces it bit-for-bit.
void verifyTopology(const std::string& label, const TopologySpec& spec, const WorkloadKnobs& knobs,
                    uint64_t cycles) {
    std::cout << "Topology: " << label << " (units=" << spec.num_units
              << ", edges=" << buildEdges(spec).size() << ")\n";

    RunResult ref = runConfig(spec, knobs, cycles, /*num_threads=*/1, /*lookahead=*/false);
    std::cout << "  reference chk=" << std::hex << ref.chk << " fp=" << ref.fpchk << std::dec
              << "\n";

    const std::vector<size_t> thread_counts = {1, 2, 4, 8};
    for (size_t nw : thread_counts) {
        for (bool lookahead : {false, true}) {
            if (nw == 1 && !lookahead) continue;  // that is the reference itself
            RunResult r = runConfig(spec, knobs, cycles, nw, lookahead);
            std::string desc =
                label + " nw=" + std::to_string(nw) + (lookahead ? " lookahead" : " barrier");
            check(r.chk == ref.chk && r.fpchk == ref.fpchk, desc);
        }
    }
}

}  // namespace

int main() {
    std::cout << "=== Synthetic workload determinism test ===\n\n";

    const uint64_t cycles = 20000;
    WorkloadKnobs knobs;
    knobs.arith_ops = 16;
    knobs.fp_ops = 8;  // include FP work to cover the FP checksum path
    knobs.footprint_bytes = 8192;
    knobs.accesses_per_tick = 3;

    {
        TopologySpec s;
        s.kind = TopologySpec::Kind::Chain;
        s.num_units = 32;
        s.delay = 1;
        verifyTopology("chain", s, knobs, cycles);
    }
    {
        TopologySpec s;
        s.kind = TopologySpec::Kind::Islands;
        s.num_units = 64;
        s.fanout = 4;
        s.delay = 1;
        verifyTopology("islands", s, knobs, cycles);
    }
    {
        TopologySpec s;
        s.kind = TopologySpec::Kind::FanoutTree;
        s.num_units = 40;
        s.fanout = 3;
        s.delay = 1;
        verifyTopology("fanout", s, knobs, cycles);
    }
    {
        TopologySpec s;
        s.kind = TopologySpec::Kind::Mesh2D;
        s.num_units = 36;
        s.delay = 1;
        verifyTopology("mesh", s, knobs, cycles);
    }
    {
        TopologySpec s;
        s.kind = TopologySpec::Kind::RandomDag;
        s.num_units = 50;
        s.fanout = 3;
        s.delay = 2;
        s.seed = 0xBEEF;
        verifyTopology("dag", s, knobs, cycles);
    }
    {
        TopologySpec s;
        s.kind = TopologySpec::Kind::Feedback;
        s.num_units = 24;
        s.delay = 1;
        verifyTopology("feedback", s, knobs, cycles);
    }

    std::cout << "\n=== Results: " << g_pass << " passed, " << g_fail << " failed ===\n";
    return g_fail == 0 ? 0 : 1;
}
