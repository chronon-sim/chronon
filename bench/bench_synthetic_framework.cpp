// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// bench_synthetic_framework.cpp
//
// Synthetic benchmark for measuring the chronon framework's own overhead under
// multi-threaded lookahead, decoupled from heavy real units. Builds a topology
// of tunable SyntheticUnits (arithmetic intensity, memory footprint, unit count,
// connection topology) and sweeps the worker count, reporting throughput.
//
// Because the per-unit result is deterministic and thread-count invariant (see
// synthetic_workload.hpp), the benchmark asserts that the aggregate checksum is
// identical across every worker count in the sweep — a built-in self-test of the
// framework's determinism guarantees ("same parameters -> consistent results").
//
// Usage:
//   bench_synthetic_framework [--topo=islands|chain|fanout|mesh|dag|feedback]
//       [--units=N] [--arith=K] [--fp=K] [--footprint=BYTES] [--accesses=M]
//       [--cycles=C] [--delay=D] [--fanout=F] [--grid=WxH] [--epoch=E]
//       [--seed=S] [--lookahead=0|1] [--rebalance=0|1] [--threads=1,2,4,8,16]
//
// Examples:
//   # Isolate pure scheduler overhead (tiny per-tick work, many clusters):
//   bench_synthetic_framework --topo=islands --units=256 --arith=4 --accesses=0
//   # Memory-bound regime with realistic per-unit SRAM (256KB, L2/L3-resident):
//   bench_synthetic_framework --topo=chain --units=64 --accesses=16 --footprint=262144
//   # Compute-bound regime:
//   bench_synthetic_framework --topo=mesh --units=256 --arith=256 --fp=64

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "synthetic_workload.hpp"

using namespace chronon::sender;
using namespace synthetic;

namespace {

struct Options {
    TopologySpec spec;
    WorkloadKnobs knobs;
    uint64_t cycles = 200000;
    uint64_t epoch_size = 256;
    bool enable_lookahead = true;
    bool enable_rebalance = false;  // fixed layout for clean, reproducible timing
    std::vector<size_t> threads = {1, 2, 4, 8, 16};
};

bool matchKey(const std::string& arg, const char* key, std::string& value) {
    std::string prefix = std::string("--") + key + "=";
    if (arg.rfind(prefix, 0) == 0) {
        value = arg.substr(prefix.size());
        return true;
    }
    return false;
}

std::vector<size_t> parseThreadList(const std::string& s) {
    std::vector<size_t> out;
    size_t pos = 0;
    while (pos < s.size()) {
        size_t comma = s.find(',', pos);
        std::string tok =
            s.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
        if (!tok.empty()) out.push_back(std::strtoul(tok.c_str(), nullptr, 10));
        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
    return out;
}

bool parseArgs(int argc, char** argv, Options& opt) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string v;
        if (matchKey(arg, "topo", v)) {
            if (!parseTopologyKind(v, opt.spec.kind)) {
                std::cerr << "unknown --topo=" << v << "\n";
                return false;
            }
        } else if (matchKey(arg, "units", v)) {
            opt.spec.num_units = std::strtoul(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "arith", v)) {
            opt.knobs.arith_ops = std::strtoul(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "fp", v)) {
            opt.knobs.fp_ops = std::strtoul(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "footprint", v)) {
            opt.knobs.footprint_bytes = std::strtoull(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "accesses", v)) {
            opt.knobs.accesses_per_tick = std::strtoul(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "cycles", v)) {
            opt.cycles = std::strtoull(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "delay", v)) {
            opt.spec.delay = std::strtoul(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "fanout", v)) {
            opt.spec.fanout = std::strtoul(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "grid", v)) {
            size_t x = v.find('x');
            if (x != std::string::npos) {
                opt.spec.grid_w = std::strtoul(v.substr(0, x).c_str(), nullptr, 10);
                opt.spec.grid_h = std::strtoul(v.substr(x + 1).c_str(), nullptr, 10);
            }
        } else if (matchKey(arg, "epoch", v)) {
            opt.epoch_size = std::strtoull(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "seed", v)) {
            opt.spec.seed = std::strtoull(v.c_str(), nullptr, 10);
        } else if (matchKey(arg, "lookahead", v)) {
            opt.enable_lookahead = (v != "0");
        } else if (matchKey(arg, "rebalance", v)) {
            opt.enable_rebalance = (v != "0");
        } else if (matchKey(arg, "threads", v)) {
            opt.threads = parseThreadList(v);
        } else if (arg == "--help" || arg == "-h") {
            return false;
        } else {
            std::cerr << "unknown arg: " << arg << "\n";
            return false;
        }
    }
    return true;
}

double run_one(const Options& opt, size_t nw, uint64_t& chk_out, uint64_t& fpchk_out) {
    TickSimulationConfig cfg;
    cfg.num_threads = nw;
    cfg.enable_parallel = (nw > 1);
    cfg.enable_lookahead = opt.enable_lookahead;
    cfg.enable_dynamic_rebalance = opt.enable_rebalance;
    cfg.max_lookahead_cycles = 100;
    cfg.epoch_size = opt.epoch_size;

    TickSimulation sim(cfg);
    auto units = createTopology(sim, opt.spec, opt.knobs);
    sim.initialize();

    auto t0 = std::chrono::steady_clock::now();
    sim.run(opt.cycles);
    auto t1 = std::chrono::steady_clock::now();

    chk_out = aggregateChecksum(units);
    fpchk_out = aggregateFpChecksum(units);
    return std::chrono::duration<double>(t1 - t0).count();
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    if (!parseArgs(argc, argv, opt)) {
        std::cout << "Usage: " << argv[0]
                  << " [--topo=islands|chain|fanout|mesh|dag|feedback] [--units=N] [--arith=K]\n"
                     "       [--fp=K] [--footprint=BYTES] [--accesses=M] [--cycles=C] [--delay=D]\n"
                     "       [--fanout=F] [--grid=WxH] [--epoch=E] [--seed=S] [--lookahead=0|1]\n"
                     "       [--rebalance=0|1] [--threads=1,2,4,8,16]\n";
        return (argc > 1 && std::string(argv[1]) != "--help" && std::string(argv[1]) != "-h") ? 1
                                                                                              : 0;
    }

    const unsigned hw = std::thread::hardware_concurrency();
    const auto edges = buildEdges(opt.spec);

    std::cout << "=== Synthetic framework benchmark ===\n";
    std::cout << "topology=" << topologyKindName(opt.spec.kind) << "  units=" << opt.spec.num_units
              << "  edges=" << edges.size() << "  delay=" << opt.spec.delay
              << "  fanout=" << opt.spec.fanout << "\n";
    std::cout << "per-tick: arith=" << opt.knobs.arith_ops << "  fp=" << opt.knobs.fp_ops
              << "  accesses=" << opt.knobs.accesses_per_tick
              << "  footprint=" << opt.knobs.footprint_bytes << "B"
              << "  (aggregate working set ~"
              << (opt.knobs.footprint_bytes * opt.spec.num_units) / (1024 * 1024) << " MiB)\n";
    std::cout << "cycles=" << opt.cycles << "  epoch=" << opt.epoch_size
              << "  lookahead=" << (opt.enable_lookahead ? "on" : "off")
              << "  rebalance=" << (opt.enable_rebalance ? "on" : "off")
              << "  hw_concurrency=" << hw << "\n\n";

    std::cout << "workers |   best(s) |   sim-Mcyc/s |  unit-Mcyc/s | speedup |        checksum\n";
    std::cout
        << "--------+-----------+--------------+--------------+---------+------------------\n";

    constexpr int REPEATS = 3;
    double base = 0.0;
    bool have_ref = false;
    uint64_t ref_chk = 0, ref_fpchk = 0;
    bool determinism_ok = true;

    for (size_t nw : opt.threads) {
        if (nw == 0) continue;
        if (nw > hw && nw != 1) continue;  // skip oversubscription beyond the machine

        uint64_t chk = 0, fpchk = 0;
        double best = 1e30;
        run_one(opt, nw, chk, fpchk);  // warmup
        for (int r = 0; r < REPEATS; ++r) {
            double s = run_one(opt, nw, chk, fpchk);
            if (s < best) best = s;
        }

        if (!have_ref) {
            ref_chk = chk;
            ref_fpchk = fpchk;
            have_ref = true;
        } else if (chk != ref_chk || fpchk != ref_fpchk) {
            determinism_ok = false;
        }

        double sim_mcyc = static_cast<double>(opt.cycles) / best / 1e6;
        double unit_mcyc = static_cast<double>(opt.spec.num_units) * opt.cycles / best / 1e6;
        if (nw == 1) base = best;
        double speedup = base > 0 ? base / best : 1.0;
        std::printf("%7zu | %9.4f | %12.2f | %12.2f | %6.2fx | %016llx\n", nw, best, sim_mcyc,
                    unit_mcyc, speedup, static_cast<unsigned long long>(chk));
    }

    std::cout << "\n";
    if (determinism_ok) {
        std::printf("determinism: OK (chk=%016llx fp=%016llx) across all worker counts\n",
                    static_cast<unsigned long long>(ref_chk),
                    static_cast<unsigned long long>(ref_fpchk));
        return 0;
    }
    std::printf("determinism: FAILED — checksum varied across worker counts (ref chk=%016llx)\n",
                static_cast<unsigned long long>(ref_chk));
    return 1;
}
