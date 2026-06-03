// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// bench_boom.cpp
//
// Framework benchmark on a BOOMv3-flavored topology: N identical out-of-order
// cores sharing one 512 KiB L2 (see boom_topology.hpp). Measures chronon's
// scheduler throughput across a worker-count sweep and asserts the system
// checksum is invariant to worker count (determinism self-test). Optionally
// emits a colored scheduler timeline trace.
//
// Usage:
//   bench_boom [--cores=N] [--cycles=C] [--threads=1,2,4,8] [--epoch=E]
//              [--lookahead=0|1] [--rebalance=0|1] [--seed=S]
//              [--timeline=FILE] [--timeline-threads=N]
//
// Examples:
//   bench_boom --cores=2 --cycles=200000 --threads=1,2,4,8
//   bench_boom --cores=4 --cycles=100000 --timeline=boom.json --timeline-threads=8

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "boom_topology.hpp"
#include "sender/core/TickSimulation.hpp"

using namespace chronon::sender;
using namespace synthetic;

namespace {

struct Options {
    uint32_t cores = 2;
    uint64_t cycles = 200000;
    uint64_t epoch_size = 256;
    bool enable_lookahead = true;
    bool enable_rebalance = false;
    uint64_t seed = 0xB00C;
    std::vector<size_t> threads = {1, 2, 4, 8};
    std::string timeline_file;
    size_t timeline_threads = 0;  // 0 => largest in sweep
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
        if (matchKey(arg, "cores", v))
            opt.cores = std::strtoul(v.c_str(), nullptr, 10);
        else if (matchKey(arg, "cycles", v))
            opt.cycles = std::strtoull(v.c_str(), nullptr, 10);
        else if (matchKey(arg, "epoch", v))
            opt.epoch_size = std::strtoull(v.c_str(), nullptr, 10);
        else if (matchKey(arg, "seed", v))
            opt.seed = std::strtoull(v.c_str(), nullptr, 10);
        else if (matchKey(arg, "lookahead", v))
            opt.enable_lookahead = (v != "0");
        else if (matchKey(arg, "rebalance", v))
            opt.enable_rebalance = (v != "0");
        else if (matchKey(arg, "threads", v))
            opt.threads = parseThreadList(v);
        else if (matchKey(arg, "timeline", v))
            opt.timeline_file = v;
        else if (matchKey(arg, "timeline-threads", v))
            opt.timeline_threads = std::strtoul(v.c_str(), nullptr, 10);
        else if (arg == "--help" || arg == "-h")
            return false;
        else {
            std::cerr << "unknown arg: " << arg << "\n";
            return false;
        }
    }
    if (opt.cores == 0) opt.cores = 1;
    return true;
}

void configure(TickSimulationConfig& cfg, const Options& opt, size_t nw) {
    cfg.num_threads = nw;
    cfg.enable_parallel = (nw > 1);
    cfg.enable_lookahead = opt.enable_lookahead;
    cfg.enable_dynamic_rebalance = opt.enable_rebalance;
    cfg.max_lookahead_cycles = 100;
    cfg.epoch_size = opt.epoch_size;
}

double run_one(const Options& opt, size_t nw, uint64_t& chk_out, uint64_t& fpchk_out) {
    TickSimulationConfig cfg;
    configure(cfg, opt, nw);
    TickSimulation sim(cfg);
    auto units = buildBoomSystem(sim, opt.cores, opt.seed);
    sim.initialize();
    auto t0 = std::chrono::steady_clock::now();
    sim.run(opt.cycles);
    auto t1 = std::chrono::steady_clock::now();
    chk_out = aggregateChecksum(units);
    fpchk_out = aggregateFpChecksum(units);
    return std::chrono::duration<double>(t1 - t0).count();
}

void emitTimeline(const Options& opt, size_t nw) {
    TickSimulationConfig cfg;
    configure(cfg, opt, nw);
    cfg.timeline_trace.enabled = true;
    cfg.timeline_trace.file = opt.timeline_file;
    // Bound the traced window so the JSON stays legible and under the event cap.
    cfg.timeline_trace.end_cycle = std::min<uint64_t>(opt.cycles, 20000);
    TickSimulation sim(cfg);
    auto units = buildBoomSystem(sim, opt.cores, opt.seed);
    sim.initialize();
    sim.run(opt.cycles);
    sim.writeTimelineTrace();
    (void)units;
    std::cout << "\ntimeline trace written to " << opt.timeline_file << " (workers=" << nw
              << ") — open at ui.perfetto.dev or chrome://tracing\n";
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    if (!parseArgs(argc, argv, opt)) {
        std::cout << "Usage: " << argv[0]
                  << " [--cores=N] [--cycles=C] [--threads=1,2,4,8] [--epoch=E]\n"
                     "       [--lookahead=0|1] [--rebalance=0|1] [--seed=S]\n"
                     "       [--timeline=FILE] [--timeline-threads=N]\n";
        return (argc > 1 && std::string(argv[1]) != "--help" && std::string(argv[1]) != "-h") ? 1
                                                                                              : 0;
    }

    const unsigned hw = std::thread::hardware_concurrency();
    const uint32_t units_per_core = kStagesPerCore;
    const uint32_t total_units = opt.cores * units_per_core + 1;

    std::cout << "=== BOOMv3-flavored dual/multi-core framework benchmark ===\n";
    std::cout << "cores=" << opt.cores << " (" << units_per_core << " units/core + 1 shared L2)"
              << "  total_units=" << total_units << "\n";
    std::cout << "shared L2=512 KiB   per-core: bpd(3c) fetch(4c) decode rename dispatch issue "
                 "exe_int exe_fp lsu(+L1D) rob\n";
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
        if (nw > hw && nw != 1) continue;
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
        } else if (chk != ref_chk || fpchk != ref_fpchk)
            determinism_ok = false;

        double sim_mcyc = static_cast<double>(opt.cycles) / best / 1e6;
        double unit_mcyc = static_cast<double>(total_units) * opt.cycles / best / 1e6;
        if (nw == 1) base = best;
        double speedup = base > 0 ? base / best : 1.0;
        std::printf("%7zu | %9.4f | %12.2f | %12.2f | %6.2fx | %016llx\n", nw, best, sim_mcyc,
                    unit_mcyc, speedup, static_cast<unsigned long long>(chk));
    }

    std::cout << "\n";
    if (determinism_ok)
        std::printf("determinism: OK (chk=%016llx fp=%016llx) across all worker counts\n",
                    static_cast<unsigned long long>(ref_chk),
                    static_cast<unsigned long long>(ref_fpchk));
    else
        std::printf(
            "determinism: FAILED — checksum varied across worker counts "
            "(unexpected; please report)\n");

    if (!opt.timeline_file.empty()) {
        size_t tnw = opt.timeline_threads;
        if (tnw == 0) {
            for (size_t nw : opt.threads)
                if (nw <= hw) tnw = std::max(tnw, nw);
            if (tnw == 0) tnw = 1;
        }
        emitTimeline(opt, tnw);
    }

    return determinism_ok ? 0 : 1;
}
