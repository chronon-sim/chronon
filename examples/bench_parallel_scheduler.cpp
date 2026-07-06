// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

// bench_parallel_scheduler.cpp
//
// Throughput benchmark for the progress-based parallel scheduler. Many light
// independent clusters make the per-cycle lookahead-floor scan (issue #24)
// dominate, so the before/after gap is visible. Reports cluster-Mcyc/s across a
// worker-count sweep.
//
// Usage: bench_parallel_scheduler [num_units] [cycles] [per_tick_work]

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"

using namespace chronon::sender;

namespace {

// Independent unit: no ports, so each becomes its own cluster. A short,
// non-elidable arithmetic loop emulates light per-cycle work.
class Worklet : public TickableUnit {
public:
    Worklet(uint32_t id, uint32_t work)
        : TickableUnit(makeName(id)),
          work_(work),
          acc_(0x1234567ULL + static_cast<uint64_t>(id) * 2654435761ULL) {}
    void tick() override {
        uint64_t a = acc_;
        for (uint32_t i = 0; i < work_; ++i) {
            a = a * 6364136223846793005ULL + 1442695040888963407ULL;  // LCG, not elidable
        }
        acc_ = a;
        ++ticks_;
    }
    uint64_t ticks() const { return ticks_; }
    uint64_t checksum() const { return acc_; }

private:
    static std::string makeName(uint32_t id) {
        std::ostringstream out;
        out << 'w' << id;
        return out.str();
    }

    uint32_t work_;
    uint64_t acc_;
    uint64_t ticks_ = 0;
};

double run_one(size_t nw, size_t num_units, uint64_t cycles, uint32_t work, uint64_t& chk_out) {
    TickSimulationConfig cfg;
    cfg.num_threads = nw;
    cfg.enable_parallel = (nw > 1);
    cfg.enable_lookahead = true;
    cfg.enable_dynamic_rebalance = false;  // fixed cluster layout for a clean comparison
    cfg.max_lookahead_cycles = 100;
    cfg.epoch_size = 256;

    TickSimulation sim(cfg);
    std::vector<Worklet*> units;
    units.reserve(num_units);
    for (uint32_t i = 0; i < num_units; ++i) {
        units.push_back(sim.createUnit<Worklet>(i, work));
    }
    sim.initialize();

    auto t0 = std::chrono::steady_clock::now();
    sim.run(cycles);
    auto t1 = std::chrono::steady_clock::now();

    uint64_t chk = 0;
    for (auto* u : units) chk ^= u->checksum();
    chk_out = chk;
    return std::chrono::duration<double>(t1 - t0).count();
}

}  // namespace

int main(int argc, char** argv) {
    const size_t num_units = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 64;
    const uint64_t cycles = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 200000;
    const uint32_t work = argc > 3 ? std::strtoul(argv[3], nullptr, 10) : 8;

    const unsigned hw = std::thread::hardware_concurrency();
    std::cout << "=== Parallel scheduler benchmark (issue #24 hot path) ===\n";
    std::cout << "units=" << num_units << " (independent clusters)  cycles=" << cycles
              << "  per_tick_work=" << work << "  hw_concurrency=" << hw << "\n\n";
    std::cout << "workers |   best(s) |  cluster-Mcyc/s | speedup vs nw=1\n";
    std::cout << "--------+-----------+-----------------+----------------\n";

    std::vector<size_t> sweep = {1, 2, 4, 8, 16};
    constexpr int REPEATS = 3;
    double base = 0.0;

    for (size_t nw : sweep) {
        if (nw > hw && nw != 1) continue;  // skip oversubscription beyond the machine
        uint64_t chk = 0;
        double best = 1e30;
        // Warmup + timed repeats; report the best (least-noisy) run.
        run_one(nw, num_units, cycles, work, chk);
        for (int r = 0; r < REPEATS; ++r) {
            double s = run_one(nw, num_units, cycles, work, chk);
            if (s < best) best = s;
        }
        double mcyc = (static_cast<double>(num_units) * cycles) / best / 1e6;
        if (nw == 1) base = best;
        double speedup = base / best;
        std::printf("%7zu | %9.4f | %15.1f | %14.2fx  [chk=%016llx]\n", nw, best, mcyc, speedup,
                    static_cast<unsigned long long>(chk));
    }
    std::cout << "\n(Compare this table before vs. after the lookahead-floor change.)\n";
    return 0;
}
