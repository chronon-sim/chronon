// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <sys/resource.h>

#include <chrono>
#include <iomanip>
#include <iostream>

#include "chronon/Chronon.hpp"

namespace chronon::benchmark {

struct ClockWork : TickableUnit {
    AsyncWritePort<uint64_t> out{this, "out"};
    AsyncReadPort<uint64_t> in{this, "in"};
    uint64_t count = 0, checksum = 0, work_digest = 1;
    unsigned work;
    bool producer;
    ClockWork(std::string name, unsigned work, bool producer)
        : TickableUnit(std::move(name)), work(work), producer(producer) {}
    void tick() override {
        for (unsigned i = 0; i < work; ++i) {
            work_digest = work_digest * 6364136223846793005ULL + i + 1;
            asm volatile("" : "+r"(work_digest));
        }
        if (producer) {
            if (out.send({count + 1, count + 1})) ++count;
        } else {
            if (auto packet = in.take()) {
                if (packet->data != count + 1) throw std::runtime_error("FIFO order mismatch");
                ++count;
                checksum += packet->data;
            }
            in.requestRead();
        }
    }
};

// Reuse the clock benchmark executable and its CSV runner. Fixed hardware work
// and full end-state digests make serial/static/dynamic and before/after runs
// comparable; initialization is reported separately from simulation throughput.
inline int runClockScaling(int argc, char** argv) {
    if (argc != 9) {
        std::cerr << "usage: multiclock_benchmark scaling STEPS THREADS PAIRS DOMAINS WORK "
                     "DYNAMIC SKEW\n";
        return 2;
    }
    const auto steps = std::stoull(argv[2]);
    const auto threads = std::stoull(argv[3]);
    const auto pairs = std::stoull(argv[4]);
    const auto domains = std::stoull(argv[5]);
    const auto work = std::stoull(argv[6]);
    const auto dynamic = std::stoull(argv[7]);
    const auto skew = std::stoull(argv[8]);
    if (!steps || steps > 1'000'000'000 || !threads || threads > 64 || !pairs || pairs > 512 ||
        domains < 2 || domains > 2 * pairs || work > 100'000 || dynamic > 1 || !skew || skew > 64)
        throw std::invalid_argument("scaling arguments outside supported bounds");
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.max_lookahead_cycles = 32;
    TickSimulation sim(config);
    for (size_t d = 0; d < domains; ++d)
        sim.addClockDomain(ClockDomain::fromHz(d + 1, "clock" + std::to_string(d),
                                               250'000'000ULL << (d % 4), 1,
                                               SimTime::picoseconds(d * 37)));
    std::vector<ClockWork*> producers, consumers;
    for (size_t p = 0; p < pairs; ++p) {
        const unsigned cost = static_cast<unsigned>(work * (p == 0 ? skew : 1));
        auto* producer = sim.createUnitInDomain<ClockWork>(
            (p * 2) % domains + 1, "producer" + std::to_string(p), cost, true);
        auto* consumer = sim.createUnitInDomain<ClockWork>(
            (p * 2 + 1) % domains + 1, "consumer" + std::to_string(p), cost, false);
        sim.connectAsyncFifo(p + 1, producer->out, consumer->in, {16, 2});
        producers.push_back(producer);
        consumers.push_back(consumer);
    }
    const auto begin = std::chrono::steady_clock::now();
    sim.initialize();
    const auto initialized = std::chrono::steady_clock::now();
    if (sim.runClockEvents(steps) != steps) throw std::runtime_error("short scaling run");
    const auto end = std::chrono::steady_clock::now();
    uint64_t ticks = 0, sent = 0, received = 0, checksum = 0, digest = 0;
    for (size_t p = 0; p < pairs; ++p) {
        ticks += producers[p]->localCycle() + consumers[p]->localCycle();
        sent += producers[p]->count;
        received += consumers[p]->count;
        checksum += consumers[p]->checksum;
        digest ^= (p + 1) * (producers[p]->work_digest + 31 * consumers[p]->work_digest);
    }
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    const double cpu = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 +
                       usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
    std::cout
        << std::setprecision(12)
        << "mode,steps,threads,pairs,domains,work,dynamic,skew,init_s,run_s,wall_s,cpu_s,"
           "maxrss_kib,unit_ticks,sent,received,checksum,work_digest,migrations,parallel,overflow\n"
        << "scaling," << steps << ',' << threads << ',' << pairs << ',' << domains << ',' << work
        << ',' << dynamic << ',' << skew << ','
        << std::chrono::duration<double>(initialized - begin).count() << ','
        << std::chrono::duration<double>(end - initialized).count() << ','
        << std::chrono::duration<double>(end - begin).count() << ',' << cpu << ','
        << usage.ru_maxrss << ',' << ticks << ',' << sent << ',' << received << ',' << checksum
        << ',' << digest << ',' << sim.rebalanceCount() << ',' << sim.useParallelExecution() << ','
        << sim.totalTransportOverflowEvents() << '\n';
    return 0;
}

}  // namespace chronon::benchmark
