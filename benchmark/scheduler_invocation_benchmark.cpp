// SPDX-License-Identifier: MPL-2.0
#include <sys/resource.h>

#include <chrono>
#include <iomanip>
#include <iostream>

#include "BenchmarkThreadAffinity.hpp"
#include "ClockAllocationCount.hpp"
#include "SchedulerInvocationModel.hpp"

#ifdef CHRONON_BENCH_SCRATCH
namespace chronon::sender {
struct SchedulerScratchTestAccess {
    static size_t bytes(const TickSimulation& sim) {
        if (!sim.scheduler_scratch_) return 0;
        size_t result =
            sizeof(TickSimulation::SchedulerScratch) +
            sim.scheduler_scratch_->workers.capacity() * sizeof(TickSimulation::WorkerRunScratch);
        const auto bytes = [](const auto& values) { return values.capacity() * sizeof(values[0]); };
        for (const auto& worker : sim.scheduler_scratch_->workers) {
            result += bytes(worker.predecessor.observed_cycles) + bytes(worker.owned_clusters) +
                      bytes(worker.owned_bridges) + bytes(worker.owned_actors) +
                      bytes(worker.ownership) + bytes(worker.priority_blocker) +
                      bytes(worker.ready_through) + bytes(worker.priority_cost);
        }
        return result;
    }
};
}  // namespace chronon::sender
#endif

int main(int argc, char** argv) {
    if (argc != 9) {
        std::cerr << "usage: scheduler_invocation_benchmark CLOCK THREADS PAIRS WORK SKEW DYNAMIC "
                     "INTERVAL STEPS\n";
        return 2;
    }
    using namespace chronon;
    using namespace chronon::benchmark;
    const bool clock = std::stoull(argv[1]);
    const size_t threads = std::stoull(argv[2]), pairs = std::stoull(argv[3]);
    const size_t work = std::stoull(argv[4]), skew = std::stoull(argv[5]);
    const bool dynamic = std::stoull(argv[6]);
    const uint64_t interval = std::stoull(argv[7]), steps = std::stoull(argv[8]);
    if (!threads || threads > 64 || !pairs || pairs > 512 || !steps || steps > 1000000000) return 2;
    TickSimulationConfig config;
    config.num_threads = threads;
    config.enable_parallel = threads > 1;
    config.enable_dynamic_rebalance = dynamic;
    config.max_lookahead_cycles = 32;
    config.epoch_size = interval ? interval : 32;
    TickSimulation sim(config);
    const auto worker_cpus = pinBenchmarkWorkers(threads);
    const auto units = invocationModel(sim, clock, pairs, work, skew);
    const auto init_alloc = clockAllocations();
    const auto init_begin = std::chrono::steady_clock::now();
    sim.initialize();
    const auto init_end = std::chrono::steady_clock::now();
    const auto init_allocations = clockAllocations() - init_alloc;
    warmBenchmarkCpus(worker_cpus);
    // Warm storage/queues once, with identical calls in baseline and candidate.
    const auto advance = [&](uint64_t count) {
        return clock ? sim.runClockEvents(count) : sim.run(count);
    };
    if (advance(128) != 128) return 3;
    const auto start = clock ? sim.schedulerSteps() : sim.currentCycle();
    uint64_t predicates = 0;
    rusage before{};
    getrusage(RUSAGE_SELF, &before);
    const auto run_alloc = clockAllocations();
    const auto begin = std::chrono::steady_clock::now();
    const auto completed = interval ? sim.runUntil(
                                          [&] {
                                              ++predicates;
                                              return (clock ? sim.schedulerSteps()
                                                            : sim.currentCycle()) >= start + steps;
                                          },
                                          steps)
                                    : advance(steps);
    const auto end = std::chrono::steady_clock::now();
    const auto allocations = clockAllocations() - run_alloc;
    if (completed != steps || (interval && predicates != (steps + interval - 1) / interval))
        return 3;
    uint64_t ticks = 0, sent = 0, received = 0, checksum = 0, digest = 0;
    for (size_t i = 0; i < units.size(); ++i) {
        const auto* u = units[i];
        ticks += u->localCycle();
        (u->writer ? sent : received) += u->count;
        checksum += u->checksum;
        digest ^= (i + 1) * u->digest;
    }
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    const auto cpuSeconds = [](const rusage& r) {
        return r.ru_utime.tv_sec + r.ru_utime.tv_usec / 1e6 + r.ru_stime.tv_sec +
               r.ru_stime.tv_usec / 1e6;
    };
    size_t retained = 0;
#ifdef CHRONON_BENCH_SCRATCH
    retained = sender::SchedulerScratchTestAccess::bytes(sim);
#endif
    std::cout << "init_s,run_s,init_allocations,run_allocations,worker_scratch_bytes,rss_kib,"
                 "run_cpu_s,voluntary_switches,involuntary_switches,predicates,parallel,ticks,sent,"
                 "received,checksum,digest,overflow\n"
              << std::setprecision(12)
              << std::chrono::duration<double>(init_end - init_begin).count() << ','
              << std::chrono::duration<double>(end - begin).count() << ',' << init_allocations
              << ',' << allocations << ',' << retained << ',' << usage.ru_maxrss << ','
              << cpuSeconds(usage) - cpuSeconds(before) << ',' << usage.ru_nvcsw - before.ru_nvcsw
              << ',' << usage.ru_nivcsw - before.ru_nivcsw << ',' << predicates << ','
              << sim.useParallelExecution() << ',' << ticks << ',' << sent << ',' << received << ','
              << checksum << ',' << digest << ',' << sim.totalTransportOverflowEvents() << '\n';
}
