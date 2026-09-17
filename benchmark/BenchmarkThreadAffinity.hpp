// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#include <unistd.h>
#endif

namespace chronon::benchmark {
// Optional measurement control, applied identically to baseline and candidate
// before model construction/timing. Pin persistent pool threads to distinct
// CPUs from the caller's taskset mask; never change simulation configuration.
inline void pinBenchmarkWorkers(size_t workers) {
    if (!std::getenv("CHRONON_BENCH_PIN_WORKERS")) return;
#if defined(__linux__)
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed)) throw std::runtime_error("get affinity");
    std::vector<int> cpus;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
        if (CPU_ISSET(cpu, &allowed)) cpus.push_back(cpu);
    std::vector<pid_t> tids;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/task")) {
        const auto tid = static_cast<pid_t>(std::stol(entry.path().filename().string()));
        if (tid != getpid()) tids.push_back(tid);
    }
    std::sort(tids.begin(), tids.end());
    if (tids.size() != workers || cpus.size() < workers)
        throw std::runtime_error(
            "pinning requires exactly the pool threads and enough allowed CPUs");
    const auto pin = [](pid_t tid, int cpu) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        if (sched_setaffinity(tid, sizeof(set), &set)) throw std::runtime_error("set affinity");
    };
    for (size_t i = 0; i < workers; ++i) pin(tids[i], cpus[i]);
    pin(0, workers == 1 ? cpus.front() : cpus.back());
#else
    throw std::runtime_error("benchmark thread pinning requires Linux");
#endif
}
}  // namespace chronon::benchmark
