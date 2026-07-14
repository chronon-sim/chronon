// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

/// @file
/// Scheduler timeline scratch state and host thread timing helpers.

#include <chrono>
#include <ctime>
#include <string>

#include "TickSimulation.hpp"

#if defined(__linux__)
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace chronon::sender {

TickSimulation::ThreadTraceCpuPoint TickSimulation::threadTraceCpuPoint_() noexcept {
    ThreadTraceCpuPoint point{};
#if defined(CLOCK_THREAD_CPUTIME_ID)
    timespec ts{};
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0) {
        point.cpu_time_ns =
            static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
    }
#endif
#if defined(__linux__)
    static thread_local uint32_t cached_tid = static_cast<uint32_t>(::syscall(SYS_gettid));
    point.tid = cached_tid;
    point.cpu = ::sched_getcpu();
#endif
    return point;
}

void TickSimulation::initTimelineTraceScratch_() {
    thread_trace_points_.clear();
    thread_trace_cpu_points_.clear();
    if (!timeline_trace_.traceUnits() || thread_units_.empty()) return;

    thread_trace_points_.resize(thread_units_.size());
    if (timeline_trace_.traceThreadCpuTime()) {
        thread_trace_cpu_points_.resize(thread_units_.size());
    }
    for (size_t t = 0; t < thread_units_.size(); ++t) {
        thread_trace_points_[t].resize(thread_units_[t].size() + 1);
        if (!thread_trace_cpu_points_.empty()) {
            thread_trace_cpu_points_[t].resize(thread_units_[t].size() + 1);
        }
    }
}

void TickSimulation::recordUnitDuration_(size_t thread_idx, std::string_view category,
                                         std::string_view name, uint64_t cycle,
                                         SchedulerTimelineTrace::TimePoint begin,
                                         SchedulerTimelineTrace::TimePoint end,
                                         std::string_view detail, bool include_thread_cpu_time,
                                         ThreadTraceCpuPoint cpu_begin,
                                         ThreadTraceCpuPoint cpu_end) {
    if (!include_thread_cpu_time) {
        timeline_trace_.recordDuration(thread_idx, category, name, cycle, begin, end, detail);
        return;
    }

    const uint64_t wall_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
    const uint64_t thread_cpu_ns = cpu_end.cpu_time_ns >= cpu_begin.cpu_time_ns
                                       ? cpu_end.cpu_time_ns - cpu_begin.cpu_time_ns
                                       : 0;
    const uint64_t wall_cpu_gap_ns = wall_ns > thread_cpu_ns ? wall_ns - thread_cpu_ns : 0;

    std::string enriched;
    if (!detail.empty()) {
        enriched.assign(detail);
        enriched.push_back(' ');
    }
    enriched += "wall_ns=" + std::to_string(wall_ns);
    enriched += " thread_cpu_ns=" + std::to_string(thread_cpu_ns);
    enriched += " wall_cpu_gap_ns=" + std::to_string(wall_cpu_gap_ns);
    enriched += " tid=" + std::to_string(cpu_begin.tid);
    enriched += " cpu_begin=" + std::to_string(cpu_begin.cpu);
    enriched += " cpu_end=" + std::to_string(cpu_end.cpu);

    timeline_trace_.recordDuration(thread_idx, category, name, cycle, begin, end, enriched);
}

}  // namespace chronon::sender
