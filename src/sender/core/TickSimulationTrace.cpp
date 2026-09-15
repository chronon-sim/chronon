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
    // Workers allocate their buffers on first capture. Eagerly allocating small
    // timestamp arrays here can put different writers on the same cache line.
    // Buffers then grow only when needed and are reused across run calls.
    if (timeline_trace_.traceThreadCpuTime()) {
        thread_trace_cpu_points_.resize(thread_units_.size());
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

void TickSimulation::resetDynamicSchedulerMarkers_() {
    dynamic_scheduler_marker_count_.store(0, std::memory_order_relaxed);
    dynamic_scheduler_marker_drops_.store(0, std::memory_order_relaxed);
}

void TickSimulation::recordDynamicSchedulerMarker_(std::string_view name, uint64_t cycle,
                                                   std::string_view detail) {
    if (!timeline_trace_.capturesCycle(cycle)) return;
    const size_t slot = dynamic_scheduler_marker_count_.fetch_add(1, std::memory_order_relaxed);
    if (slot >= dynamic_scheduler_markers_.size()) {
        dynamic_scheduler_marker_drops_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    auto& marker = dynamic_scheduler_markers_[slot];
    marker.time = SchedulerTimelineTrace::Clock::now();
    marker.cycle = cycle;
    marker.name = name;
    marker.detail = detail;
}

void TickSimulation::flushDynamicSchedulerMarkers_() {
    if (!timeline_trace_.enabled()) {
        resetDynamicSchedulerMarkers_();
        return;
    }

    const size_t count = std::min(dynamic_scheduler_marker_count_.load(std::memory_order_relaxed),
                                  dynamic_scheduler_markers_.size());
    for (size_t i = 0; i < count; ++i) {
        const auto& marker = dynamic_scheduler_markers_[i];
        if (marker.name.empty()) continue;
        timeline_trace_.recordInstant(timeline_trace_.schedulerStream(), "scheduler rebalance",
                                      marker.name, marker.cycle, marker.time, marker.detail);
    }

    const uint64_t drops = dynamic_scheduler_marker_drops_.load(std::memory_order_relaxed);
    if (drops > 0 && timeline_trace_.capturesCycle(current_cycle_)) {
        const auto now = SchedulerTimelineTrace::Clock::now();
        timeline_trace_.recordInstant(timeline_trace_.schedulerStream(), "scheduler rebalance",
                                      "Chronon epoch-free rebalance markers dropped",
                                      current_cycle_, now, "drops=" + std::to_string(drops));
    }

    resetDynamicSchedulerMarkers_();
}

}  // namespace chronon::sender
