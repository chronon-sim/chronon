// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../../observe/PerfettoTraceWriter.hpp"
#include "../../observe/TimelineData.hpp"
#include "../core/TickableUnit.hpp"

namespace chronon::sender {

/** @brief Configuration for SchedulerTimelineTrace (Perfetto). */
struct SchedulerTimelineTraceConfig {
    bool enabled = false;
    /// Output filename (relative to the observation output directory when one
    /// is active, otherwise relative to cwd).  Always written as a standalone
    /// file separate from the simulation's timeline.pftrace.
    std::string file = "scheduler_timeline.pftrace";
    uint64_t max_events = 1'000'000;
    uint64_t start_cycle = 0;
    uint64_t end_cycle = std::numeric_limits<uint64_t>::max();
    bool trace_units = true;
    bool trace_waits = true;
    bool trace_epochs = true;
    /// When enabled, unit slices append per-thread CPU time diagnostics to detail.
    bool trace_thread_cpu_time = false;
    uint64_t min_duration_ns = 0;
};

/**
 * @brief Wall-clock timeline recorder for scheduler execution (Perfetto output).
 *
 * Records duration slices into per-stream arenas with no per-event allocation
 * (this measures the scheduler itself, so the record path stays off the
 * observation queues). At end of run the streams either merge into the
 * observation backend's unified timeline.pftrace (via exportData()) or are
 * written standalone (via write()).
 */
class SchedulerTimelineTrace {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    void configure(SchedulerTimelineTraceConfig config) {
        config_ = std::move(config);
        configured_ = config_.enabled;
        trace_units_ = config_.enabled && config_.trace_units;
        trace_waits_ = config_.enabled && config_.trace_waits;
        trace_epochs_ = config_.enabled && config_.trace_epochs;
        trace_thread_cpu_time_ =
            config_.enabled && config_.trace_units && config_.trace_thread_cpu_time;
        written_ = false;
        reserved_events_ = 0;
        stream_budgets_.reset();
        stream_budget_count_ = 0;
    }

    bool enabled() const noexcept { return configured_; }
    bool traceUnits() const noexcept { return trace_units_; }
    bool traceWaits() const noexcept { return trace_waits_; }
    bool traceEpochs() const noexcept { return trace_epochs_; }
    bool traceThreadCpuTime() const noexcept { return trace_thread_cpu_time_; }

    void start(const std::vector<std::vector<size_t>>& thread_units,
               const std::vector<TickableUnit*>& unit_ptrs) {
        if (!enabled() || started_) return;
        (void)unit_ptrs;

        base_time_ = Clock::now();
        const size_t stream_count = thread_units.empty() ? 1 : thread_units.size();
        data_ = observe::TimelineStreamData{};
        data_.streams.resize(stream_count + 1);  // Last stream is the scheduler lane.
        data_.arenas.resize(stream_count + 1);
        stream_budget_count_ = stream_count + 1;
        stream_budgets_ = std::make_unique<StreamBudget[]>(stream_budget_count_);
        event_budget_chunk_ =
            config_.max_events / data_.streams.size() < kEventBudgetChunk ? 1 : kEventBudgetChunk;
        const size_t reserve_per = config_.max_events / data_.streams.size();
        for (auto& s : data_.streams) {
            s.reserve(reserve_per);
        }
        // Pre-size each string arena to its share of events (~16 B/event heuristic)
        // so the common case appends without reallocating.
        for (auto& a : data_.arenas) {
            a.reserve(reserve_per * 16);
        }
        data_.stream_names.reserve(stream_count + 1);

        for (size_t stream = 0; stream < stream_count; ++stream) {
            std::ostringstream name;
            name << "stream " << stream << " (logical worker)";
            data_.stream_names.push_back(name.str());
        }
        scheduler_stream_ = stream_count;
        data_.stream_names.push_back("scheduler");
        started_ = true;
    }

    /**
     * @brief Record a duration event in a stream.
     *
     * Each stream must be written by exactly one thread during an epoch. Stream indices
     * map 1:1 to stdexec bulk indices; the scheduler stream is only written from the main
     * thread after sync_wait returns.
     */
    void recordDuration(size_t stream, std::string_view category, std::string_view name,
                        uint64_t cycle, TimePoint begin, TimePoint end,
                        std::string_view detail = {}) {
        if (!started_ || stream >= data_.streams.size()) return;
        if (cycle < config_.start_cycle || cycle >= config_.end_cycle) return;
        if (end < begin) return;

        const uint64_t duration_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
        if (duration_ns < config_.min_duration_ns) return;

        if (!claimEventSlot_(stream)) return;

        // Copy the strings into this stream's byte arena and store [offset,len)
        // slices. No per-event std::string is constructed (the arena's capacity is
        // reused across events), and because the bytes are copied here the trace
        // owns them — caller-supplied temporaries are safe. Per the contract above,
        // a given stream's arena is written by exactly one thread at a time.
        std::string& arena = data_.arenas[stream];
        const auto intern = [&arena](std::string_view s) -> std::pair<uint32_t, uint32_t> {
            const uint32_t off = static_cast<uint32_t>(arena.size());
            arena.append(s.data(), s.size());
            return {off, static_cast<uint32_t>(s.size())};
        };
        const auto [cat_off, cat_len] = intern(category);
        const auto [name_off, name_len] = intern(name);
        const auto [detail_off, detail_len] = intern(detail);
        data_.streams[stream].push_back({cat_off, cat_len, name_off, name_len, detail_off,
                                         detail_len, cycle, relNs(begin), duration_ns});
    }

    /// Record a point event in a stream.
    void recordInstant(size_t stream, std::string_view category, std::string_view name,
                       uint64_t cycle, TimePoint time, std::string_view detail = {}) {
        if (!started_ || stream >= data_.streams.size()) return;
        if (cycle < config_.start_cycle || cycle >= config_.end_cycle) return;

        if (!claimEventSlot_(stream)) return;

        std::string& arena = data_.arenas[stream];
        const auto intern = [&arena](std::string_view s) -> std::pair<uint32_t, uint32_t> {
            const uint32_t off = static_cast<uint32_t>(arena.size());
            arena.append(s.data(), s.size());
            return {off, static_cast<uint32_t>(s.size())};
        };
        const auto [cat_off, cat_len] = intern(category);
        const auto [name_off, name_len] = intern(name);
        const auto [detail_off, detail_len] = intern(detail);
        data_.streams[stream].push_back({cat_off, cat_len, name_off, name_len, detail_off,
                                         detail_len, cycle, relNs(time), 0, true});
    }

    size_t schedulerStream() const noexcept { return scheduler_stream_; }
    const std::string& file() const noexcept { return config_.file; }

    /**
     * @brief Move the recorded streams out for the unified timeline.pftrace.
     *
     * Intended for ObservationManager::submitTimeline(). The recorder is left
     * empty; subsequent write() calls are no-ops.
     */
    observe::TimelineStreamData exportData() {
        written_ = true;
        data_.dropped_events = droppedEvents_();
        return std::move(data_);
    }

    /// Write to the configured filename, resolved relative to @p output_dir.
    void write(const std::filesystem::path& output_dir) {
        const std::string& fname =
            config_.file.empty() ? "scheduler_timeline.pftrace" : config_.file;
        writeToPath_(output_dir / fname);
    }

    /// Write to the configured path (relative to cwd when no output dir is known).
    void write() {
        const std::string& fname =
            config_.file.empty() ? "scheduler_timeline.pftrace" : config_.file;
        writeToPath_(std::filesystem::path(fname));
    }

private:
    void writeToPath_(const std::filesystem::path& output_path) {
        if (!enabled() || !started_ || written_) return;
        written_ = true;

        if (output_path.has_parent_path()) {
            std::error_code ec;
            std::filesystem::create_directories(output_path.parent_path(), ec);
        }

        observe::PerfettoTraceWriter writer;
        if (!writer.open(output_path)) {
            std::cerr << "SchedulerTimelineTrace: failed to open " << output_path << "\n";
            return;
        }

        data_.dropped_events = droppedEvents_();
        observe::writeTimeline(writer, data_);
        writer.close();
    }

    bool claimEventSlot_(size_t stream) noexcept {
        // Each stream has one writer. Reserve global capacity in blocks so the
        // common event path only advances stream-local, cache-line-isolated state.
        auto& budget = stream_budgets_[stream];
        uint64_t available = budget.available.load(std::memory_order_relaxed);
        while (available != 0) {
            if (budget.available.compare_exchange_weak(available, available - 1,
                                                       std::memory_order_relaxed,
                                                       std::memory_order_relaxed)) {
                return true;
            }
        }

        std::lock_guard lock(event_budget_mutex_);
        if (reserved_events_ == config_.max_events) {
            // Credits are local atomics so reclaim races linearly with their
            // single writer. A successful writer CAS consumes the credit first;
            // otherwise exchange() reclaims it for another stream.
            uint64_t reclaimed = 0;
            for (size_t index = 0; index < stream_budget_count_; ++index) {
                reclaimed +=
                    stream_budgets_[index].available.exchange(0, std::memory_order_relaxed);
            }
            reserved_events_ -= reclaimed;
        }

        if (reserved_events_ == config_.max_events) {
            ++budget.dropped;
            return false;
        }

        const uint64_t grant = std::min(event_budget_chunk_, config_.max_events - reserved_events_);
        reserved_events_ += grant;
        budget.available.store(grant - 1, std::memory_order_relaxed);
        return true;
    }

    uint64_t droppedEvents_() const noexcept {
        uint64_t total = 0;
        for (size_t index = 0; index < stream_budget_count_; ++index) {
            total += stream_budgets_[index].dropped;
        }
        return total;
    }

    uint64_t relNs(TimePoint tp) const {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(tp - base_time_).count());
    }

    SchedulerTimelineTraceConfig config_{};
    bool configured_ = false;
    bool trace_units_ = false;
    bool trace_waits_ = false;
    bool trace_epochs_ = false;
    bool trace_thread_cpu_time_ = false;
    bool started_ = false;
    bool written_ = false;
    TimePoint base_time_{};
    size_t scheduler_stream_ = 0;
    static constexpr uint64_t kEventBudgetChunk = 256;
    struct alignas(64) StreamBudget {
        std::atomic<uint64_t> available{0};
        uint64_t dropped = 0;
    };
    /// Recorded streams: slices, per-stream byte arenas, lane names.
    observe::TimelineStreamData data_;
    std::unique_ptr<StreamBudget[]> stream_budgets_;
    size_t stream_budget_count_ = 0;
    uint64_t event_budget_chunk_ = 1;
    uint64_t reserved_events_ = 0;
    std::mutex event_budget_mutex_;
};

}  // namespace chronon::sender
