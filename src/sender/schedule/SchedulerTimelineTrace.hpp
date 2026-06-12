// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
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
    bool trace_arbitration = true;
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
        trace_arbitration_ = config_.enabled && config_.trace_arbitration;
        written_ = false;
        dropped_events_.store(0, std::memory_order_relaxed);
        event_count_.store(0, std::memory_order_relaxed);
    }

    bool enabled() const noexcept { return configured_; }
    bool traceUnits() const noexcept { return trace_units_; }
    bool traceWaits() const noexcept { return trace_waits_; }
    bool traceEpochs() const noexcept { return trace_epochs_; }
    bool traceArbitration() const noexcept { return trace_arbitration_; }

    void start(const std::vector<std::vector<size_t>>& thread_units,
               const std::vector<TickableUnit*>& unit_ptrs) {
        if (!enabled() || started_) return;
        (void)unit_ptrs;

        base_time_ = Clock::now();
        const size_t stream_count = thread_units.empty() ? 1 : thread_units.size();
        data_ = observe::TimelineStreamData{};
        data_.streams.resize(stream_count + 1);  // Last stream is the scheduler lane.
        data_.arenas.resize(stream_count + 1);
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

        const uint64_t slot = event_count_.fetch_add(1, std::memory_order_relaxed);
        if (slot >= config_.max_events) {
            dropped_events_.fetch_add(1, std::memory_order_relaxed);
            return;
        }

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
        data_.dropped_events = dropped_events_.load(std::memory_order_relaxed);
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

        data_.dropped_events = dropped_events_.load(std::memory_order_relaxed);
        observe::writeTimeline(writer, data_);
        writer.close();
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
    bool trace_arbitration_ = false;
    bool started_ = false;
    bool written_ = false;
    TimePoint base_time_{};
    size_t scheduler_stream_ = 0;
    /// Recorded streams: slices, per-stream byte arenas, lane names.
    observe::TimelineStreamData data_;
    std::atomic<uint64_t> event_count_{0};
    std::atomic<uint64_t> dropped_events_{0};
};

}  // namespace chronon::sender
