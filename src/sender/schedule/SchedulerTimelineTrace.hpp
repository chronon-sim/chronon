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
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../core/TickableUnit.hpp"

namespace chronon::sender {

/** @brief Configuration for SchedulerTimelineTrace (Chrome Trace / Perfetto). */
struct SchedulerTimelineTraceConfig {
    bool enabled = false;
    std::string file = "chronon_timeline.json";
    uint64_t max_events = 1'000'000;
    uint64_t start_cycle = 0;
    uint64_t end_cycle = std::numeric_limits<uint64_t>::max();
    bool trace_units = true;
    bool trace_waits = true;
    bool trace_epochs = true;
    bool trace_arbitration = true;
    uint64_t min_duration_ns = 0;
};

/** @brief Chrome Trace / Perfetto timeline writer for scheduler execution. */
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
        streams_.clear();
        streams_.resize(stream_count + 1);  // Last stream is the scheduler lane.
        const size_t reserve_per = config_.max_events / streams_.size();
        for (auto& s : streams_) {
            s.reserve(reserve_per);
        }
        stream_names_.clear();
        stream_names_.reserve(stream_count + 1);

        for (size_t stream = 0; stream < stream_count; ++stream) {
            std::ostringstream name;
            name << "stream " << stream << " (logical worker)";
            stream_names_.push_back(name.str());
        }
        scheduler_stream_ = stream_count;
        stream_names_.push_back("scheduler");
        started_ = true;
    }

    /**
     * @brief Record a duration event in a stream.
     *
     * Each stream must be written by exactly one thread during an epoch. Stream indices
     * map 1:1 to stdexec bulk indices; the scheduler stream is only written from the main
     * thread after sync_wait returns.
     */
    void recordDuration(size_t stream, const char* category, std::string_view name, uint64_t cycle,
                        TimePoint begin, TimePoint end, std::string detail = {}) {
        if (!started_ || stream >= streams_.size()) return;
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

        streams_[stream].push_back(
            {category, std::string(name), cycle, relNs(begin), duration_ns, std::move(detail)});
    }

    size_t schedulerStream() const noexcept { return scheduler_stream_; }
    const std::string& file() const noexcept { return config_.file; }

    void write() {
        if (!enabled() || !started_ || written_) return;
        written_ = true;

        std::filesystem::path output_path(config_.file.empty() ? "chronon_timeline.json"
                                                               : config_.file);
        if (output_path.has_parent_path()) {
            std::error_code ec;
            std::filesystem::create_directories(output_path.parent_path(), ec);
        }

        std::ofstream out(output_path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            std::cerr << "SchedulerTimelineTrace: failed to open " << output_path << "\n";
            return;
        }

        out << "{\"traceEvents\":[\n";
        bool first = true;

        appendProcessMetadata(out, first, "Chronon Scheduler");
        for (size_t stream = 0; stream < stream_names_.size(); ++stream) {
            appendThreadMetadata(out, first, traceTid(stream), "thread_name",
                                 stream_names_[stream]);
        }

        for (size_t sid = 0; sid < streams_.size(); ++sid) {
            for (const auto& event : streams_[sid]) {
                if (!first) out << ",\n";
                first = false;
                out << "{\"name\":\"" << escape(event.name) << "\",\"cat\":\""
                    << escape(event.category)
                    << "\",\"ph\":\"X\",\"pid\":1,\"tid\":" << traceTid(sid) << ",\"ts\":";
                writeMicros(out, event.ts_ns);
                out << ",\"dur\":";
                writeMicros(out, event.dur_ns);
                out << ",\"args\":{\"cycle\":" << event.cycle << ",\"stream\":" << sid;
                if (!event.detail.empty()) {
                    out << ",\"detail\":\"" << escape(event.detail) << "\"";
                }
                out << "}}";
            }
        }

        if (dropped_events_.load(std::memory_order_relaxed) > 0) {
            if (!first) out << ",\n";
            first = false;
            out << "{\"name\":\"dropped events\",\"cat\":\"summary\",\"ph\":\"i\","
                   "\"s\":\"p\",\"pid\":1,\"tid\":"
                << traceTid(scheduler_stream_) << ",\"ts\":0,\"args\":{\"count\":"
                << dropped_events_.load(std::memory_order_relaxed) << "}}";
        }

        out << "\n],\"displayTimeUnit\":\"ns\"}\n";
    }

private:
    struct Event {
        std::string category;
        std::string name;
        uint64_t cycle = 0;
        uint64_t ts_ns = 0;
        uint64_t dur_ns = 0;
        std::string detail;
    };

    uint64_t relNs(TimePoint tp) const {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(tp - base_time_).count());
    }

    static std::string escape(std::string_view input) {
        std::string out;
        out.reserve(input.size());
        for (char ch : input) {
            switch (ch) {
                case '"':
                    out += "\\\"";
                    break;
                case '\\':
                    out += "\\\\";
                    break;
                case '\n':
                    out += "\\n";
                    break;
                case '\r':
                    out += "\\r";
                    break;
                case '\t':
                    out += "\\t";
                    break;
                default:
                    out += ch;
                    break;
            }
        }
        return out;
    }

    static void writeMicros(std::ostream& out, uint64_t ns) {
        const uint64_t us = ns / 1000;
        const uint32_t frac = static_cast<uint32_t>(ns % 1000);
        out << us << '.' << static_cast<char>('0' + frac / 100)
            << static_cast<char>('0' + (frac / 10) % 10) << static_cast<char>('0' + frac % 10);
    }

    static void appendProcessMetadata(std::ostream& out, bool& first, const std::string& value) {
        if (!first) out << ",\n";
        first = false;
        out << "{\"name\":\"process_name\",\"ph\":\"M\",\"pid\":1,\"args\":{\"name\":\""
            << escape(value) << "\"}}";
    }

    static void appendThreadMetadata(std::ostream& out, bool& first, size_t tid,
                                     const char* metadata_name, const std::string& value) {
        if (!first) out << ",\n";
        first = false;
        out << "{\"name\":\"" << metadata_name << "\",\"ph\":\"M\",\"pid\":1,\"tid\":" << tid
            << ",\"args\":{\"name\":\"" << escape(value) << "\"}}";
    }

    /// 1-based: Perfetto treats tid=0 as special in some Chrome Trace views, which can
    /// make worker 0 look like a main thread or collapse labels during inspection.
    static size_t traceTid(size_t stream) noexcept { return stream + 1; }

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
    std::vector<std::vector<Event>> streams_;
    std::vector<std::string> stream_names_;
    std::atomic<uint64_t> event_count_{0};
    std::atomic<uint64_t> dropped_events_{0};
};

}  // namespace chronon::sender
