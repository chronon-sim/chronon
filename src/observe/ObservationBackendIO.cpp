// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file ObservationBackendIO.cpp
/// @brief File I/O, text output formatting, Perfetto timeline output, counter
///        CSV writing, and output directory initialization for ObservationBackend.

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <bit>
#include <chrono>
#include <cstring>
#include <iostream>
#include <span>

#include "ArgFormat.hpp"
#include "FormatRegistry.hpp"
#include "ObservationBackend.hpp"
#include "ObserveApi.hpp"

namespace chronon::observe {

// ---------------------------------------------------------------------------
// Text event formatting
// ---------------------------------------------------------------------------

void ObservationBackend::writeEventAsText_(const StructuredRecord* rec, const std::byte* args_data,
                                           size_t args_size, const char* level_str,
                                           LogFileSink& sink) {
    const FormatInfo& fmt_info = FormatRegistry::instance().getFormat(rec->format_id);
    if (fmt_info.format_string.empty()) {
        return;  // Invalid format ID
    }

    std::string_view source_name;
    if (rec->source_id > 0 && rec->source_id < source_name_cache_.size()) {
        source_name = source_name_cache_[rec->source_id];
    }

    if (!source_name.empty()) {
        fmt::format_to(std::back_inserter(sink.buffer), "[{:>10}] [{:>5}] {}: ", rec->cycle,
                       level_str, source_name);
    } else {
        fmt::format_to(std::back_inserter(sink.buffer), "[{:>10}] [{:>5}] ", rec->cycle, level_str);
    }

    reconstructMessageTo_(sink.buffer, fmt_info, rec, args_data, args_size);
    sink.buffer.push_back('\n');

    size_t approx_bytes = 25 + source_name.size() + fmt_info.format_string.size() + args_size * 2;
    local_bytes_written_ += approx_bytes;

    if (sink.buffer.size() >= TEXT_BUFFER_FLUSH_SIZE) {
        sink.flush();
    }
}

// ---------------------------------------------------------------------------
// Perfetto timeline output
// ---------------------------------------------------------------------------

uint64_t ObservationBackend::timelineTrackForSource_(uint16_t source_id) {
    if (source_id >= source_track_uuids_.size()) {
        source_track_uuids_.resize(static_cast<size_t>(source_id) + 1, 0);
    }
    uint64_t& uuid = source_track_uuids_[source_id];
    if (uuid == 0) {
        std::string_view name = "unknown";
        if (source_id > 0 && source_id < source_name_cache_.size()) {
            name = source_name_cache_[source_id];
        }
        uuid = perfetto_writer_->addTrack(name, sim_process_uuid_);
    }
    return uuid;
}

void ObservationBackend::writeTraceToTimeline_(const StructuredRecord* rec,
                                               const std::byte* args_data, size_t args_size) {
    const FormatInfo& fmt_info = FormatRegistry::instance().getFormat(rec->format_id);
    if (fmt_info.format_string.empty()) {
        return;  // Invalid format ID
    }

    timeline_msg_buffer_.clear();
    reconstructMessageTo_(timeline_msg_buffer_, fmt_info, rec, args_data, args_size);

    // 1 cycle is rendered as 1 ns on the timeline axis.
    perfetto_writer_->instant(
        timelineTrackForSource_(rec->source_id), "trace",
        std::string_view(timeline_msg_buffer_.data(), timeline_msg_buffer_.size()), rec->cycle);

    local_bytes_written_ += timeline_msg_buffer_.size() + 24;
}

void ObservationBackend::writeCounterToTimeline_(uint64_t cycle, std::string_view unit_name,
                                                 std::string_view counter_name, uint64_t value) {
    std::string key;
    key.reserve(unit_name.size() + 1 + counter_name.size());
    key.append(unit_name);
    key.push_back('.');
    key.append(counter_name);

    auto [it, inserted] = counter_track_uuids_.try_emplace(key, 0);
    if (inserted) {
        it->second = perfetto_writer_->addCounterTrack(key, /*unit_name=*/{}, sim_process_uuid_);
    }

    perfetto_writer_->counterValue(it->second, cycle, static_cast<int64_t>(value));
    local_bytes_written_ += 24;
}

// ---------------------------------------------------------------------------
// Timeline lane / counter events (TimelineRecord)
// ---------------------------------------------------------------------------

std::string_view ObservationBackend::timelineEventName_(uint16_t name_id) {
    if (name_id >= timeline_event_name_cache_.size()) {
        timeline_event_name_cache_.resize(static_cast<size_t>(name_id) + 1);
    }
    std::string_view& name = timeline_event_name_cache_[name_id];
    if (name.empty() && name_id != 0) {
        name = EventNameRegistry::instance().get(name_id);
    }
    return name;
}

std::string_view ObservationBackend::timelineAnnotationKey_(uint16_t key_id) {
    if (key_id >= timeline_annotation_key_cache_.size()) {
        timeline_annotation_key_cache_.resize(static_cast<size_t>(key_id) + 1);
    }
    std::string_view& key = timeline_annotation_key_cache_[key_id];
    if (key.empty() && key_id != 0) {
        key = AnnotationKeyRegistry::instance().get(key_id);
    }
    return key;
}

std::string_view ObservationBackend::timelineCategoryName_(uint32_t category_mask) {
    const uint32_t user_bits = category_mask & static_cast<uint32_t>(category::USER_CATEGORY_MASK);
    if (user_bits == 0) {
        return "timeline";
    }
    const uint32_t bit = static_cast<uint32_t>(std::countr_zero(user_bits));
    std::string_view& name = timeline_category_names_[bit];
    if (name.empty()) {
        name = CategoryRegistry::instance().nameForBit(bit);
        if (name.empty()) {
            name = "timeline";
        }
    }
    return name;
}

uint64_t ObservationBackend::timelineSlotTrack_(uint32_t track_id, uint16_t slot) {
    if (track_id >= timeline_track_uuids_.size()) {
        timeline_track_uuids_.resize(static_cast<size_t>(track_id) + 1);
    }
    TimelineTrackUuids& uuids = timeline_track_uuids_[track_id];

    const TimelineTrackInfo& info = TimelineTrackRegistry::instance().get(track_id);
    const uint64_t parent = timelineTrackForSource_(info.source_id);

    if (info.kind == TimelineTrackInfo::Kind::Counter) {
        if (uuids.single == 0) {
            uuids.single = perfetto_writer_->addCounterTrack(info.name, info.unit, parent);
        }
        return uuids.single;
    }

    if (info.lanes <= 1) {
        if (uuids.single == 0) {
            std::string_view name = info.name.empty() ? std::string_view("lane") : info.name;
            uuids.single = perfetto_writer_->addTrack(name, parent);
        }
        return uuids.single;
    }

    if (uuids.group == 0) {
        uuids.group = perfetto_writer_->addTrack(info.name, parent);
    }
    if (slot >= uuids.slots.size()) {
        uuids.slots.resize(static_cast<size_t>(slot) + 1, 0);
    }
    uint64_t& slot_uuid = uuids.slots[slot];
    if (slot_uuid == 0) {
        timeline_msg_buffer_.clear();
        fmt::format_to(std::back_inserter(timeline_msg_buffer_), "{}[{}]", info.name, slot);
        slot_uuid = perfetto_writer_->addTrack(
            std::string_view(timeline_msg_buffer_.data(), timeline_msg_buffer_.size()),
            uuids.group);
    }
    return slot_uuid;
}

void ObservationBackend::processTimelineEvent_(const std::byte* data, size_t data_size) {
    if (data_size < sizeof(TimelineRecord)) {
        return;
    }
    TimelineRecord rec;
    std::memcpy(&rec, data, sizeof(TimelineRecord));
    if (data_size < sizeof(TimelineRecord) + rec.arg_count * TIMELINE_ARG_SIZE ||
        rec.arg_count > MAX_TIMELINE_ARGS) {
        return;
    }

    if (rec.cycle > timeline_max_cycle_) {
        timeline_max_cycle_ = rec.cycle;
    }

    const uint64_t span_key = (static_cast<uint64_t>(rec.track_id) << 16) | rec.slot;

    // Span ends address their begin's track via the open-span table; resolving
    // the track here would create tracks for orphan ends that get dropped.
    const auto kind = static_cast<TimelineEventKind>(rec.kind);
    const uint64_t track_uuid =
        (kind == TimelineEventKind::SpanEnd) ? 0 : timelineSlotTrack_(rec.track_id, rec.slot);

    // Decode typed args into writer annotations (keys resolved via registry).
    PerfettoTraceWriter::Annotation annotations[MAX_TIMELINE_ARGS];
    const std::byte* arg_data = data + sizeof(TimelineRecord);
    for (size_t i = 0; i < rec.arg_count; ++i) {
        const TimelineArgValue arg = unpackTimelineArg(arg_data + i * TIMELINE_ARG_SIZE);
        annotations[i].name = timelineAnnotationKey_(arg.key_id);
        annotations[i].bits = arg.bits;
        switch (arg.kind) {
            case TimelineArgKind::Int:
                annotations[i].kind = PerfettoTraceWriter::Annotation::Kind::Int;
                break;
            case TimelineArgKind::Double:
                annotations[i].kind = PerfettoTraceWriter::Annotation::Kind::Double;
                break;
            case TimelineArgKind::Bool:
                annotations[i].kind = PerfettoTraceWriter::Annotation::Kind::Bool;
                break;
            case TimelineArgKind::Pointer:
                annotations[i].kind = PerfettoTraceWriter::Annotation::Kind::Pointer;
                break;
            case TimelineArgKind::Uint:
            default:
                annotations[i].kind = PerfettoTraceWriter::Annotation::Kind::Uint;
                break;
        }
    }
    const std::span<const PerfettoTraceWriter::Annotation> ann_span(annotations, rec.arg_count);

    switch (kind) {
        case TimelineEventKind::Instant:
            perfetto_writer_->instant(track_uuid, timelineCategoryName_(rec.category),
                                      timelineEventName_(rec.name_id), rec.cycle, rec.payload,
                                      ann_span);
            break;

        case TimelineEventKind::SpanBegin: {
            // Hardware slot reuse: a begin on an occupied slot implicitly
            // closes the previous span at this cycle.
            auto [it, inserted] = open_spans_.try_emplace(span_key, track_uuid);
            if (!inserted) {
                perfetto_writer_->sliceEnd(it->second, rec.cycle);
                it->second = track_uuid;
            }
            perfetto_writer_->sliceBegin(track_uuid, timelineCategoryName_(rec.category),
                                         timelineEventName_(rec.name_id), rec.cycle, rec.payload,
                                         ann_span);
            break;
        }

        case TimelineEventKind::SpanEnd: {
            // Ends without a matching begin are dropped: this is what makes a
            // temporally suppressed begin suppress its end as well.
            auto it = open_spans_.find(span_key);
            if (it == open_spans_.end()) {
                break;
            }
            perfetto_writer_->sliceEnd(it->second, rec.cycle);
            open_spans_.erase(it);
            break;
        }

        case TimelineEventKind::CounterSample:
            perfetto_writer_->counterValue(track_uuid, rec.cycle,
                                           static_cast<int64_t>(rec.payload));
            break;

        default:
            break;
    }

    local_bytes_written_ += data_size;
}

/// Appends timeline streams submitted via submitTimeline() (e.g. the scheduler
/// execution timeline) and closes the Perfetto writer. Runs on the worker
/// thread during shutdown, after the final event drain.
void ObservationBackend::finalizeTimeline_() {
    if (!perfetto_writer_) {
        return;
    }

    // Close dangling spans (dropped or never-emitted ends) at the last cycle
    // seen, so the trace renders bounded slices instead of open-ended ones.
    for (const auto& [key, track_uuid] : open_spans_) {
        perfetto_writer_->sliceEnd(track_uuid, timeline_max_cycle_);
    }
    open_spans_.clear();

    std::vector<TimelineStreamData> pending;
    {
        std::lock_guard<std::mutex> lock(timeline_submit_mutex_);
        pending.swap(submitted_timelines_);
    }

    for (const auto& data : pending) {
        writeTimeline(*perfetto_writer_, data);
    }

    perfetto_writer_->flush();
    perfetto_writer_->close();
    timeline_sink_open_.store(false, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// Format-string reconstruction
// ---------------------------------------------------------------------------

std::string ObservationBackend::reconstructMessage_(const FormatInfo& fmt_info,
                                                    const StructuredRecord* rec,
                                                    const std::byte* args_data, size_t args_size) {
    fmt::memory_buffer buf;
    buf.reserve(fmt_info.format_string.size() + args_size * 2);
    reconstructMessageTo_(buf, fmt_info, rec, args_data, args_size);
    return std::string(buf.data(), buf.size());
}

/// Lazy-compiles the format string into segments on first use, then iterates
/// segments instead of scanning char-by-char on every event.
void ObservationBackend::reconstructMessageTo_(fmt::memory_buffer& out, const FormatInfo& fmt_info,
                                               const StructuredRecord* rec,
                                               const std::byte* args_data, size_t args_size) {
    if (!fmt_info.compiled.compiled) {
        fmt_info.compiled.compile(fmt_info.format_string);
    }

    const auto& segments = fmt_info.compiled.segments;
    const std::string& fmt = fmt_info.format_string;
    const std::byte* arg_ptr = args_data;
    const std::byte* arg_end = args_data + args_size;
    size_t arg_index = 0;

    for (const auto& seg : segments) {
        if (seg.type == FormatSegment::Type::Literal) {
            out.append(fmt.data() + seg.start, fmt.data() + seg.start + seg.length);
        } else {
            if (arg_ptr < arg_end && arg_index < rec->arg_count) {
                ArgType arg_type = ArgType::None;
                if (arg_index < fmt_info.arg_count && arg_index < MAX_FORMAT_ARGS) {
                    arg_type = fmt_info.arg_types[arg_index];
                }
                formatArgToFast_(out, arg_ptr, arg_type, seg.hex);
                arg_ptr += argSize_(arg_type, arg_ptr, arg_end);
                arg_index++;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Argument formatting
// ---------------------------------------------------------------------------

std::string ObservationBackend::formatArg_(const std::byte* data, ArgType type,
                                           std::string_view spec) {
    fmt::memory_buffer buf;
    formatArgTo_(buf, data, type, spec);
    return std::string(buf.data(), buf.size());
}

void ObservationBackend::formatArgTo_(fmt::memory_buffer& out, const std::byte* data, ArgType type,
                                      std::string_view spec) {
    bool hex = spec.find('x') != std::string_view::npos || spec.find('X') != std::string_view::npos;
    formatArgToFast_(out, data, type, hex);
}

/// Fast argument formatting with pre-computed hex flag -- avoids re-parsing
/// the format spec string on every call.
void ObservationBackend::formatArgToFast_(fmt::memory_buffer& out, const std::byte* data,
                                          ArgType type, bool hex) {
    switch (type) {
        case ArgType::Int8: {
            int8_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", static_cast<int>(val));
            } else {
                fmt::format_to(std::back_inserter(out), "{}", static_cast<int>(val));
            }
            return;
        }
        case ArgType::Int16: {
            int16_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::Int32: {
            int32_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::Int64: {
            int64_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::UInt8: {
            uint8_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", static_cast<unsigned>(val));
            } else {
                fmt::format_to(std::back_inserter(out), "{}", static_cast<unsigned>(val));
            }
            return;
        }
        case ArgType::UInt16: {
            uint16_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::UInt32: {
            uint32_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::UInt64: {
            uint64_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::Float: {
            float val;
            std::memcpy(&val, data, sizeof(val));
            fmt::format_to(std::back_inserter(out), "{}", val);
            return;
        }
        case ArgType::Double: {
            double val;
            std::memcpy(&val, data, sizeof(val));
            fmt::format_to(std::back_inserter(out), "{}", val);
            return;
        }
        case ArgType::Pointer: {
            const void* val;
            std::memcpy(&val, data, sizeof(val));
            fmt::format_to(std::back_inserter(out), "0x{:x}", reinterpret_cast<uintptr_t>(val));
            return;
        }
        case ArgType::String: {
            const char* str = reinterpret_cast<const char*>(data);
            out.append(str, str + std::char_traits<char>::length(str));
            return;
        }
        case ArgType::Bool: {
            bool val;
            std::memcpy(&val, data, sizeof(val));
            const char* s = val ? "true" : "false";
            out.append(s, s + (val ? 4 : 5));
            return;
        }
        case ArgType::None:
        default:
            out.push_back('?');
            return;
    }
}

size_t ObservationBackend::argSize_(ArgType type, const std::byte* data, const std::byte* end) {
    return argSize(type, data, end);
}

// ---------------------------------------------------------------------------
// Buffer and file flushing
// ---------------------------------------------------------------------------

void ObservationBackend::flushAllTextBuffers_() {
    if (default_sink_) {
        default_sink_->flush();
    }
    for (auto& [name, sink] : custom_sinks_) {
        sink->flush();
    }
}

void ObservationBackend::flushCounterBuffer_() {
    if (counter_buffer_.size() > 0 && counter_file_.is_open()) {
        counter_file_.write(counter_buffer_.data(),
                            static_cast<std::streamsize>(counter_buffer_.size()));
        counter_buffer_.clear();
    }
}

void ObservationBackend::flush_() {
    flushAllTextBuffers_();

    flushCounterBuffer_();

    // Time-based OS flush: only call file.flush() periodically to reduce
    // syscall overhead.
    auto now = std::chrono::steady_clock::now();
    if (now - last_os_flush_time_ >= OS_FLUSH_INTERVAL) {
        if (counter_file_.is_open()) counter_file_.flush();
        if (default_sink_) default_sink_->osFlush();
        for (auto& [name, sink] : custom_sinks_) {
            sink->osFlush();
        }
        if (perfetto_writer_) perfetto_writer_->flush();
        last_os_flush_time_ = now;
    }

    if (local_bytes_written_ > 0) {
        bytes_written_.fetch_add(local_bytes_written_, std::memory_order_relaxed);
        local_bytes_written_ = 0;
    }
}

// ---------------------------------------------------------------------------
// Counter CSV output
// ---------------------------------------------------------------------------

void ObservationBackend::finalizeCounterColumns_() {
    for (const auto& [key, value] : counter_first_batch_) {
        auto [it, inserted] = counter_col_index_.try_emplace(key, counter_columns_.size());
        if (inserted) {
            counter_columns_.push_back(key);
        }
    }

    resolveDerivedCounters_();
}

void ObservationBackend::writeCounterCsvHeader_() {
    counter_file_.open(output_dir_ / "counters.csv");
    if (!counter_file_.is_open()) {
        return;
    }

    fmt::memory_buffer buf;
    fmt::format_to(std::back_inserter(buf), "cycle");
    for (const auto& col : counter_columns_) {
        fmt::format_to(std::back_inserter(buf), ",{}", col);
    }
    for (const auto& rd : resolved_derived_) {
        fmt::format_to(std::back_inserter(buf), ",{}", rd.column_name);
    }
    buf.push_back('\n');
    counter_file_.write(buf.data(), static_cast<std::streamsize>(buf.size()));
}

void ObservationBackend::flushCounterRow_(uint64_t cycle) {
    if (!counter_file_.is_open() || counter_columns_.empty()) {
        return;
    }

    if (!resolved_derived_.empty()) {
        computeDerived_();
    }

    fmt::memory_buffer buf;
    fmt::format_to(std::back_inserter(buf), "{}", cycle);
    for (size_t i = 0; i < counter_columns_.size(); ++i) {
        fmt::format_to(std::back_inserter(buf), ",{}", current_counter_row_[i]);
    }
    for (double v : current_derived_row_) {
        fmt::format_to(std::back_inserter(buf), ",{:.6f}", v);
    }
    buf.push_back('\n');
    counter_file_.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    counter_file_.flush();

    local_bytes_written_ += buf.size();
}

/// Handles the single-batch edge case (only one dump cycle occurred) and
/// flushes the last pending row on shutdown.
void ObservationBackend::finalizeCounterCsv_() {
    if (!counter_csv_streaming_ && !counter_first_batch_.empty()) {
        finalizeCounterColumns_();
        writeCounterCsvHeader_();
        current_counter_row_.assign(counter_columns_.size(), 0);
        for (const auto& [k, v] : counter_first_batch_) {
            auto it = counter_col_index_.find(k);
            if (it != counter_col_index_.end()) {
                current_counter_row_[it->second] = v;
            }
        }
        flushCounterRow_(counter_first_cycle_);
        counter_first_batch_.clear();
    } else if (counter_csv_streaming_ && current_counter_cycle_ != UINT64_MAX) {
        flushCounterRow_(current_counter_cycle_);
    }

    if (config_.counter_csv_format == CounterCsvFormat::Long && !derived_counter_defs_.empty() &&
        long_current_cycle_ != UINT64_MAX && !long_cycle_values_.empty()) {
        emitLongDerivedValues_(long_current_cycle_);
        long_cycle_values_.clear();
        flushCounterBuffer_();
    }
}

// ---------------------------------------------------------------------------
// Derived counters
// ---------------------------------------------------------------------------

void ObservationBackend::setDerivedCounterDefs(std::vector<DerivedCounterDef> defs) {
    derived_counter_defs_ = std::move(defs);
}

void ObservationBackend::resolveDerivedCounters_() {
    resolved_derived_.clear();

    for (const auto& def : derived_counter_defs_) {
        std::vector<size_t> cols;
        cols.reserve(def.source_names.size());
        bool all_found = true;

        for (const auto& src_name : def.source_names) {
            std::string qualified;
            qualified.reserve(def.unit_name.size() + 1 + src_name.size());
            qualified.append(def.unit_name);
            qualified.push_back('.');
            qualified.append(src_name);

            auto it = counter_col_index_.find(qualified);
            if (it != counter_col_index_.end()) {
                cols.push_back(it->second);
            } else {
                std::cerr << "[observe] derived counter '" << def.unit_name << '.'
                          << def.derived_name << "': source '" << qualified << "' not found\n";
                all_found = false;
            }
        }

        if (all_found) {
            ResolvedDerived rd;
            rd.column_name.reserve(def.unit_name.size() + 1 + def.derived_name.size());
            rd.column_name.append(def.unit_name);
            rd.column_name.push_back('.');
            rd.column_name.append(def.derived_name);
            rd.source_cols = std::move(cols);
            rd.compute = def.compute;
            resolved_derived_.push_back(std::move(rd));
        }
    }

    current_derived_row_.resize(resolved_derived_.size(), 0.0);
}

void ObservationBackend::computeDerived_() {
    thread_local std::vector<uint64_t> vals;
    for (size_t i = 0; i < resolved_derived_.size(); ++i) {
        const auto& rd = resolved_derived_[i];
        vals.clear();
        for (size_t col : rd.source_cols) {
            vals.push_back(current_counter_row_[col]);
        }
        current_derived_row_[i] = rd.compute(std::span<const uint64_t>(vals));
    }
}

void ObservationBackend::emitLongDerivedValues_(uint64_t cycle) {
    std::vector<uint64_t> vals;
    for (const auto& def : derived_counter_defs_) {
        vals.clear();
        vals.reserve(def.source_names.size());
        for (const auto& src_name : def.source_names) {
            std::string qualified = def.unit_name + "." + src_name;
            auto it = long_cycle_values_.find(qualified);
            vals.push_back(it != long_cycle_values_.end() ? it->second : 0);
        }
        double result = def.compute(std::span<const uint64_t>(vals));
        fmt::format_to(std::back_inserter(counter_buffer_), "{},{},{},{:.6f}\n", cycle,
                       def.unit_name, def.derived_name, result);
    }
}

// ---------------------------------------------------------------------------
// Output directory initialization
// ---------------------------------------------------------------------------

/// Sets up the timestamped output directory, opens counter CSV, the Perfetto
/// timeline writer, and text log sinks.
void ObservationBackend::initializeOutputDir_() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::string timestamp = fmt::format("{:%Y%m%d_%H%M%S}", *std::localtime(&time));

    output_dir_ = std::filesystem::path(config_.output_dir) / timestamp;
    std::filesystem::create_directories(output_dir_);

    // Create/update "latest" symlink pointing to this run's output directory
    {
        auto latest_link = std::filesystem::path(config_.output_dir) / "latest";
        std::error_code ec;
        std::filesystem::remove(latest_link, ec);
        std::filesystem::create_directory_symlink(output_dir_.filename(), latest_link, ec);
    }

    if (config_.enable_counter_csv && config_.counter_csv_format == CounterCsvFormat::Long) {
        counter_file_.open(output_dir_ / "counters.csv");
        counter_file_ << "cycle,unit,counter_name,value\n";
    }

    if (config_.timeline_enabled) {
        perfetto_writer_ = std::make_unique<PerfettoTraceWriter>();
        // Track caches index into the writer's per-file UUID space; reset them
        // so a restarted backend re-declares its tracks in the new file.
        source_track_uuids_.clear();
        counter_track_uuids_.clear();
        timeline_track_uuids_.clear();
        open_spans_.clear();
        timeline_max_cycle_ = 0;
        sim_process_uuid_ = 0;
        const std::string timeline_file =
            config_.timeline_file.empty() ? std::string("timeline.pftrace") : config_.timeline_file;
        PerfettoTraceWriter::Options writer_options;
        writer_options.compress = config_.timeline_compress;
        if (perfetto_writer_->open(output_dir_ / timeline_file, writer_options)) {
            std::string process_name = config_.simulation_name.empty() ? std::string("Simulation")
                                                                       : config_.simulation_name;
            sim_process_uuid_ = perfetto_writer_->addProcessTrack(process_name, /*pid=*/1);
            timeline_sink_open_.store(true, std::memory_order_release);
        } else {
            std::cerr << "[observe] failed to open timeline file " << timeline_file
                      << " (timeline output disabled)\n";
            perfetto_writer_.reset();
        }
    }

    {
        default_sink_ = std::make_unique<LogFileSink>();
        default_sink_->file.open(output_dir_ / "events.log");
        default_sink_->buffer.reserve(TEXT_BUFFER_FLUSH_SIZE * 2);

        for (auto& sink : channel_sink_) {
            sink = default_sink_.get();
        }

        auto getOrCreateSink = [&](const std::string& filename) -> LogFileSink* {
            auto it = custom_sinks_.find(filename);
            if (it != custom_sinks_.end()) {
                return it->second.get();
            }
            auto sink = std::make_unique<LogFileSink>();
            sink->file.open(output_dir_ / filename);
            sink->buffer.reserve(TEXT_BUFFER_FLUSH_SIZE * 2);
            auto* ptr = sink.get();
            custom_sinks_.emplace(filename, std::move(sink));
            return ptr;
        };

        struct ChannelFileEntry {
            Channel channel;
            const std::string& file;
        };
        const ChannelFileEntry channel_files[] = {
            {Channel::Trace, config_.trace_file}, {Channel::Debug, config_.debug_file},
            {Channel::Info, config_.info_file},   {Channel::Warn, config_.warn_file},
            {Channel::Error, config_.error_file},
        };
        for (const auto& entry : channel_files) {
            if (!entry.file.empty()) {
                channel_sink_[static_cast<size_t>(entry.channel)] = getOrCreateSink(entry.file);
            }
        }
    }
}

}  // namespace chronon::observe
