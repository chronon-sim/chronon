// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file PerfettoTraceWriter.cpp
/// @brief Hand-encoded Perfetto protobuf trace output.
///
/// Field numbers below are taken from the perfetto proto definitions
/// (protos/perfetto/trace/...); protobuf field numbers are stable by contract.

#include "PerfettoTraceWriter.hpp"

#include "TimelineData.hpp"

namespace chronon::observe {

namespace {

// Trace (trace.proto)
constexpr uint32_t TRACE_PACKET = 1;

// TracePacket (trace_packet.proto)
constexpr uint32_t PKT_TIMESTAMP = 8;
constexpr uint32_t PKT_TRUSTED_SEQ_ID = 10;
constexpr uint32_t PKT_TRACK_EVENT = 11;
constexpr uint32_t PKT_SEQUENCE_FLAGS = 13;
constexpr uint32_t PKT_TRACK_DESCRIPTOR = 60;
constexpr uint32_t PKT_FIRST_PACKET_ON_SEQUENCE = 87;
constexpr uint64_t SEQ_INCREMENTAL_STATE_CLEARED = 1;

// TrackDescriptor (track_descriptor.proto)
constexpr uint32_t TD_UUID = 1;
constexpr uint32_t TD_NAME = 2;
constexpr uint32_t TD_PROCESS = 3;
constexpr uint32_t TD_PARENT_UUID = 5;
constexpr uint32_t TD_COUNTER = 8;

// ProcessDescriptor (process_descriptor.proto)
constexpr uint32_t PD_PID = 1;
constexpr uint32_t PD_PROCESS_NAME = 6;

// CounterDescriptor (counter_descriptor.proto)
constexpr uint32_t CD_UNIT_NAME = 6;

// TrackEvent (track_event.proto)
constexpr uint32_t TE_DEBUG_ANNOTATIONS = 4;
constexpr uint32_t TE_TYPE = 9;
constexpr uint32_t TE_TRACK_UUID = 11;
constexpr uint32_t TE_CATEGORIES = 22;
constexpr uint32_t TE_NAME = 23;
constexpr uint32_t TE_COUNTER_VALUE = 30;

constexpr uint64_t TYPE_SLICE_BEGIN = 1;
constexpr uint64_t TYPE_SLICE_END = 2;
constexpr uint64_t TYPE_INSTANT = 3;
constexpr uint64_t TYPE_COUNTER = 4;

// DebugAnnotation (debug_annotation.proto)
constexpr uint32_t DA_UINT_VALUE = 3;
constexpr uint32_t DA_STRING_VALUE = 6;
constexpr uint32_t DA_NAME = 10;

constexpr uint32_t WIRE_VARINT = 0;
constexpr uint32_t WIRE_LEN = 2;

}  // namespace

PerfettoTraceWriter::~PerfettoTraceWriter() { close(); }

void PerfettoTraceWriter::appendVarint(std::string& buf, uint64_t value) {
    while (value >= 0x80) {
        buf.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<char>(value));
}

void PerfettoTraceWriter::appendTag(std::string& buf, uint32_t field, uint32_t wire_type) {
    appendVarint(buf, (static_cast<uint64_t>(field) << 3) | wire_type);
}

void PerfettoTraceWriter::appendVarintField(std::string& buf, uint32_t field, uint64_t value) {
    appendTag(buf, field, WIRE_VARINT);
    appendVarint(buf, value);
}

void PerfettoTraceWriter::appendStringField(std::string& buf, uint32_t field,
                                            std::string_view value) {
    appendTag(buf, field, WIRE_LEN);
    appendVarint(buf, value.size());
    buf.append(value.data(), value.size());
}

void PerfettoTraceWriter::appendMessageField(std::string& buf, uint32_t field,
                                             const std::string& sub) {
    appendTag(buf, field, WIRE_LEN);
    appendVarint(buf, sub.size());
    buf.append(sub);
}

bool PerfettoTraceWriter::open(const std::filesystem::path& path) {
    if (file_.is_open()) {
        return false;
    }
    file_.open(path, std::ios::binary | std::ios::trunc);
    if (!file_.is_open()) {
        return false;
    }

    out_buf_.clear();
    out_buf_.reserve(FLUSH_THRESHOLD * 2);
    next_uuid_ = 1;
    first_packet_ = true;
    events_written_ = 0;
    bytes_written_ = 0;
    return true;
}

void PerfettoTraceWriter::beginPacket_() {
    pkt_.clear();
    appendVarintField(pkt_, PKT_TRUSTED_SEQ_ID, SEQUENCE_ID);
    if (first_packet_) {
        // Mark the sequence start so trace_processor never discards TrackEvents
        // for missing incremental state (we use none, but be explicit).
        appendVarintField(pkt_, PKT_SEQUENCE_FLAGS, SEQ_INCREMENTAL_STATE_CLEARED);
        appendVarintField(pkt_, PKT_FIRST_PACKET_ON_SEQUENCE, 1);
        first_packet_ = false;
    }
}

void PerfettoTraceWriter::commitPacket_() {
    appendTag(out_buf_, TRACE_PACKET, WIRE_LEN);
    appendVarint(out_buf_, pkt_.size());
    out_buf_.append(pkt_);
    maybeFlush_();
}

void PerfettoTraceWriter::emitTrackDescriptor_(const std::string& descriptor) {
    beginPacket_();
    appendMessageField(pkt_, PKT_TRACK_DESCRIPTOR, descriptor);
    commitPacket_();
}

uint64_t PerfettoTraceWriter::addProcessTrack(std::string_view process_name, int32_t pid) {
    const uint64_t uuid = next_uuid_++;
    sub_.clear();
    appendVarintField(sub_, PD_PID, static_cast<uint64_t>(pid));
    appendStringField(sub_, PD_PROCESS_NAME, process_name);

    msg_.clear();
    appendVarintField(msg_, TD_UUID, uuid);
    appendMessageField(msg_, TD_PROCESS, sub_);
    emitTrackDescriptor_(msg_);
    return uuid;
}

uint64_t PerfettoTraceWriter::addTrack(std::string_view name, uint64_t parent_uuid) {
    const uint64_t uuid = next_uuid_++;
    msg_.clear();
    appendVarintField(msg_, TD_UUID, uuid);
    appendStringField(msg_, TD_NAME, name);
    if (parent_uuid != 0) {
        appendVarintField(msg_, TD_PARENT_UUID, parent_uuid);
    }
    emitTrackDescriptor_(msg_);
    return uuid;
}

uint64_t PerfettoTraceWriter::addCounterTrack(std::string_view name, std::string_view unit_name,
                                              uint64_t parent_uuid) {
    const uint64_t uuid = next_uuid_++;
    sub_.clear();
    if (!unit_name.empty()) {
        appendStringField(sub_, CD_UNIT_NAME, unit_name);
    }

    msg_.clear();
    appendVarintField(msg_, TD_UUID, uuid);
    appendStringField(msg_, TD_NAME, name);
    if (parent_uuid != 0) {
        appendVarintField(msg_, TD_PARENT_UUID, parent_uuid);
    }
    appendMessageField(msg_, TD_COUNTER, sub_);
    emitTrackDescriptor_(msg_);
    return uuid;
}

void PerfettoTraceWriter::sliceComplete(uint64_t track_uuid, std::string_view category,
                                        std::string_view name, uint64_t ts_ns, uint64_t dur_ns,
                                        uint64_t cycle, std::string_view detail) {
    if (!isOpen()) {
        return;
    }

    // SLICE_BEGIN carries the name, category, and annotations.
    msg_.clear();
    appendVarintField(msg_, TE_TYPE, TYPE_SLICE_BEGIN);
    appendVarintField(msg_, TE_TRACK_UUID, track_uuid);
    if (!category.empty()) {
        appendStringField(msg_, TE_CATEGORIES, category);
    }
    appendStringField(msg_, TE_NAME, name);

    sub_.clear();
    appendStringField(sub_, DA_NAME, "cycle");
    appendVarintField(sub_, DA_UINT_VALUE, cycle);
    appendMessageField(msg_, TE_DEBUG_ANNOTATIONS, sub_);

    if (!detail.empty()) {
        sub_.clear();
        appendStringField(sub_, DA_NAME, "detail");
        appendStringField(sub_, DA_STRING_VALUE, detail);
        appendMessageField(msg_, TE_DEBUG_ANNOTATIONS, sub_);
    }

    beginPacket_();
    appendVarintField(pkt_, PKT_TIMESTAMP, ts_ns);
    appendMessageField(pkt_, PKT_TRACK_EVENT, msg_);
    commitPacket_();

    // SLICE_END only needs the track and timestamp.
    msg_.clear();
    appendVarintField(msg_, TE_TYPE, TYPE_SLICE_END);
    appendVarintField(msg_, TE_TRACK_UUID, track_uuid);

    beginPacket_();
    appendVarintField(pkt_, PKT_TIMESTAMP, ts_ns + dur_ns);
    appendMessageField(pkt_, PKT_TRACK_EVENT, msg_);
    commitPacket_();

    ++events_written_;
}

void PerfettoTraceWriter::instant(uint64_t track_uuid, std::string_view category,
                                  std::string_view name, uint64_t ts_ns) {
    if (!isOpen()) {
        return;
    }

    msg_.clear();
    appendVarintField(msg_, TE_TYPE, TYPE_INSTANT);
    appendVarintField(msg_, TE_TRACK_UUID, track_uuid);
    if (!category.empty()) {
        appendStringField(msg_, TE_CATEGORIES, category);
    }
    appendStringField(msg_, TE_NAME, name);

    beginPacket_();
    appendVarintField(pkt_, PKT_TIMESTAMP, ts_ns);
    appendMessageField(pkt_, PKT_TRACK_EVENT, msg_);
    commitPacket_();

    ++events_written_;
}

void PerfettoTraceWriter::counterValue(uint64_t track_uuid, uint64_t ts_ns, int64_t value) {
    if (!isOpen()) {
        return;
    }

    msg_.clear();
    appendVarintField(msg_, TE_TYPE, TYPE_COUNTER);
    appendVarintField(msg_, TE_TRACK_UUID, track_uuid);
    // int64 fields use plain two's-complement varint encoding (not zigzag).
    appendVarintField(msg_, TE_COUNTER_VALUE, static_cast<uint64_t>(value));

    beginPacket_();
    appendVarintField(pkt_, PKT_TIMESTAMP, ts_ns);
    appendMessageField(pkt_, PKT_TRACK_EVENT, msg_);
    commitPacket_();

    ++events_written_;
}

void PerfettoTraceWriter::maybeFlush_() {
    if (out_buf_.size() >= FLUSH_THRESHOLD) {
        flush();
    }
}

void PerfettoTraceWriter::flush() {
    if (!file_.is_open() || out_buf_.empty()) {
        return;
    }
    file_.write(out_buf_.data(), static_cast<std::streamsize>(out_buf_.size()));
    bytes_written_ += out_buf_.size();
    out_buf_.clear();
    file_.flush();
}

void PerfettoTraceWriter::close() {
    if (!file_.is_open()) {
        return;
    }
    flush();
    file_.close();
}

void writeTimeline(PerfettoTraceWriter& writer, const TimelineStreamData& data) {
    if (!writer.isOpen() || data.streams.empty()) {
        return;
    }

    const uint64_t process_uuid = writer.addProcessTrack(data.process_name, data.pid);

    std::vector<uint64_t> stream_tracks;
    stream_tracks.reserve(data.streams.size());
    for (size_t sid = 0; sid < data.streams.size(); ++sid) {
        std::string_view name =
            sid < data.stream_names.size() ? std::string_view(data.stream_names[sid]) : "stream";
        stream_tracks.push_back(writer.addTrack(name, process_uuid));
    }

    static const std::string empty_arena;
    for (size_t sid = 0; sid < data.streams.size(); ++sid) {
        const std::string& arena = sid < data.arenas.size() ? data.arenas[sid] : empty_arena;
        const auto slice = [&arena](uint32_t off, uint32_t len) {
            return std::string_view(arena.data() + off, len);
        };
        for (const auto& event : data.streams[sid]) {
            writer.sliceComplete(stream_tracks[sid], slice(event.cat_off, event.cat_len),
                                 slice(event.name_off, event.name_len), event.ts_ns, event.dur_ns,
                                 event.cycle, slice(event.detail_off, event.detail_len));
        }
    }

    if (data.dropped_events > 0 && !stream_tracks.empty()) {
        std::string note = "dropped events: " + std::to_string(data.dropped_events);
        writer.instant(stream_tracks.back(), "summary", note, 0);
    }
}

}  // namespace chronon::observe
