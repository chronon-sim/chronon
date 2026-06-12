// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file PerfettoTraceWriter.cpp
/// @brief Perfetto timeline output via the SDK's protozero message writers.
///
/// Uses the amalgamated Perfetto SDK only for protobuf encoding
/// (protozero::HeapBuffered + perfetto::protos::pbzero::Trace); no tracing
/// session, backend, or category registration. Packets accumulate in the
/// heap-buffered Trace message and flush to the file periodically — a Perfetto
/// trace is a plain concatenation of serialized packets, so incremental writes
/// stay valid.

#include "PerfettoTraceWriter.hpp"

#include <perfetto.h>

#include <fstream>

#include "TimelineData.hpp"

namespace chronon::observe {

namespace {

namespace pbz = ::perfetto::protos::pbzero;

constexpr uint32_t SEQUENCE_ID = 1;
/// Flush the packet buffer to disk once this many packets accumulate.
constexpr size_t FLUSH_PACKET_COUNT = 4096;

protozero::ConstChars chars(std::string_view s) { return {s.data(), s.size()}; }

}  // namespace

struct PerfettoTraceWriter::Impl {
    std::ofstream file;
    protozero::HeapBuffered<pbz::Trace> trace;
    size_t packets_buffered = 0;
    bool first_packet = true;

    /// Starts the next TracePacket with the sequence id (+ start-of-sequence
    /// flags on the first packet of the file).
    pbz::TracePacket* newPacket() {
        auto* pkt = trace->add_packet();
        pkt->set_trusted_packet_sequence_id(SEQUENCE_ID);
        if (first_packet) {
            pkt->set_sequence_flags(pbz::TracePacket::SEQ_INCREMENTAL_STATE_CLEARED);
            pkt->set_first_packet_on_sequence(true);
            first_packet = false;
        }
        ++packets_buffered;
        return pkt;
    }
};

PerfettoTraceWriter::PerfettoTraceWriter() : impl_(std::make_unique<Impl>()) {}

PerfettoTraceWriter::~PerfettoTraceWriter() { close(); }

bool PerfettoTraceWriter::isOpen() const noexcept { return impl_->file.is_open(); }

bool PerfettoTraceWriter::open(const std::filesystem::path& path) {
    if (impl_->file.is_open()) {
        return false;
    }
    impl_->file.open(path, std::ios::binary | std::ios::trunc);
    if (!impl_->file.is_open()) {
        return false;
    }

    impl_->trace.Reset();
    impl_->packets_buffered = 0;
    impl_->first_packet = true;
    next_uuid_ = 1;
    events_written_ = 0;
    bytes_written_ = 0;
    return true;
}

uint64_t PerfettoTraceWriter::addProcessTrack(std::string_view process_name, int32_t pid) {
    const uint64_t uuid = next_uuid_++;
    auto* td = impl_->newPacket()->set_track_descriptor();
    td->set_uuid(uuid);
    auto* process = td->set_process();
    process->set_pid(pid);
    process->set_process_name(chars(process_name));
    return uuid;
}

uint64_t PerfettoTraceWriter::addTrack(std::string_view name, uint64_t parent_uuid) {
    const uint64_t uuid = next_uuid_++;
    auto* td = impl_->newPacket()->set_track_descriptor();
    td->set_uuid(uuid);
    td->set_name(chars(name));
    if (parent_uuid != 0) {
        td->set_parent_uuid(parent_uuid);
    }
    return uuid;
}

uint64_t PerfettoTraceWriter::addCounterTrack(std::string_view name, std::string_view unit_name,
                                              uint64_t parent_uuid) {
    const uint64_t uuid = next_uuid_++;
    auto* td = impl_->newPacket()->set_track_descriptor();
    td->set_uuid(uuid);
    td->set_name(chars(name));
    if (parent_uuid != 0) {
        td->set_parent_uuid(parent_uuid);
    }
    auto* counter = td->set_counter();
    if (!unit_name.empty()) {
        counter->set_unit_name(chars(unit_name));
    }
    return uuid;
}

void PerfettoTraceWriter::sliceComplete(uint64_t track_uuid, std::string_view category,
                                        std::string_view name, uint64_t ts_ns, uint64_t dur_ns,
                                        uint64_t cycle, std::string_view detail) {
    if (!isOpen()) {
        return;
    }

    // SLICE_BEGIN carries the name, category, and annotations.
    {
        auto* pkt = impl_->newPacket();
        pkt->set_timestamp(ts_ns);
        auto* event = pkt->set_track_event();
        event->set_type(pbz::TrackEvent::TYPE_SLICE_BEGIN);
        event->set_track_uuid(track_uuid);
        if (!category.empty()) {
            event->add_categories(chars(category));
        }
        event->set_name(chars(name));

        auto* cycle_ann = event->add_debug_annotations();
        cycle_ann->set_name("cycle");
        cycle_ann->set_uint_value(cycle);

        if (!detail.empty()) {
            auto* detail_ann = event->add_debug_annotations();
            detail_ann->set_name("detail");
            detail_ann->set_string_value(chars(detail));
        }
    }

    // SLICE_END only needs the track and timestamp.
    {
        auto* pkt = impl_->newPacket();
        pkt->set_timestamp(ts_ns + dur_ns);
        auto* event = pkt->set_track_event();
        event->set_type(pbz::TrackEvent::TYPE_SLICE_END);
        event->set_track_uuid(track_uuid);
    }

    ++events_written_;
    if (impl_->packets_buffered >= FLUSH_PACKET_COUNT) {
        flush();
    }
}

void PerfettoTraceWriter::instant(uint64_t track_uuid, std::string_view category,
                                  std::string_view name, uint64_t ts_ns) {
    if (!isOpen()) {
        return;
    }

    auto* pkt = impl_->newPacket();
    pkt->set_timestamp(ts_ns);
    auto* event = pkt->set_track_event();
    event->set_type(pbz::TrackEvent::TYPE_INSTANT);
    event->set_track_uuid(track_uuid);
    if (!category.empty()) {
        event->add_categories(chars(category));
    }
    event->set_name(chars(name));

    ++events_written_;
    if (impl_->packets_buffered >= FLUSH_PACKET_COUNT) {
        flush();
    }
}

void PerfettoTraceWriter::counterValue(uint64_t track_uuid, uint64_t ts_ns, int64_t value) {
    if (!isOpen()) {
        return;
    }

    auto* pkt = impl_->newPacket();
    pkt->set_timestamp(ts_ns);
    auto* event = pkt->set_track_event();
    event->set_type(pbz::TrackEvent::TYPE_COUNTER);
    event->set_track_uuid(track_uuid);
    event->set_counter_value(value);

    ++events_written_;
    if (impl_->packets_buffered >= FLUSH_PACKET_COUNT) {
        flush();
    }
}

void PerfettoTraceWriter::flush() {
    if (!impl_->file.is_open()) {
        return;
    }
    if (impl_->packets_buffered > 0) {
        // Serialized Trace messages concatenate into a valid Perfetto trace,
        // so each flushed batch is appended as-is.
        std::vector<uint8_t> bytes = impl_->trace.SerializeAsArray();
        impl_->file.write(reinterpret_cast<const char*>(bytes.data()),
                          static_cast<std::streamsize>(bytes.size()));
        bytes_written_ += bytes.size();
        impl_->trace.Reset();
        impl_->packets_buffered = 0;
    }
    impl_->file.flush();
}

void PerfettoTraceWriter::close() {
    if (!impl_->file.is_open()) {
        return;
    }
    flush();
    impl_->file.close();
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
