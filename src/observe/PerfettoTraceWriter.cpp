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
/// stay valid even with per-batch compressed_packets wrapping.

#include "PerfettoTraceWriter.hpp"

#include <perfetto.h>
#include <zlib.h>

#include <fstream>
#include <iostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "TimelineData.hpp"

namespace chronon::observe {

namespace {

namespace pbz = ::perfetto::protos::pbzero;

/// Simulation-domain sequence: cycle clock, interning, incremental timestamps.
constexpr uint32_t SIM_SEQUENCE_ID = 1;
/// Wall-clock sequence: scheduler execution timeline, absolute boot-clock ns.
constexpr uint32_t WALL_SEQUENCE_ID = 2;
/// Carrier sequence for compressed_packets wrapper packets (no event state).
constexpr uint32_t WRAPPER_SEQUENCE_ID = 3;

/// First sequence-scoped custom clock id allowed by the Perfetto data model.
constexpr uint32_t CYCLE_CLOCK_ID = 64;
constexpr uint32_t BOOT_CLOCK_ID = static_cast<uint32_t>(pbz::BuiltinClock::BUILTIN_CLOCK_BOOTTIME);

/// Flush the packet buffer to disk once this many packets accumulate.
constexpr size_t FLUSH_PACKET_COUNT = 4096;

protozero::ConstChars chars(std::string_view s) { return {s.data(), s.size()}; }

}  // namespace

struct PerfettoTraceWriter::Impl {
    std::ofstream file;
    protozero::HeapBuffered<pbz::Trace> trace;
    size_t packets_buffered = 0;
    Options options;

    /// String → iid map for one InternedData field; iids restart at 1 after
    /// every SEQ_INCREMENTAL_STATE_CLEARED.
    struct InternTable {
        std::unordered_map<std::string, uint64_t> map;

        void clear() { map.clear(); }

        /// @return {iid, inserted}; inserted means the caller must emit the
        /// InternedData entry on the current packet.
        std::pair<uint64_t, bool> intern(std::string_view s) {
            auto [it, inserted] = map.try_emplace(std::string(s), map.size() + 1);
            return {it->second, inserted};
        }
    };

    struct SequenceState {
        SequenceState(uint32_t id_, bool uses_cycle_clock_)
            : id(id_), uses_cycle_clock(uses_cycle_clock_) {}

        uint32_t id;
        bool uses_cycle_clock;
        bool first_packet = true;
        bool needs_state_clear = true;
        uint64_t last_cycle = 0;  ///< Incremental clock state (cycle-clock sequences).
        uint64_t max_cycle = 0;   ///< Monotonic checkpoint baseline for clock snapshots.
        size_t packets_since_clear = 0;
        InternTable event_names;
        InternTable categories;
        InternTable annotation_names;

        void reset() {
            first_packet = true;
            needs_state_clear = true;
            last_cycle = 0;
            max_cycle = 0;
            packets_since_clear = 0;
            event_names.clear();
            categories.clear();
            annotation_names.clear();
        }
    };

    SequenceState sim{SIM_SEQUENCE_ID, /*uses_cycle_clock=*/true};
    SequenceState wall{WALL_SEQUENCE_ID, /*uses_cycle_clock=*/false};

    /// Emits the SEQ_INCREMENTAL_STATE_CLEARED packet that (re)starts @p seq:
    /// resets intern tables and, on the cycle-clock sequence, declares the
    /// incremental cycle clock paired 1:1 with the boot clock and makes it the
    /// sequence default via TracePacketDefaults.
    void emitStateClear(SequenceState& seq) {
        auto* pkt = trace->add_packet();
        pkt->set_trusted_packet_sequence_id(seq.id);
        pkt->set_sequence_flags(pbz::TracePacket::SEQ_INCREMENTAL_STATE_CLEARED);
        if (seq.first_packet) {
            pkt->set_first_packet_on_sequence(true);
            seq.first_packet = false;
        }
        if (seq.uses_cycle_clock) {
            // Snapshot at max_cycle (not 0) so successive checkpoints stay
            // monotonic for the reader's clock tracker.
            auto* snapshot = pkt->set_clock_snapshot();
            auto* cycle_clock = snapshot->add_clocks();
            cycle_clock->set_clock_id(CYCLE_CLOCK_ID);
            cycle_clock->set_timestamp(seq.max_cycle);
            cycle_clock->set_is_incremental(true);
            auto* boot_clock = snapshot->add_clocks();
            boot_clock->set_clock_id(BOOT_CLOCK_ID);
            boot_clock->set_timestamp(seq.max_cycle);
            auto* defaults = pkt->set_trace_packet_defaults();
            defaults->set_timestamp_clock_id(CYCLE_CLOCK_ID);
            seq.last_cycle = seq.max_cycle;
        }
        seq.event_names.clear();
        seq.categories.clear();
        seq.annotation_names.clear();
        seq.packets_since_clear = 0;
        seq.needs_state_clear = false;
        ++packets_buffered;
    }

    /// Starts a packet on @p seq, emitting the state-clear packet first when
    /// the sequence is new or a checkpoint is due.
    pbz::TracePacket* newPacket(SequenceState& seq, bool needs_incremental_state) {
        if (seq.packets_since_clear >= options.checkpoint_interval_packets ||
            seq.event_names.map.size() >= options.max_interned_strings ||
            seq.categories.map.size() >= options.max_interned_strings ||
            seq.annotation_names.map.size() >= options.max_interned_strings) {
            seq.needs_state_clear = true;
        }
        if (seq.needs_state_clear) {
            emitStateClear(seq);
        }
        auto* pkt = trace->add_packet();
        pkt->set_trusted_packet_sequence_id(seq.id);
        if (needs_incremental_state) {
            pkt->set_sequence_flags(pbz::TracePacket::SEQ_NEEDS_INCREMENTAL_STATE);
        }
        ++packets_buffered;
        ++seq.packets_since_clear;
        return pkt;
    }

    /// Starts a simulation-domain event packet stamped at @p cycle: a varint
    /// delta on the incremental cycle clock when cycles are monotonic, or an
    /// absolute boot-clock timestamp otherwise (reorder-buffer force-flushes
    /// and non-reordered multi-queue drains can go backwards).
    pbz::TracePacket* newSimEventPacket(uint64_t cycle) {
        auto* pkt = newPacket(sim, /*needs_incremental_state=*/true);
        if (cycle >= sim.last_cycle) {
            pkt->set_timestamp(cycle - sim.last_cycle);
            sim.last_cycle = cycle;
        } else {
            pkt->set_timestamp(cycle);
            pkt->set_timestamp_clock_id(BOOT_CLOCK_ID);
        }
        if (cycle > sim.max_cycle) {
            sim.max_cycle = cycle;
        }
        return pkt;
    }

    /// Collects the InternedData entries for one event up front (the
    /// interned_data submessage must be complete before set_track_event opens
    /// the next nested field on the packet).
    struct EventStrings {
        uint64_t name_iid = 0;
        uint64_t category_iid = 0;
        bool name_new = false;
        bool category_new = false;
    };

    EventStrings internEventStrings(SequenceState& seq, pbz::TracePacket* pkt,
                                    std::string_view category, std::string_view name,
                                    std::initializer_list<std::string_view> annotation_names_used,
                                    uint64_t* annotation_iids) {
        EventStrings out;
        std::tie(out.name_iid, out.name_new) = seq.event_names.intern(name);
        if (!category.empty()) {
            std::tie(out.category_iid, out.category_new) = seq.categories.intern(category);
        }

        bool any_annotation_new = false;
        std::pair<uint64_t, bool> ann[4];
        size_t i = 0;
        for (std::string_view ann_name : annotation_names_used) {
            ann[i] = seq.annotation_names.intern(ann_name);
            annotation_iids[i] = ann[i].first;
            any_annotation_new |= ann[i].second;
            ++i;
        }

        if (out.name_new || out.category_new || any_annotation_new) {
            auto* interned = pkt->set_interned_data();
            if (out.category_new) {
                auto* entry = interned->add_event_categories();
                entry->set_iid(out.category_iid);
                entry->set_name(chars(category));
            }
            if (out.name_new) {
                auto* entry = interned->add_event_names();
                entry->set_iid(out.name_iid);
                entry->set_name(chars(name));
            }
            i = 0;
            for (std::string_view ann_name : annotation_names_used) {
                if (ann[i].second) {
                    auto* entry = interned->add_debug_annotation_names();
                    entry->set_iid(ann[i].first);
                    entry->set_name(chars(ann_name));
                }
                ++i;
            }
        }
        return out;
    }
};

PerfettoTraceWriter::PerfettoTraceWriter() : impl_(std::make_unique<Impl>()) {}

PerfettoTraceWriter::~PerfettoTraceWriter() { close(); }

bool PerfettoTraceWriter::isOpen() const noexcept { return impl_->file.is_open(); }

bool PerfettoTraceWriter::open(const std::filesystem::path& path, const Options& options) {
    if (impl_->file.is_open()) {
        return false;
    }
    impl_->file.open(path, std::ios::binary | std::ios::trunc);
    if (!impl_->file.is_open()) {
        return false;
    }

    impl_->trace.Reset();
    impl_->packets_buffered = 0;
    impl_->options = options;
    impl_->sim.reset();
    impl_->wall.reset();
    next_uuid_ = 1;
    events_written_ = 0;
    bytes_written_ = 0;
    return true;
}

uint64_t PerfettoTraceWriter::addProcessTrack(std::string_view process_name, int32_t pid) {
    const uint64_t uuid = next_uuid_++;
    auto* td =
        impl_->newPacket(impl_->sim, /*needs_incremental_state=*/false)->set_track_descriptor();
    td->set_uuid(uuid);
    auto* process = td->set_process();
    process->set_pid(pid);
    process->set_process_name(chars(process_name));
    return uuid;
}

uint64_t PerfettoTraceWriter::addTrack(std::string_view name, uint64_t parent_uuid) {
    const uint64_t uuid = next_uuid_++;
    auto* td =
        impl_->newPacket(impl_->sim, /*needs_incremental_state=*/false)->set_track_descriptor();
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
    auto* td =
        impl_->newPacket(impl_->sim, /*needs_incremental_state=*/false)->set_track_descriptor();
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
        auto* pkt = impl_->newPacket(impl_->wall, /*needs_incremental_state=*/true);
        pkt->set_timestamp(ts_ns);

        uint64_t annotation_iids[2] = {0, 0};
        auto strings = detail.empty()
                           ? impl_->internEventStrings(impl_->wall, pkt, category, name, {"cycle"},
                                                       annotation_iids)
                           : impl_->internEventStrings(impl_->wall, pkt, category, name,
                                                       {"cycle", "detail"}, annotation_iids);

        auto* event = pkt->set_track_event();
        event->set_type(pbz::TrackEvent::TYPE_SLICE_BEGIN);
        event->set_track_uuid(track_uuid);
        if (strings.category_iid != 0) {
            event->add_category_iids(strings.category_iid);
        }
        event->set_name_iid(strings.name_iid);

        auto* cycle_ann = event->add_debug_annotations();
        cycle_ann->set_name_iid(annotation_iids[0]);
        cycle_ann->set_uint_value(cycle);

        if (!detail.empty()) {
            auto* detail_ann = event->add_debug_annotations();
            detail_ann->set_name_iid(annotation_iids[1]);
            detail_ann->set_string_value(chars(detail));
        }
    }

    // SLICE_END only needs the track and timestamp.
    {
        auto* pkt = impl_->newPacket(impl_->wall, /*needs_incremental_state=*/true);
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

void PerfettoTraceWriter::wallInstant(uint64_t track_uuid, std::string_view category,
                                      std::string_view name, uint64_t ts_ns) {
    if (!isOpen()) {
        return;
    }

    auto* pkt = impl_->newPacket(impl_->wall, /*needs_incremental_state=*/true);
    pkt->set_timestamp(ts_ns);
    auto strings = impl_->internEventStrings(impl_->wall, pkt, category, name, {}, nullptr);
    auto* event = pkt->set_track_event();
    event->set_type(pbz::TrackEvent::TYPE_INSTANT);
    event->set_track_uuid(track_uuid);
    if (strings.category_iid != 0) {
        event->add_category_iids(strings.category_iid);
    }
    event->set_name_iid(strings.name_iid);

    ++events_written_;
    if (impl_->packets_buffered >= FLUSH_PACKET_COUNT) {
        flush();
    }
}

void PerfettoTraceWriter::instant(uint64_t track_uuid, std::string_view category,
                                  std::string_view name, uint64_t cycle) {
    if (!isOpen()) {
        return;
    }

    auto* pkt = impl_->newSimEventPacket(cycle);
    auto strings = impl_->internEventStrings(impl_->sim, pkt, category, name, {}, nullptr);
    auto* event = pkt->set_track_event();
    event->set_type(pbz::TrackEvent::TYPE_INSTANT);
    event->set_track_uuid(track_uuid);
    if (strings.category_iid != 0) {
        event->add_category_iids(strings.category_iid);
    }
    event->set_name_iid(strings.name_iid);

    ++events_written_;
    if (impl_->packets_buffered >= FLUSH_PACKET_COUNT) {
        flush();
    }
}

void PerfettoTraceWriter::counterValue(uint64_t track_uuid, uint64_t cycle, int64_t value) {
    if (!isOpen()) {
        return;
    }

    auto* pkt = impl_->newSimEventPacket(cycle);
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
        // so each flushed batch is appended as-is (raw or wrapped in one
        // compressed_packets carrier packet).
        std::vector<uint8_t> bytes = impl_->trace.SerializeAsArray();
        impl_->trace.Reset();
        impl_->packets_buffered = 0;

        bool wrote_compressed = false;
        if (impl_->options.compress) {
            uLongf compressed_size = compressBound(static_cast<uLong>(bytes.size()));
            std::vector<uint8_t> compressed(compressed_size);
            // compress2 emits a zlib stream, matching Perfetto's own
            // compressed_packets producer; readers auto-detect the format.
            int rc = compress2(compressed.data(), &compressed_size, bytes.data(),
                               static_cast<uLong>(bytes.size()), Z_DEFAULT_COMPRESSION);
            if (rc == Z_OK) {
                protozero::HeapBuffered<pbz::Trace> wrapper;
                auto* pkt = wrapper->add_packet();
                pkt->set_trusted_packet_sequence_id(WRAPPER_SEQUENCE_ID);
                pkt->set_compressed_packets(compressed.data(), compressed_size);
                std::vector<uint8_t> wrapped = wrapper.SerializeAsArray();
                impl_->file.write(reinterpret_cast<const char*>(wrapped.data()),
                                  static_cast<std::streamsize>(wrapped.size()));
                bytes_written_ += wrapped.size();
                wrote_compressed = true;
            } else {
                std::cerr << "[observe] timeline deflate failed (rc=" << rc
                          << "); writing batch uncompressed\n";
            }
        }
        if (!wrote_compressed) {
            impl_->file.write(reinterpret_cast<const char*>(bytes.data()),
                              static_cast<std::streamsize>(bytes.size()));
            bytes_written_ += bytes.size();
        }
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
        writer.wallInstant(stream_tracks.back(), "summary", note, 0);
    }
}

}  // namespace chronon::observe
