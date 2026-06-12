// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file test_perfetto_trace_writer.cpp
/// @brief Tests for the minimal Perfetto protobuf writer: walks the emitted
///        wire format with an independent decoder and checks packet structure,
///        track-event payloads, interning, the incremental cycle clock,
///        compressed_packets batches, and the timeline stream export.

#include <zlib.h>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "observe/PerfettoTraceWriter.hpp"
#include "observe/TimelineData.hpp"

using namespace chronon::observe;

// Always-evaluated check: unlike CHECK(), survives NDEBUG builds, so the test
// still verifies in Release and -Werror sees no unused result variables.
#define CHECK(cond)                                                                         \
    do {                                                                                    \
        if (!(cond)) {                                                                      \
            std::cerr << "CHECK failed at " << __FILE__ << ":" << __LINE__ << ": " << #cond \
                      << "\n";                                                              \
            std::abort();                                                                   \
        }                                                                                   \
    } while (0)

namespace {

// ---------------------------------------------------------------------------
// Minimal independent protobuf wire-format decoder (varint + length-delimited)
// ---------------------------------------------------------------------------

struct Decoder {
    const uint8_t* p;
    const uint8_t* end;

    bool done() const { return p >= end; }

    uint64_t varint() {
        uint64_t value = 0;
        int shift = 0;
        while (p < end) {
            uint8_t byte = *p++;
            value |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) return value;
            shift += 7;
            CHECK(shift < 64 && "varint too long");
        }
        CHECK(false && "truncated varint");
        return value;
    }

    // Returns {field_number, wire_type}.
    std::pair<uint32_t, uint32_t> tag() {
        uint64_t t = varint();
        return {static_cast<uint32_t>(t >> 3), static_cast<uint32_t>(t & 0x7)};
    }

    Decoder lenDelimited() {
        uint64_t len = varint();
        CHECK(p + len <= end && "length-delimited field overruns buffer");
        Decoder sub{p, p + len};
        p += len;
        return sub;
    }

    std::string str() {
        Decoder sub = lenDelimited();
        return std::string(reinterpret_cast<const char*>(sub.p),
                           static_cast<size_t>(sub.end - sub.p));
    }

    void skip(uint32_t wire_type) {
        switch (wire_type) {
            case 0:
                varint();
                break;
            case 2:
                lenDelimited();
                break;
            default:
                CHECK(false && "unexpected wire type");
        }
    }
};

/// zlib-inflates one compressed_packets payload (a serialized Trace fragment).
std::vector<uint8_t> inflateBytes(const uint8_t* data, size_t size) {
    std::vector<uint8_t> out(size * 8 + 4096);
    z_stream zs{};
    CHECK(inflateInit(&zs) == Z_OK);
    zs.next_in = const_cast<Bytef*>(data);
    zs.avail_in = static_cast<uInt>(size);
    size_t written = 0;
    for (;;) {
        zs.next_out = out.data() + written;
        zs.avail_out = static_cast<uInt>(out.size() - written);
        int rc = inflate(&zs, Z_NO_FLUSH);
        written = out.size() - zs.avail_out;
        if (rc == Z_STREAM_END) break;
        CHECK((rc == Z_OK || rc == Z_BUF_ERROR) && "inflate failed");
        if (zs.avail_out == 0) {
            out.resize(out.size() * 2);
        } else {
            CHECK(rc != Z_BUF_ERROR && "truncated deflate stream");
        }
    }
    inflateEnd(&zs);
    out.resize(written);
    return out;
}

// Decoded views of the packet subset the writer emits.
struct DecodedTrackEvent {
    uint32_t sequence_id = 0;
    uint64_t timestamp = 0;      ///< Resolved (absolute) timestamp / cycle.
    uint64_t raw_timestamp = 0;  ///< On-the-wire value (delta on incremental clocks).
    /// Set when the packet carried an explicit timestamp_clock_id override.
    std::optional<uint32_t> explicit_clock_id;
    uint64_t type = 0;
    uint64_t track_uuid = 0;
    std::string name;
    std::string category;
    std::optional<int64_t> counter_value;
    std::map<std::string, uint64_t> uint_annotations;
    std::map<std::string, std::string> string_annotations;
    bool name_was_interned = false;
};

struct DecodedTrack {
    uint64_t uuid = 0;
    uint64_t parent_uuid = 0;
    std::string name;
    bool is_process = false;
    bool is_counter = false;
    std::string process_name;
};

struct DecodedClock {
    uint32_t clock_id = 0;
    uint64_t timestamp = 0;
    bool is_incremental = false;
};

struct DecodedTrace {
    std::vector<DecodedTrack> tracks;
    std::vector<DecodedTrackEvent> events;
    bool saw_sequence_start = false;
    size_t compressed_wrappers = 0;
    /// sequence id → count of SEQ_INCREMENTAL_STATE_CLEARED packets.
    std::map<uint32_t, size_t> state_clears;
    /// sequence id → total InternedData entries emitted (all three tables).
    std::map<uint32_t, size_t> interned_entries;
    std::vector<DecodedClock> cycle_clock_snapshots;
};

/// Per-sequence decoder state mirroring Perfetto's incremental-state rules.
struct SequenceDecodeState {
    std::map<uint64_t, std::string> event_names;
    std::map<uint64_t, std::string> categories;
    std::map<uint64_t, std::string> annotation_names;
    std::optional<uint32_t> default_clock_id;
    bool default_clock_incremental = false;
    uint64_t last_timestamp = 0;

    void clear() {
        event_names.clear();
        categories.clear();
        annotation_names.clear();
        default_clock_id.reset();
        default_clock_incremental = false;
        last_timestamp = 0;
    }
};

constexpr uint32_t kCycleClockId = 64;
constexpr uint32_t kBootClockId = 6;

void decodePacket(Decoder pkt, DecodedTrace& trace,
                  std::map<uint32_t, SequenceDecodeState>& sequences);

void decodeTraceBytes(const uint8_t* data, size_t size, DecodedTrace& trace,
                      std::map<uint32_t, SequenceDecodeState>& sequences) {
    Decoder d{data, data + size};
    while (!d.done()) {
        auto [field, wire] = d.tag();
        CHECK(field == 1 && wire == 2 && "top level must be Trace.packet");
        decodePacket(d.lenDelimited(), trace, sequences);
    }
}

void decodePacket(Decoder pkt, DecodedTrace& trace,
                  std::map<uint32_t, SequenceDecodeState>& sequences) {
    // A packet's fields can arrive in any order, and interned data must be
    // applied before the track event that references it — so collect raw
    // sub-ranges first, then process in dependency order.
    uint64_t timestamp = 0;
    std::optional<uint32_t> ts_clock_id;
    uint32_t seq_id = 0;
    uint64_t seq_flags = 0;
    std::optional<Decoder> clock_snapshot;
    std::optional<Decoder> defaults;
    std::optional<Decoder> interned;
    std::optional<Decoder> track_descriptor;
    std::optional<Decoder> track_event;
    std::optional<Decoder> compressed;

    while (!pkt.done()) {
        auto [pf, pw] = pkt.tag();
        switch (pf) {
            case 8:  // timestamp
                timestamp = pkt.varint();
                break;
            case 58:  // timestamp_clock_id
                ts_clock_id = static_cast<uint32_t>(pkt.varint());
                break;
            case 10:  // trusted_packet_sequence_id
                seq_id = static_cast<uint32_t>(pkt.varint());
                break;
            case 13:  // sequence_flags
                seq_flags = pkt.varint();
                break;
            case 6:  // clock_snapshot
                clock_snapshot = pkt.lenDelimited();
                break;
            case 59:  // trace_packet_defaults
                defaults = pkt.lenDelimited();
                break;
            case 12:  // interned_data
                interned = pkt.lenDelimited();
                break;
            case 60:  // track_descriptor
                track_descriptor = pkt.lenDelimited();
                break;
            case 11:  // track_event
                track_event = pkt.lenDelimited();
                break;
            case 50:  // compressed_packets
                compressed = pkt.lenDelimited();
                break;
            default:
                pkt.skip(pw);
        }
    }

    CHECK(seq_id != 0 && "every packet must carry trusted_packet_sequence_id");

    if (compressed) {
        ++trace.compressed_wrappers;
        std::vector<uint8_t> inner =
            inflateBytes(compressed->p, static_cast<size_t>(compressed->end - compressed->p));
        decodeTraceBytes(inner.data(), inner.size(), trace, sequences);
        return;
    }

    SequenceDecodeState& seq = sequences[seq_id];

    if (seq_flags & 1) {  // SEQ_INCREMENTAL_STATE_CLEARED
        trace.saw_sequence_start = true;
        ++trace.state_clears[seq_id];
        seq.clear();
    }

    if (defaults) {
        Decoder d = *defaults;
        while (!d.done()) {
            auto [df, dw] = d.tag();
            if (df == 58) {  // TracePacketDefaults.timestamp_clock_id
                seq.default_clock_id = static_cast<uint32_t>(d.varint());
            } else {
                d.skip(dw);
            }
        }
    }

    if (clock_snapshot) {
        Decoder cs = *clock_snapshot;
        while (!cs.done()) {
            auto [cf, cw] = cs.tag();
            if (cf == 1) {  // clocks
                Decoder c = cs.lenDelimited();
                DecodedClock clk;
                while (!c.done()) {
                    auto [ff, fw] = c.tag();
                    if (ff == 1) {
                        clk.clock_id = static_cast<uint32_t>(c.varint());
                    } else if (ff == 2) {
                        clk.timestamp = c.varint();
                    } else if (ff == 3) {
                        clk.is_incremental = c.varint() != 0;
                    } else {
                        c.skip(fw);
                    }
                }
                if (clk.clock_id == kCycleClockId) {
                    trace.cycle_clock_snapshots.push_back(clk);
                    if (clk.is_incremental) {
                        seq.default_clock_incremental = true;
                        seq.last_timestamp = clk.timestamp;
                    }
                }
            } else {
                cs.skip(cw);
            }
        }
    }

    if (interned) {
        Decoder in = *interned;
        while (!in.done()) {
            auto [inf, inw] = in.tag();
            // 1 = event_categories, 2 = event_names, 3 = debug_annotation_names
            if (inf == 1 || inf == 2 || inf == 3) {
                Decoder entry = in.lenDelimited();
                uint64_t iid = 0;
                std::string name;
                while (!entry.done()) {
                    auto [ef, ew] = entry.tag();
                    if (ef == 1) {
                        iid = entry.varint();
                    } else if (ef == 2) {
                        name = entry.str();
                    } else {
                        entry.skip(ew);
                    }
                }
                CHECK(iid != 0 && "interned entries must have a non-zero iid");
                auto& table = (inf == 1)   ? seq.categories
                              : (inf == 2) ? seq.event_names
                                           : seq.annotation_names;
                CHECK(table.find(iid) == table.end() && "iid re-interned without state clear");
                table[iid] = std::move(name);
                ++trace.interned_entries[seq_id];
            } else {
                in.skip(inw);
            }
        }
    }

    if (track_descriptor) {
        DecodedTrack track;
        Decoder td = *track_descriptor;
        while (!td.done()) {
            auto [tf, tw] = td.tag();
            switch (tf) {
                case 1:
                    track.uuid = td.varint();
                    break;
                case 2:
                    track.name = td.str();
                    break;
                case 3: {  // process descriptor
                    track.is_process = true;
                    Decoder pd = td.lenDelimited();
                    while (!pd.done()) {
                        auto [ff, fw] = pd.tag();
                        if (ff == 6) {
                            track.process_name = pd.str();
                        } else {
                            pd.skip(fw);
                        }
                    }
                    break;
                }
                case 5:
                    track.parent_uuid = td.varint();
                    break;
                case 8:  // counter descriptor
                    track.is_counter = true;
                    td.skip(tw);
                    break;
                default:
                    td.skip(tw);
            }
        }
        trace.tracks.push_back(std::move(track));
    }

    if (track_event) {
        DecodedTrackEvent ev;
        ev.sequence_id = seq_id;
        ev.raw_timestamp = timestamp;
        ev.explicit_clock_id = ts_clock_id;

        // Resolve the timestamp against the sequence's clock state.
        uint32_t effective_clock =
            ts_clock_id.value_or(seq.default_clock_id.value_or(kBootClockId));
        if (effective_clock == kCycleClockId && seq.default_clock_incremental) {
            seq.last_timestamp += timestamp;
            ev.timestamp = seq.last_timestamp;
        } else {
            ev.timestamp = timestamp;
        }

        Decoder te = *track_event;
        while (!te.done()) {
            auto [tf, tw] = te.tag();
            switch (tf) {
                case 9:
                    ev.type = te.varint();
                    break;
                case 11:
                    ev.track_uuid = te.varint();
                    break;
                case 3: {  // category_iids
                    uint64_t iid = te.varint();
                    auto it = seq.categories.find(iid);
                    CHECK(it != seq.categories.end() && "unresolved category iid");
                    ev.category = it->second;
                    break;
                }
                case 22:
                    ev.category = te.str();
                    break;
                case 10: {  // name_iid
                    uint64_t iid = te.varint();
                    auto it = seq.event_names.find(iid);
                    CHECK(it != seq.event_names.end() && "unresolved event name iid");
                    ev.name = it->second;
                    ev.name_was_interned = true;
                    break;
                }
                case 23:
                    ev.name = te.str();
                    break;
                case 30:
                    ev.counter_value = static_cast<int64_t>(te.varint());
                    break;
                case 4: {  // debug annotation
                    Decoder da = te.lenDelimited();
                    std::string name;
                    std::optional<uint64_t> uval;
                    std::optional<std::string> sval;
                    while (!da.done()) {
                        auto [df, dw] = da.tag();
                        if (df == 1) {  // name_iid
                            uint64_t iid = da.varint();
                            auto it = seq.annotation_names.find(iid);
                            CHECK(it != seq.annotation_names.end() &&
                                  "unresolved annotation name iid");
                            name = it->second;
                        } else if (df == 10) {
                            name = da.str();
                        } else if (df == 3) {
                            uval = da.varint();
                        } else if (df == 6) {
                            sval = da.str();
                        } else {
                            da.skip(dw);
                        }
                    }
                    if (uval) ev.uint_annotations[name] = *uval;
                    if (sval) ev.string_annotations[name] = *sval;
                    break;
                }
                default:
                    te.skip(tw);
            }
        }
        trace.events.push_back(std::move(ev));
    }
}

DecodedTrace decodeFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    CHECK(in.is_open());
    std::vector<uint8_t> bytes{std::istreambuf_iterator<char>(in),
                               std::istreambuf_iterator<char>()};

    DecodedTrace trace;
    std::map<uint32_t, SequenceDecodeState> sequences;
    decodeTraceBytes(bytes.data(), bytes.size(), trace, sequences);
    return trace;
}

const DecodedTrack* findTrack(const DecodedTrace& trace, uint64_t uuid) {
    for (const auto& t : trace.tracks) {
        if (t.uuid == uuid) return &t;
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

std::filesystem::path tempFile(const char* name) {
    return std::filesystem::temp_directory_path() / name;
}

PerfettoTraceWriter::Options uncompressed() {
    PerfettoTraceWriter::Options options;
    options.compress = false;
    return options;
}

void test_tracks_and_events() {
    std::cout << "Testing track descriptors and track events... ";

    auto path = tempFile("chronon_pftrace_writer_test.pftrace");
    PerfettoTraceWriter writer;
    const bool opened = writer.open(path, uncompressed());
    CHECK(opened);
    CHECK(writer.isOpen());

    uint64_t process = writer.addProcessTrack("Simulation", 1);
    uint64_t unit_track = writer.addTrack("fetch", process);
    uint64_t counter_track = writer.addCounterTrack("fetch.ops", "ops", process);

    writer.sliceComplete(unit_track, "unit", "tick", /*ts_ns=*/100, /*dur_ns=*/50,
                         /*cycle=*/42, "detail text");
    writer.instant(unit_track, "trace", "fetched 0xdeadbeef", /*cycle=*/123);
    writer.counterValue(counter_track, /*cycle=*/200, /*value=*/7);

    CHECK(writer.eventsWritten() == 3);
    writer.close();
    CHECK(!writer.isOpen());

    DecodedTrace trace = decodeFile(path);
    CHECK(trace.saw_sequence_start);

    // Both sequences start with exactly one incremental-state clear, and the
    // simulation sequence declares the incremental cycle clock.
    CHECK(trace.state_clears.size() == 2);
    for (const auto& [seq, clears] : trace.state_clears) {
        CHECK(clears == 1);
    }
    CHECK(trace.cycle_clock_snapshots.size() == 1);
    CHECK(trace.cycle_clock_snapshots[0].is_incremental);
    CHECK(trace.cycle_clock_snapshots[0].timestamp == 0);

    // Tracks: process group, unit track nested under it, counter track.
    const DecodedTrack* proc = findTrack(trace, process);
    CHECK(proc && proc->is_process && proc->process_name == "Simulation");

    const DecodedTrack* unit = findTrack(trace, unit_track);
    CHECK(unit && unit->name == "fetch" && unit->parent_uuid == process);
    CHECK(!unit->is_counter);

    const DecodedTrack* counter = findTrack(trace, counter_track);
    CHECK(counter && counter->name == "fetch.ops" && counter->is_counter);
    CHECK(counter->parent_uuid == process);

    // Events: slice begin/end pair, instant, counter sample.
    CHECK(trace.events.size() == 4);

    const auto& begin = trace.events[0];
    CHECK(begin.type == 1 && begin.track_uuid == unit_track);
    CHECK(begin.timestamp == 100 && begin.name == "tick" && begin.category == "unit");
    CHECK(begin.name_was_interned);
    CHECK(begin.uint_annotations.at("cycle") == 42);
    CHECK(begin.string_annotations.at("detail") == "detail text");

    const auto& end = trace.events[1];
    CHECK(end.type == 2 && end.track_uuid == unit_track && end.timestamp == 150);

    const auto& instant = trace.events[2];
    CHECK(instant.type == 3 && instant.track_uuid == unit_track);
    CHECK(instant.timestamp == 123 && instant.name == "fetched 0xdeadbeef");
    CHECK(instant.name_was_interned);

    const auto& sample = trace.events[3];
    CHECK(sample.type == 4 && sample.track_uuid == counter_track);
    CHECK(sample.timestamp == 200 && sample.counter_value && *sample.counter_value == 7);

    // Wall and simulation events live on different sequences.
    CHECK(begin.sequence_id != instant.sequence_id);
    CHECK(instant.sequence_id == sample.sequence_id);

    std::filesystem::remove(path);
    std::cout << "PASSED\n";
}

void test_interning_reuse() {
    std::cout << "Testing interned-string reuse... ";

    auto path = tempFile("chronon_pftrace_intern_test.pftrace");
    PerfettoTraceWriter writer;
    CHECK(writer.open(path, uncompressed()));

    uint64_t track = writer.addTrack("lsu");
    writer.instant(track, "trace", "miss", 10);
    writer.instant(track, "trace", "miss", 20);
    writer.instant(track, "trace", "hit", 30);
    writer.instant(track, "trace", "miss", 40);
    writer.close();

    DecodedTrace trace = decodeFile(path);
    CHECK(trace.events.size() == 4);
    for (const auto& ev : trace.events) {
        CHECK(ev.name_was_interned);
    }
    CHECK(trace.events[0].name == "miss" && trace.events[1].name == "miss");
    CHECK(trace.events[2].name == "hit" && trace.events[3].name == "miss");

    // One sequence, 3 interned entries: "trace" category + 2 distinct names.
    CHECK(trace.interned_entries.size() == 1);
    CHECK(trace.interned_entries.begin()->second == 3);

    std::filesystem::remove(path);
    std::cout << "PASSED\n";
}

void test_incremental_timestamps() {
    std::cout << "Testing incremental cycle-clock timestamps... ";

    auto path = tempFile("chronon_pftrace_delta_test.pftrace");
    PerfettoTraceWriter writer;
    CHECK(writer.open(path, uncompressed()));

    uint64_t track = writer.addTrack("unit");
    writer.instant(track, "trace", "a", 100);
    writer.instant(track, "trace", "b", 250);
    writer.instant(track, "trace", "c", 90);  // Out of order: absolute fallback.
    writer.instant(track, "trace", "d", 300);
    writer.close();

    DecodedTrace trace = decodeFile(path);
    CHECK(trace.events.size() == 4);

    // Resolved cycles round-trip regardless of encoding.
    CHECK(trace.events[0].timestamp == 100);
    CHECK(trace.events[1].timestamp == 250);
    CHECK(trace.events[2].timestamp == 90);
    CHECK(trace.events[3].timestamp == 300);

    // Monotonic events ride the incremental clock as deltas...
    CHECK(!trace.events[0].explicit_clock_id && trace.events[0].raw_timestamp == 100);
    CHECK(!trace.events[1].explicit_clock_id && trace.events[1].raw_timestamp == 150);
    // ...the out-of-order one falls back to an absolute boot-clock stamp and
    // leaves the incremental state untouched.
    CHECK(trace.events[2].explicit_clock_id && *trace.events[2].explicit_clock_id == 6);
    CHECK(trace.events[2].raw_timestamp == 90);
    CHECK(!trace.events[3].explicit_clock_id && trace.events[3].raw_timestamp == 50);

    std::filesystem::remove(path);
    std::cout << "PASSED\n";
}

void test_incremental_state_checkpoints() {
    std::cout << "Testing incremental-state checkpoints... ";

    auto path = tempFile("chronon_pftrace_checkpoint_test.pftrace");
    PerfettoTraceWriter writer;
    PerfettoTraceWriter::Options options = uncompressed();
    options.checkpoint_interval_packets = 4;
    CHECK(writer.open(path, options));

    uint64_t track = writer.addTrack("unit");
    constexpr uint64_t kEvents = 16;
    for (uint64_t i = 0; i < kEvents; ++i) {
        writer.instant(track, "trace", "ev", 1000 + i * 10);
    }
    writer.close();

    DecodedTrace trace = decodeFile(path);
    CHECK(trace.events.size() == kEvents);
    for (uint64_t i = 0; i < kEvents; ++i) {
        CHECK(trace.events[i].timestamp == 1000 + i * 10);
        CHECK(trace.events[i].name == "ev");  // Re-interned after each clear.
    }

    // Multiple state clears on the simulation sequence, each with a fresh,
    // monotonically advancing clock snapshot.
    uint32_t sim_seq = trace.events[0].sequence_id;
    CHECK(trace.state_clears.at(sim_seq) > 1);
    CHECK(trace.cycle_clock_snapshots.size() == trace.state_clears.at(sim_seq));
    for (size_t i = 1; i < trace.cycle_clock_snapshots.size(); ++i) {
        CHECK(trace.cycle_clock_snapshots[i].timestamp >=
              trace.cycle_clock_snapshots[i - 1].timestamp);
        CHECK(trace.cycle_clock_snapshots[i].is_incremental);
    }
    // Interning restarted after each clear → more entries than distinct strings.
    CHECK(trace.interned_entries.at(sim_seq) == 2 * trace.state_clears.at(sim_seq));

    std::filesystem::remove(path);
    std::cout << "PASSED\n";
}

void test_intern_cap_forces_checkpoint() {
    std::cout << "Testing intern-table cap... ";

    auto path = tempFile("chronon_pftrace_interncap_test.pftrace");
    PerfettoTraceWriter writer;
    PerfettoTraceWriter::Options options = uncompressed();
    options.max_interned_strings = 4;
    CHECK(writer.open(path, options));

    uint64_t track = writer.addTrack("unit");
    for (uint64_t i = 0; i < 64; ++i) {
        writer.instant(track, "trace", "name " + std::to_string(i), i);
    }
    writer.close();

    DecodedTrace trace = decodeFile(path);
    CHECK(trace.events.size() == 64);
    for (uint64_t i = 0; i < 64; ++i) {
        CHECK(trace.events[i].name == "name " + std::to_string(i));
        CHECK(trace.events[i].timestamp == i);
    }
    uint32_t sim_seq = trace.events[0].sequence_id;
    CHECK(trace.state_clears.at(sim_seq) > 1);

    std::filesystem::remove(path);
    std::cout << "PASSED\n";
}

void test_compressed_packets() {
    std::cout << "Testing compressed_packets batches... ";

    auto raw_path = tempFile("chronon_pftrace_raw_test.pftrace");
    auto compressed_path = tempFile("chronon_pftrace_compressed_test.pftrace");

    auto write = [](PerfettoTraceWriter& writer) {
        uint64_t process = writer.addProcessTrack("Simulation", 1);
        uint64_t track = writer.addTrack("fetch", process);
        uint64_t counter = writer.addCounterTrack("fetch.ops", "ops", process);
        for (uint64_t i = 0; i < 2000; ++i) {
            writer.instant(track, "trace", "fetched pc", 100 + i);
            writer.counterValue(counter, 100 + i, static_cast<int64_t>(i % 7));
        }
        writer.sliceComplete(track, "unit", "tick", 5, 10, 1, "x");
    };

    PerfettoTraceWriter raw_writer;
    CHECK(raw_writer.open(raw_path, uncompressed()));
    write(raw_writer);
    raw_writer.close();

    PerfettoTraceWriter compressed_writer;
    PerfettoTraceWriter::Options options;  // compress = true by default
    CHECK(compressed_writer.open(compressed_path, options));
    write(compressed_writer);
    compressed_writer.close();

    DecodedTrace raw = decodeFile(raw_path);
    DecodedTrace compressed = decodeFile(compressed_path);

    CHECK(raw.compressed_wrappers == 0);
    CHECK(compressed.compressed_wrappers > 0);

    // Identical decoded content either way.
    CHECK(raw.events.size() == compressed.events.size());
    CHECK(raw.tracks.size() == compressed.tracks.size());
    for (size_t i = 0; i < raw.events.size(); ++i) {
        CHECK(raw.events[i].timestamp == compressed.events[i].timestamp);
        CHECK(raw.events[i].name == compressed.events[i].name);
        CHECK(raw.events[i].type == compressed.events[i].type);
        CHECK(raw.events[i].counter_value == compressed.events[i].counter_value);
    }

    // And a real size win on repetitive content.
    auto raw_size = std::filesystem::file_size(raw_path);
    auto compressed_size = std::filesystem::file_size(compressed_path);
    CHECK(compressed_size < raw_size / 2);

    std::filesystem::remove(raw_path);
    std::filesystem::remove(compressed_path);
    std::cout << "PASSED\n";
}

void test_timeline_stream_export() {
    std::cout << "Testing timeline stream export... ";

    TimelineStreamData data;
    data.process_name = "Chronon Scheduler";
    data.pid = 2;
    data.stream_names = {"stream 0", "scheduler"};
    data.streams.resize(2);
    data.arenas.resize(2);

    auto record = [&](size_t stream, std::string_view cat, std::string_view name, uint64_t cycle,
                      uint64_t ts, uint64_t dur, std::string_view detail) {
        std::string& arena = data.arenas[stream];
        auto intern = [&arena](std::string_view s) -> std::pair<uint32_t, uint32_t> {
            uint32_t off = static_cast<uint32_t>(arena.size());
            arena.append(s.data(), s.size());
            return {off, static_cast<uint32_t>(s.size())};
        };
        auto [co, cl] = intern(cat);
        auto [no, nl] = intern(name);
        auto [dto, dtl] = intern(detail);
        data.streams[stream].push_back({co, cl, no, nl, dto, dtl, cycle, ts, dur});
    };

    record(0, "unit", "fetch", 10, 1000, 300, "");
    record(0, "wait", "cross-cluster stall", 11, 1300, 700, "iq0");
    record(1, "scheduler", "epoch", 10, 900, 1500, "");
    data.dropped_events = 5;

    // Default options → exercises the compressed path end-to-end.
    auto path = tempFile("chronon_pftrace_timeline_test.pftrace");
    PerfettoTraceWriter writer;
    PerfettoTraceWriter::Options options;
    const bool opened = writer.open(path, options);
    CHECK(opened);
    writeTimeline(writer, data);
    writer.close();

    DecodedTrace trace = decodeFile(path);

    // One process track + one track per stream.
    CHECK(trace.tracks.size() == 3);
    CHECK(trace.tracks[0].is_process && trace.tracks[0].process_name == "Chronon Scheduler");
    CHECK(trace.tracks[1].name == "stream 0");
    CHECK(trace.tracks[2].name == "scheduler");
    CHECK(trace.tracks[1].parent_uuid == trace.tracks[0].uuid);

    // 3 slices = 6 begin/end events, plus the dropped-events instant.
    CHECK(trace.events.size() == 7);

    const auto& stall_begin = trace.events[2];
    CHECK(stall_begin.type == 1 && stall_begin.name == "cross-cluster stall");
    CHECK(stall_begin.category == "wait" && stall_begin.timestamp == 1300);
    CHECK(stall_begin.uint_annotations.at("cycle") == 11);
    CHECK(stall_begin.string_annotations.at("detail") == "iq0");
    const auto& stall_end = trace.events[3];
    CHECK(stall_end.type == 2 && stall_end.timestamp == 2000);

    const auto& dropped = trace.events.back();
    CHECK(dropped.type == 3 && dropped.name == "dropped events: 5");

    std::filesystem::remove(path);
    std::cout << "PASSED\n";
}

void test_empty_and_reopen() {
    std::cout << "Testing empty trace and reopen behavior... ";

    auto path = tempFile("chronon_pftrace_empty_test.pftrace");
    PerfettoTraceWriter writer;
    const bool opened = writer.open(path);
    CHECK(opened);
    const bool reopened = writer.open(path);
    CHECK(!reopened);  // Double open rejected.
    writer.close();

    // A trace with no packets is a valid (empty) file.
    CHECK(std::filesystem::exists(path));
    DecodedTrace trace = decodeFile(path);
    CHECK(trace.tracks.empty() && trace.events.empty());

    // Events on a closed writer are dropped, not crashes.
    writer.instant(1, "x", "y", 0);
    CHECK(writer.eventsWritten() == 0);

    std::filesystem::remove(path);
    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== PerfettoTraceWriter Tests ===\n";

    test_tracks_and_events();
    test_interning_reuse();
    test_incremental_timestamps();
    test_incremental_state_checkpoints();
    test_intern_cap_forces_checkpoint();
    test_compressed_packets();
    test_timeline_stream_export();
    test_empty_and_reopen();

    std::cout << "\nAll tests PASSED\n";
    return 0;
}
