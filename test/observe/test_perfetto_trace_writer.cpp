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
///        track-event payloads, and the timeline stream export.

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

// Decoded views of the packet subset the writer emits.
struct DecodedTrackEvent {
    uint64_t timestamp = 0;
    uint64_t type = 0;
    uint64_t track_uuid = 0;
    std::string name;
    std::string category;
    std::optional<int64_t> counter_value;
    std::map<std::string, uint64_t> uint_annotations;
    std::map<std::string, std::string> string_annotations;
};

struct DecodedTrack {
    uint64_t uuid = 0;
    uint64_t parent_uuid = 0;
    std::string name;
    bool is_process = false;
    bool is_counter = false;
    std::string process_name;
};

struct DecodedTrace {
    std::vector<DecodedTrack> tracks;
    std::vector<DecodedTrackEvent> events;
    bool saw_sequence_start = false;
};

DecodedTrace decodeFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    CHECK(in.is_open());
    std::vector<uint8_t> bytes{std::istreambuf_iterator<char>(in),
                               std::istreambuf_iterator<char>()};

    DecodedTrace trace;
    Decoder d{bytes.data(), bytes.data() + bytes.size()};
    while (!d.done()) {
        auto [field, wire] = d.tag();
        CHECK(field == 1 && wire == 2 && "top level must be Trace.packet");
        Decoder pkt = d.lenDelimited();

        uint64_t timestamp = 0;
        bool has_seq_id = false;
        while (!pkt.done()) {
            auto [pf, pw] = pkt.tag();
            switch (pf) {
                case 8:  // timestamp
                    timestamp = pkt.varint();
                    break;
                case 10:  // trusted_packet_sequence_id
                    has_seq_id = true;
                    CHECK(pkt.varint() != 0);
                    break;
                case 13:  // sequence_flags
                    if (pkt.varint() & 1) trace.saw_sequence_start = true;
                    break;
                case 60: {  // track_descriptor
                    DecodedTrack track;
                    Decoder td = pkt.lenDelimited();
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
                    break;
                }
                case 11: {  // track_event
                    DecodedTrackEvent ev;
                    ev.timestamp = timestamp;
                    Decoder te = pkt.lenDelimited();
                    while (!te.done()) {
                        auto [tf, tw] = te.tag();
                        switch (tf) {
                            case 9:
                                ev.type = te.varint();
                                break;
                            case 11:
                                ev.track_uuid = te.varint();
                                break;
                            case 22:
                                ev.category = te.str();
                                break;
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
                                    if (df == 10) {
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
                    break;
                }
                default:
                    pkt.skip(pw);
            }
        }
        CHECK(has_seq_id && "every packet must carry trusted_packet_sequence_id");
    }
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

void test_tracks_and_events() {
    std::cout << "Testing track descriptors and track events... ";

    auto path = tempFile("chronon_pftrace_writer_test.pftrace");
    PerfettoTraceWriter writer;
    const bool opened = writer.open(path);
    CHECK(opened);
    CHECK(writer.isOpen());

    uint64_t process = writer.addProcessTrack("Simulation", 1);
    uint64_t unit_track = writer.addTrack("fetch", process);
    uint64_t counter_track = writer.addCounterTrack("fetch.ops", "ops", process);

    writer.sliceComplete(unit_track, "unit", "tick", /*ts_ns=*/100, /*dur_ns=*/50,
                         /*cycle=*/42, "detail text");
    writer.instant(unit_track, "trace", "fetched 0xdeadbeef", /*ts_ns=*/123);
    writer.counterValue(counter_track, /*ts_ns=*/200, /*value=*/7);

    CHECK(writer.eventsWritten() == 3);
    writer.close();
    CHECK(!writer.isOpen());

    DecodedTrace trace = decodeFile(path);
    CHECK(trace.saw_sequence_start);

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
    CHECK(begin.uint_annotations.at("cycle") == 42);
    CHECK(begin.string_annotations.at("detail") == "detail text");

    const auto& end = trace.events[1];
    CHECK(end.type == 2 && end.track_uuid == unit_track && end.timestamp == 150);

    const auto& instant = trace.events[2];
    CHECK(instant.type == 3 && instant.track_uuid == unit_track);
    CHECK(instant.timestamp == 123 && instant.name == "fetched 0xdeadbeef");

    const auto& sample = trace.events[3];
    CHECK(sample.type == 4 && sample.track_uuid == counter_track);
    CHECK(sample.timestamp == 200 && sample.counter_value && *sample.counter_value == 7);

    std::filesystem::remove(path);
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

    auto path = tempFile("chronon_pftrace_timeline_test.pftrace");
    PerfettoTraceWriter writer;
    const bool opened = writer.open(path);
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
    test_timeline_stream_export();
    test_empty_and_reopen();

    std::cout << "\nAll tests PASSED\n";
    return 0;
}
