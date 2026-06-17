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

#include <bit>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "PftraceTestDecoder.hpp"
#include "observe/PerfettoTraceWriter.hpp"
#include "observe/TimelineData.hpp"

using namespace chronon::observe;
using namespace pftrace_test;

namespace {

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
    CHECK(!instant.name_was_interned);

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
        CHECK(!ev.name_was_interned);
    }
    CHECK(trace.events[0].name == "miss" && trace.events[1].name == "miss");
    CHECK(trace.events[2].name == "hit" && trace.events[3].name == "miss");

    // One sequence, one interned entry: the "trace" category. Simulation
    // instants emit direct TrackEvent.name fields and do not fill event_names.
    CHECK(trace.interned_entries.size() == 1);
    CHECK(trace.interned_entries.begin()->second == 1);

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
    // Interning restarted after each clear; direct instant names stay out of
    // event_names, so only the category is re-interned per epoch.
    CHECK(trace.interned_entries.at(sim_seq) == trace.state_clears.at(sim_seq));

    std::filesystem::remove(path);
    std::cout << "PASSED\n";
}

void test_direct_names_do_not_fill_intern_cap() {
    std::cout << "Testing direct instant names skip intern-table cap... ";

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
    CHECK(trace.state_clears.at(sim_seq) == 1);
    CHECK(trace.interned_entries.at(sim_seq) == 1);

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

void test_compressed_batch_size_cap() {
    std::cout << "Testing compressed batch splitting at the input cap... ";

    auto path = tempFile("chronon_pftrace_batchcap_test.pftrace");
    PerfettoTraceWriter writer;
    CHECK(writer.open(path));  // Default options: compression on.

    // ~3 MB of unique ~1 KB event names in a single flush (close() flushes
    // once: 3000 packets stay below the 4096-packet auto-flush threshold).
    // Embedding the index makes every direct TrackEvent.name distinct, keeping
    // each packet large even though the names are not interned.
    uint64_t track = writer.addTrack("noise");
    const std::string filler(1024, 'x');
    for (uint64_t i = 0; i < 3000; ++i) {
        std::string name = filler;
        const std::string idx = std::to_string(i);
        name.replace(0, idx.size(), idx);
        writer.instant(track, "trace", name, i);
    }
    writer.close();

    DecodedTrace trace = decodeFile(path);
    CHECK(trace.events.size() == 3000);

    // The batch must split into multiple wrappers, each fed at most ~256 KB
    // of packets (one oversized packet of slack), with the compressed payload
    // necessarily smaller still.
    CHECK(trace.compressed_wrappers > 1);
    constexpr size_t kInputCap = 256 * 1024;
    for (const auto& [payload, inflated] : trace.compressed_wrapper_sizes) {
        CHECK(inflated <= kInputCap + 2048);
        CHECK(payload <= inflated + 1024);
    }

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

void test_slices_flows_typed_annotations() {
    std::cout << "Testing sim-domain slices, flows, and typed annotations... ";

    auto path = tempFile("chronon_pftrace_slices_test.pftrace");
    PerfettoTraceWriter writer;
    CHECK(writer.open(path, uncompressed()));

    uint64_t track = writer.addTrack("mshr[0]");

    const PerfettoTraceWriter::Annotation begin_anns[] = {
        {"addr", PerfettoTraceWriter::Annotation::Kind::Uint, 0xdead0000ULL},
        {"way", PerfettoTraceWriter::Annotation::Kind::Int, std::bit_cast<uint64_t>(int64_t{-3})},
        {"ipc", PerfettoTraceWriter::Annotation::Kind::Double, std::bit_cast<uint64_t>(1.25)},
        {"hit", PerfettoTraceWriter::Annotation::Kind::Bool, 0},
    };
    writer.sliceBegin(track, "dcache_miss", "miss", /*cycle=*/100, /*flow_id=*/777, begin_anns);
    writer.sliceEnd(track, /*cycle=*/140);
    writer.instant(track, "dcache_miss", "replay", /*cycle=*/150, /*flow_id=*/777, {});
    writer.close();

    DecodedTrace trace = decodeFile(path);
    CHECK(trace.events.size() == 3);

    const auto& begin = trace.events[0];
    CHECK(begin.type == 1 && begin.track_uuid == track && begin.timestamp == 100);
    CHECK(begin.name == "miss" && begin.name_was_interned);
    CHECK(begin.category == "dcache_miss");
    CHECK(begin.flow_ids.size() == 1 && begin.flow_ids[0] == 777);
    CHECK(begin.uint_annotations.at("addr") == 0xdead0000ULL);
    CHECK(begin.int_annotations.at("way") == -3);
    CHECK(begin.double_annotations.at("ipc") == 1.25);
    CHECK(begin.bool_annotations.at("hit") == false);

    const auto& end = trace.events[1];
    CHECK(end.type == 2 && end.track_uuid == track && end.timestamp == 140);

    const auto& replay = trace.events[2];
    CHECK(replay.type == 3 && replay.timestamp == 150);
    CHECK(replay.name == "replay" && replay.name_was_interned);
    CHECK(replay.flow_ids.size() == 1 && replay.flow_ids[0] == 777);

    // Same sequence (cycle clock) for all three; deltas on the wire.
    CHECK(begin.sequence_id == end.sequence_id && end.sequence_id == replay.sequence_id);
    CHECK(end.raw_timestamp == 40 && replay.raw_timestamp == 10);

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
    test_direct_names_do_not_fill_intern_cap();
    test_compressed_packets();
    test_compressed_batch_size_cap();
    test_slices_flows_typed_annotations();
    test_timeline_stream_export();
    test_empty_and_reopen();

    std::cout << "\nAll tests PASSED\n";
    return 0;
}
