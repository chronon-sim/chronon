// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file test_timeline_api.cpp
/// @brief End-to-end tests for the timeline lane / counter API: declarative
///        unit members → ObservationContext records → backend open-span table
///        → timeline.pftrace, verified with the independent wire decoder.
///        Covers span addressing, slot reuse, orphan-end suppression,
///        shutdown close, temporal-filter semantics, lookahead
///        commit/rollback, and an informational producer-cost measurement.

#include <bit>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "PftraceTestDecoder.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/ObservationBackend.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationQueue.hpp"
#include "observe/ObserveApi.hpp"
#include "observe/ThreadContextManager.hpp"
#include "observe/TimelineObserve.hpp"
#include "observe/TimelineTrack.hpp"

using namespace chronon::observe;
using namespace pftrace_test;

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

namespace {

inline const auto LANE_CAT = Category<"lane_cat", "Timeline API test category">{};

struct TestUnit : public ObservableUnit {
    TimelineLane mshr{this, "mshr", 4};
    TimelineLane port{this, "port"};
    TimelineCounter occ{this, "occ", "entries"};
    TimelineSpan stall{this, "stall"};
    TimelineGauge member_gauge{this, "member_gauge", "entries"};
    TimelineCapacity member_capacity{this, "member_capacity", "entries"};

    uint64_t cycle = 0;
    uint64_t getObserveCycle() const noexcept override { return cycle; }
};

/// Events on @p track_uuid, in file order.
std::vector<DecodedTrackEvent> eventsOn(const DecodedTrace& trace, uint64_t track_uuid) {
    std::vector<DecodedTrackEvent> out;
    for (const auto& ev : trace.events) {
        if (ev.track_uuid == track_uuid) {
            out.push_back(ev);
        }
    }
    return out;
}

const DecodedTrack* childTrack(const DecodedTrace& trace, std::string_view name,
                               uint64_t parent_uuid) {
    for (const auto& t : trace.tracks) {
        if (t.name == name && t.parent_uuid == parent_uuid) {
            return &t;
        }
    }
    return nullptr;
}

void test_lanes_end_to_end() {
    std::cout << "Testing lanes, spans, flows, and counters end-to-end... ";

    const std::string out_dir = "/tmp/chronon_timeline_api_test";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(256 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_enabled = true;
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);
    backend.setSourceNameLookup(
        [](uint16_t id) -> std::string_view { return id == 1 ? "lsu0" : ""; });
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "lsu0", 1);
        ctx.enableCategory(category::TRACE | LANE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);
        CHECK(unit.mshr.isRegistered() && unit.port.isRegistered() && unit.occ.isRegistered());

        // Span with flow + typed args, ended 30 cycles later.
        unit.cycle = 100;
        unit.mshr.begin(2, LANE_CAT, "miss"_ev, flow(42), arg<"addr">(uint64_t{0xbeef}),
                        arg<"dirty">(true));
        unit.occ.sample(5);
        unit.cycle = 130;
        unit.mshr.end(2);
        unit.occ.sample(7);

        // Lane instant on a single-lane track, linked by the same flow.
        unit.cycle = 131;
        unit.port.instant(0, LANE_CAT, "replay"_ev, flow(42));

        // Hardware slot reuse: second begin implicitly closes the first.
        unit.cycle = 140;
        unit.mshr.begin(1, LANE_CAT, "fill"_ev);
        unit.cycle = 150;
        unit.mshr.begin(1, LANE_CAT, "fill"_ev);
        unit.cycle = 160;
        unit.mshr.end(1);

        // Orphan end: no begin on slot 3 → dropped, and no track created.
        unit.cycle = 170;
        unit.mshr.end(3);

        // Dangling begin: closed automatically at shutdown, at the last
        // timeline cycle seen (200, from the final counter sample).
        unit.cycle = 180;
        unit.mshr.begin(0, LANE_CAT, "stuck"_ev);
        unit.cycle = 200;
        unit.occ.sample(9);

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);

    // Track shape: unit track → lane group → per-slot children; counter and
    // single-lane tracks directly under the unit.
    const DecodedTrack* unit_track = findTrackByName(trace, "lsu0");
    CHECK(unit_track != nullptr);
    const DecodedTrack* mshr_group = childTrack(trace, "mshr", unit_track->uuid);
    CHECK(mshr_group != nullptr);
    const DecodedTrack* mshr2 = childTrack(trace, "mshr[2]", mshr_group->uuid);
    const DecodedTrack* mshr1 = childTrack(trace, "mshr[1]", mshr_group->uuid);
    const DecodedTrack* mshr0 = childTrack(trace, "mshr[0]", mshr_group->uuid);
    CHECK(mshr2 && mshr1 && mshr0);
    CHECK(childTrack(trace, "mshr[3]", mshr_group->uuid) == nullptr);  // Orphan end.
    const DecodedTrack* port_track = childTrack(trace, "port", unit_track->uuid);
    CHECK(port_track != nullptr);
    const DecodedTrack* occ_track = childTrack(trace, "occ", unit_track->uuid);
    CHECK(occ_track != nullptr && occ_track->is_counter);

    // mshr[2]: begin@100 (flow, typed args) + end@130.
    auto mshr2_events = eventsOn(trace, mshr2->uuid);
    CHECK(mshr2_events.size() == 2);
    CHECK(mshr2_events[0].type == 1 && mshr2_events[0].timestamp == 100);
    CHECK(mshr2_events[0].name == "miss" && mshr2_events[0].category == "lane_cat");
    CHECK(mshr2_events[0].flow_ids == std::vector<uint64_t>{42});
    CHECK(mshr2_events[0].uint_annotations.at("addr") == 0xbeef);
    CHECK(mshr2_events[0].bool_annotations.at("dirty") == true);
    CHECK(mshr2_events[1].type == 2 && mshr2_events[1].timestamp == 130);

    // mshr[1]: slot reuse → begin@140, implicit end@150, begin@150, end@160.
    auto mshr1_events = eventsOn(trace, mshr1->uuid);
    CHECK(mshr1_events.size() == 4);
    CHECK(mshr1_events[0].type == 1 && mshr1_events[0].timestamp == 140);
    CHECK(mshr1_events[1].type == 2 && mshr1_events[1].timestamp == 150);
    CHECK(mshr1_events[2].type == 1 && mshr1_events[2].timestamp == 150);
    CHECK(mshr1_events[3].type == 2 && mshr1_events[3].timestamp == 160);

    // mshr[0]: dangling begin closed at shutdown at the last seen cycle.
    auto mshr0_events = eventsOn(trace, mshr0->uuid);
    CHECK(mshr0_events.size() == 2);
    CHECK(mshr0_events[0].type == 1 && mshr0_events[0].timestamp == 180);
    CHECK(mshr0_events[0].name == "stuck");
    CHECK(mshr0_events[1].type == 2 && mshr0_events[1].timestamp == 200);

    // port: instant with the instruction flow.
    auto port_events = eventsOn(trace, port_track->uuid);
    CHECK(port_events.size() == 1);
    CHECK(port_events[0].type == 3 && port_events[0].timestamp == 131);
    CHECK(port_events[0].name == "replay");
    CHECK(port_events[0].flow_ids == std::vector<uint64_t>{42});

    // occ: push-model counter samples.
    auto occ_events = eventsOn(trace, occ_track->uuid);
    CHECK(occ_events.size() == 3);
    CHECK(occ_events[0].timestamp == 100 && *occ_events[0].counter_value == 5);
    CHECK(occ_events[1].timestamp == 130 && *occ_events[1].counter_value == 7);
    CHECK(occ_events[2].timestamp == 200 && *occ_events[2].counter_value == 9);

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_path_track_hierarchy() {
    std::cout << "Testing hierarchical path → track tree... ";

    const std::string out_dir = "/tmp/chronon_timeline_api_hier";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(256 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);
    backend.setSourceNameLookup(
        [](uint16_t id) -> std::string_view { return id == 1 ? "cpu0.lsu" : ""; });
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "cpu0.lsu", 1);
        ctx.enableCategory(category::TRACE | LANE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);

        unit.cycle = 10;
        unit.mshr.begin(2, LANE_CAT, "miss"_ev);
        unit.cycle = 20;
        unit.mshr.end(2);
        unit.occ.sample(3);

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);

    // The dotted unit path becomes a parent_uuid chain mirroring the design
    // hierarchy: process → cpu0 → lsu → mshr → mshr[2].
    const DecodedTrack* process = nullptr;
    for (const auto& t : trace.tracks) {
        if (t.is_process) process = &t;
    }
    CHECK(process != nullptr);
    const DecodedTrack* cpu0 = childTrack(trace, "cpu0", process->uuid);
    CHECK(cpu0 != nullptr);
    const DecodedTrack* lsu = childTrack(trace, "lsu", cpu0->uuid);
    CHECK(lsu != nullptr);
    const DecodedTrack* mshr = childTrack(trace, "mshr", lsu->uuid);
    CHECK(mshr != nullptr);
    const DecodedTrack* mshr2 = childTrack(trace, "mshr[2]", mshr->uuid);
    CHECK(mshr2 != nullptr);
    const DecodedTrack* occ = childTrack(trace, "occ", lsu->uuid);
    CHECK(occ != nullptr && occ->is_counter);

    auto span_events = eventsOn(trace, mshr2->uuid);
    CHECK(span_events.size() == 2);
    CHECK(span_events[0].type == 1 && span_events[0].timestamp == 10);
    CHECK(span_events[1].type == 2 && span_events[1].timestamp == 20);

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_high_bit_category() {
    std::cout << "Testing category name resolution for bits >= 32... ";

    // Push the auto-assigned bit allocator past bit 32 (#43 review: the
    // record used to truncate the mask to 32 bits, losing these).
    for (int i = 0; i < 40; ++i) {
        CategoryRegistry::instance().registerCategory("hb_filler", "");
    }
    static const auto HIGH_CAT = Category<"high_bit_cat", "Category beyond bit 32">{};
    CHECK(std::countr_zero(HIGH_CAT.mask()) >= 32);

    const std::string out_dir = "/tmp/chronon_timeline_api_highbit";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(256 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "u", 1);
        ctx.enableCategory(category::TRACE | HIGH_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);
        unit.cycle = 5;
        unit.port.instant(0, HIGH_CAT, "high"_ev);

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);
    CHECK(trace.events.size() == 1);
    CHECK(trace.events[0].name == "high");
    CHECK(trace.events[0].category == "high_bit_cat");

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_trace_pipe_category_is_plain_trace() {
    std::cout << "Testing pipe-category trace stays a plain trace instant... ";

    static const auto PIPE_CAT =
        Category<"pipe", "Pipeline category for deprecated trace events">{};
    CHECK(std::countr_zero(PIPE_CAT.mask()) >= 32);

    const std::string out_dir = "/tmp/chronon_timeline_api_highbit_pipe";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(256 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);
    backend.setSourceNameLookup(
        [](uint16_t id) -> std::string_view { return id == 1 ? "cpu0.fetch" : ""; });
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "cpu0.fetch", 1);
        ctx.enableCategory(category::TRACE | PIPE_CAT.mask() | LANE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);
        unit.cycle = 3;
        unit.mshr.begin(0, PIPE_CAT, "dangling"_ev);
        unit.cycle = 7;
        unit.trace<"12DEC#0;pc=0x100">(PIPE_CAT);
        unit.cycle = 9;
        unit.trace<"ordinary trace">(LANE_CAT);

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);
    const DecodedTrack* process = nullptr;
    for (const auto& t : trace.tracks) {
        if (t.is_process) process = &t;
    }
    CHECK(process != nullptr);
    const DecodedTrack* cpu0 = childTrack(trace, "cpu0", process->uuid);
    CHECK(cpu0 != nullptr);
    const DecodedTrack* fetch = childTrack(trace, "fetch", cpu0->uuid);
    CHECK(fetch != nullptr);
    auto trace_events = eventsOn(trace, fetch->uuid);
    CHECK(trace_events.size() == 2);
    CHECK(trace_events[0].type == 3 && trace_events[0].timestamp == 7);
    CHECK(trace_events[0].name == "12DEC#0;pc=0x100");
    CHECK(trace_events[1].type == 3 && trace_events[1].timestamp == 9);
    CHECK(trace_events[1].name == "ordinary trace");

    const DecodedTrack* mshr = childTrack(trace, "mshr", fetch->uuid);
    CHECK(mshr != nullptr);
    const DecodedTrack* mshr0 = childTrack(trace, "mshr[0]", mshr->uuid);
    CHECK(mshr0 != nullptr);
    auto mshr_events = eventsOn(trace, mshr0->uuid);
    CHECK(mshr_events.size() == 2);
    CHECK(mshr_events[0].type == 1 && mshr_events[0].timestamp == 3);
    CHECK(mshr_events[1].type == 2 && mshr_events[1].timestamp == 9);

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_temporal_filter_span_semantics() {
    std::cout << "Testing temporal-filter span semantics... ";

    const std::string out_dir = "/tmp/chronon_timeline_api_temporal";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(256 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);
    backend.setSourceNameLookup([](uint16_t id) -> std::string_view { return id == 1 ? "u" : ""; });
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "u", 1);
        ctx.enableCategory(LANE_CAT.mask());
        ctx.filter().addCycleRange(0, 100);  // Observe cycles [0, 100] only.

        TestUnit unit;
        unit.setObservationContext(&ctx);

        // Begin inside the window, end outside: the end must still close the
        // span (ends are not temporally filtered).
        unit.cycle = 50;
        unit.mshr.begin(0, LANE_CAT, "inside"_ev);
        unit.cycle = 60;
        spanBegin<"category_span">(unit, LANE_CAT, "category"_ev);
        unit.cycle = 70;
        spanEnd<"category_span">(unit);
        unit.cycle = 150;
        unit.mshr.end(0);

        // Begin outside the window: suppressed at the producer; its end is
        // dropped by the backend's open-span table.
        unit.cycle = 120;
        unit.mshr.begin(1, LANE_CAT, "outside"_ev);
        unit.cycle = 130;
        unit.mshr.end(1);

        ctx.filter().clearCycleRanges();
        ctx.filter().addCycleRange(200, 300);

        // A suppressed sample must not poison the sample-on-change cache:
        // the first equal value inside the category window still has to emit.
        unit.cycle = 190;
        unit.stall.update(true, LANE_CAT, "window_stall"_ev, arg<"reason">(uint64_t{1}));
        unit.member_gauge.sampleOnChange(LANE_CAT, 5);
        unit.member_capacity.sampleOnChange(LANE_CAT, 2, 8);
        unit.cycle = 210;
        unit.stall.update(true, LANE_CAT, "window_stall"_ev, arg<"reason">(uint64_t{1}));
        unit.member_gauge.sampleOnChange(LANE_CAT, 5);
        unit.member_capacity.sampleOnChange(LANE_CAT, 2, 8);
        unit.cycle = 220;
        unit.stall.update(false, LANE_CAT, "window_stall"_ev);

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);

    const DecodedTrack* mshr_group = findTrackByName(trace, "mshr");
    CHECK(mshr_group != nullptr);
    const DecodedTrack* slot0 = childTrack(trace, "mshr[0]", mshr_group->uuid);
    CHECK(slot0 != nullptr);
    CHECK(childTrack(trace, "mshr[1]", mshr_group->uuid) == nullptr);

    auto slot0_events = eventsOn(trace, slot0->uuid);
    CHECK(slot0_events.size() == 2);
    CHECK(slot0_events[0].type == 1 && slot0_events[0].timestamp == 50);
    CHECK(slot0_events[0].name == "inside");
    CHECK(slot0_events[1].type == 2 && slot0_events[1].timestamp == 150);

    const DecodedTrack* unit_track = findTrackByName(trace, "u");
    CHECK(unit_track != nullptr);
    const DecodedTrack* category_span = childTrack(trace, "category_span", unit_track->uuid);
    CHECK(category_span != nullptr);
    auto category_span_events = eventsOn(trace, category_span->uuid);
    CHECK(category_span_events.size() == 2);
    CHECK(category_span_events[1].type == 2 && category_span_events[1].timestamp == 70);
    const DecodedTrack* stall = childTrack(trace, "stall", unit_track->uuid);
    CHECK(stall != nullptr);
    auto stall_events = eventsOn(trace, stall->uuid);
    CHECK(stall_events.size() == 2);
    CHECK(stall_events[0].type == 1 && stall_events[0].timestamp == 210);
    CHECK(stall_events[0].name == "window_stall");
    CHECK(stall_events[0].uint_annotations.at("reason") == 1);
    CHECK(stall_events[1].type == 2 && stall_events[1].timestamp == 220);

    const DecodedTrack* member_gauge = childTrack(trace, "member_gauge", unit_track->uuid);
    CHECK(member_gauge != nullptr && member_gauge->is_counter);
    auto gauge_events = eventsOn(trace, member_gauge->uuid);
    CHECK(gauge_events.size() == 1);
    CHECK(gauge_events[0].timestamp == 210 && gauge_events[0].counter_value.value() == 5);

    const DecodedTrack* capacity_used = childTrack(trace, "member_capacity.used", unit_track->uuid);
    const DecodedTrack* capacity_free = childTrack(trace, "member_capacity.free", unit_track->uuid);
    CHECK(capacity_used != nullptr && capacity_used->is_counter);
    CHECK(capacity_free != nullptr && capacity_free->is_counter);
    auto capacity_used_events = eventsOn(trace, capacity_used->uuid);
    auto capacity_free_events = eventsOn(trace, capacity_free->uuid);
    CHECK(capacity_used_events.size() == 1);
    CHECK(capacity_free_events.size() == 1);
    CHECK(capacity_used_events[0].timestamp == 210 &&
          capacity_used_events[0].counter_value.value() == 2);
    CHECK(capacity_free_events[0].timestamp == 210 &&
          capacity_free_events[0].counter_value.value() == 6);

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_timeline_observe_convenience_api() {
    std::cout << "Testing timeline convenience API... ";

    const std::string out_dir = "/tmp/chronon_timeline_observe_api";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(256 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);
    backend.setSourceNameLookup(
        [](uint16_t id) -> std::string_view { return id == 1 ? "decode0" : ""; });
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "decode0", 1);
        ctx.enableCategory(category::TRACE | LANE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);

        unit.cycle = 10;
        event<"flush">(unit, LANE_CAT, arg<"removed">(uint64_t{3}));
        unit.cycle = 11;
        unit.event<"member_event">(LANE_CAT, arg<"value">(uint64_t{7}));

        unit.cycle = 12;
        instant<"marks">(unit, LANE_CAT, "ready"_ev, flow(42));

        unit.cycle = 20;
        spanBegin<"function_stall">(unit, LANE_CAT, "blocked"_ev, arg<"fq_size">(uint64_t{5}));
        unit.cycle = 25;
        spanEnd<"function_stall">(unit);

        unit.cycle = 30;
        unit.stall.update(true, LANE_CAT, "out_blocked"_ev, arg<"out_rem">(uint64_t{0}));
        unit.cycle = 35;
        unit.stall.update(false, LANE_CAT, "out_blocked"_ev);

        unit.cycle = 40;
        gauge<"function_occ">(unit, 6, "entries");
        unit.gauge<"member_occ">(7, "entries");
        unit.member_gauge.sampleOnChange(2);
        unit.member_gauge.sampleOnChange(2);
        unit.cycle = 41;
        unit.member_gauge.sampleOnChange(3);

        unit.cycle = 50;
        capacity<"function_queue">(unit, 6, 10, "entries");
        unit.capacity<"member_queue">(7, 10, "entries");
        unit.member_capacity.sampleOnChange(4, 8);
        unit.member_capacity.sampleOnChange(4, 8);
        unit.cycle = 51;
        unit.member_capacity.sampleOnChange(5, 8);

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);

    const DecodedTrack* unit_track = findTrackByName(trace, "decode0");
    CHECK(unit_track != nullptr);

    const DecodedTrack* events = childTrack(trace, "events", unit_track->uuid);
    CHECK(events != nullptr);
    auto event_records = eventsOn(trace, events->uuid);
    CHECK(event_records.size() == 2);
    CHECK(event_records[0].type == 3 && event_records[0].timestamp == 10);
    CHECK(event_records[0].name == "flush");
    CHECK(event_records[0].uint_annotations.at("removed") == 3);
    CHECK(event_records[1].type == 3 && event_records[1].timestamp == 11);
    CHECK(event_records[1].name == "member_event");
    CHECK(event_records[1].uint_annotations.at("value") == 7);

    const DecodedTrack* marks = childTrack(trace, "marks", unit_track->uuid);
    CHECK(marks != nullptr);
    auto mark_records = eventsOn(trace, marks->uuid);
    CHECK(mark_records.size() == 1);
    CHECK(mark_records[0].type == 3 && mark_records[0].timestamp == 12);
    CHECK(mark_records[0].name == "ready");
    CHECK(mark_records[0].flow_ids == std::vector<uint64_t>{42});

    const DecodedTrack* function_stall = childTrack(trace, "function_stall", unit_track->uuid);
    CHECK(function_stall != nullptr);
    auto function_stall_events = eventsOn(trace, function_stall->uuid);
    CHECK(function_stall_events.size() == 2);
    CHECK(function_stall_events[0].type == 1 && function_stall_events[0].timestamp == 20);
    CHECK(function_stall_events[0].name == "blocked");
    CHECK(function_stall_events[0].uint_annotations.at("fq_size") == 5);
    CHECK(function_stall_events[1].type == 2 && function_stall_events[1].timestamp == 25);

    const DecodedTrack* stall = childTrack(trace, "stall", unit_track->uuid);
    CHECK(stall != nullptr);
    auto stall_events = eventsOn(trace, stall->uuid);
    CHECK(stall_events.size() == 2);
    CHECK(stall_events[0].type == 1 && stall_events[0].timestamp == 30);
    CHECK(stall_events[0].name == "out_blocked");
    CHECK(stall_events[0].uint_annotations.at("out_rem") == 0);
    CHECK(stall_events[1].type == 2 && stall_events[1].timestamp == 35);

    const DecodedTrack* function_occ = childTrack(trace, "function_occ", unit_track->uuid);
    const DecodedTrack* member_occ = childTrack(trace, "member_occ", unit_track->uuid);
    const DecodedTrack* member_gauge_track = childTrack(trace, "member_gauge", unit_track->uuid);
    CHECK(function_occ != nullptr && function_occ->is_counter);
    CHECK(member_occ != nullptr && member_occ->is_counter);
    CHECK(member_gauge_track != nullptr && member_gauge_track->is_counter);
    CHECK(eventsOn(trace, function_occ->uuid)[0].counter_value.value() == 6);
    CHECK(eventsOn(trace, member_occ->uuid)[0].counter_value.value() == 7);
    auto member_gauge_events = eventsOn(trace, member_gauge_track->uuid);
    CHECK(member_gauge_events.size() == 2);
    CHECK(member_gauge_events[0].timestamp == 40 &&
          member_gauge_events[0].counter_value.value() == 2);
    CHECK(member_gauge_events[1].timestamp == 41 &&
          member_gauge_events[1].counter_value.value() == 3);

    const DecodedTrack* function_used = childTrack(trace, "function_queue.used", unit_track->uuid);
    const DecodedTrack* function_free = childTrack(trace, "function_queue.free", unit_track->uuid);
    const DecodedTrack* member_used = childTrack(trace, "member_queue.used", unit_track->uuid);
    const DecodedTrack* member_free = childTrack(trace, "member_queue.free", unit_track->uuid);
    const DecodedTrack* member_capacity_used =
        childTrack(trace, "member_capacity.used", unit_track->uuid);
    const DecodedTrack* member_capacity_free =
        childTrack(trace, "member_capacity.free", unit_track->uuid);
    CHECK(function_used && function_used->is_counter);
    CHECK(function_free && function_free->is_counter);
    CHECK(member_used && member_used->is_counter);
    CHECK(member_free && member_free->is_counter);
    CHECK(member_capacity_used && member_capacity_used->is_counter);
    CHECK(member_capacity_free && member_capacity_free->is_counter);
    CHECK(eventsOn(trace, function_used->uuid)[0].counter_value.value() == 6);
    CHECK(eventsOn(trace, function_free->uuid)[0].counter_value.value() == 4);
    CHECK(eventsOn(trace, member_used->uuid)[0].counter_value.value() == 7);
    CHECK(eventsOn(trace, member_free->uuid)[0].counter_value.value() == 3);

    auto member_capacity_used_events = eventsOn(trace, member_capacity_used->uuid);
    auto member_capacity_free_events = eventsOn(trace, member_capacity_free->uuid);
    CHECK(member_capacity_used_events.size() == 2);
    CHECK(member_capacity_free_events.size() == 2);
    CHECK(member_capacity_used_events[0].counter_value.value() == 4);
    CHECK(member_capacity_free_events[0].counter_value.value() == 4);
    CHECK(member_capacity_used_events[1].counter_value.value() == 5);
    CHECK(member_capacity_free_events[1].counter_value.value() == 3);

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_pipeline_counter_track_ordering() {
    std::cout << "Testing pipeline/counter track ordering ranks... ";

    const std::string out_dir = "/tmp/chronon_timeline_api_track_order";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(256 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);
    backend.setSourceNameLookup(
        [](uint16_t id) -> std::string_view { return id == 1 ? "fetch" : ""; });
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);
        ctx.enableCategory(category::TRACE | LANE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);

        // Counter descriptor is created before pipeline descriptors. The
        // framework rank must still keep counters after pipeline lanes in
        // Perfetto's explicit child ordering.
        unit.cycle = 10;
        unit.occ.sample(4);
        unit.cycle = 11;
        unit.pipeStage<0, "BP0">(LANE_CAT, 100);
        unit.cycle = 12;
        unit.pipeStage<0, "BP1">(LANE_CAT, 101);
        unit.cycle = 13;
        unit.pipeStage<0, "BP0">(LANE_CAT, 0);
        unit.cycle = 14;
        unit.pipeStage<0, "BP1">(LANE_CAT, 0);

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);

    const DecodedTrack* fetch = findTrackByName(trace, "fetch");
    CHECK(fetch != nullptr);
    const DecodedTrack* occ = childTrack(trace, "occ", fetch->uuid);
    const DecodedTrack* bp0 = childTrack(trace, "pipe 0 stage BP0", fetch->uuid);
    const DecodedTrack* bp1 = childTrack(trace, "pipe 0 stage BP1", fetch->uuid);
    CHECK(occ && occ->is_counter && occ->sibling_order_rank);
    CHECK(bp0 && bp0->sibling_order_rank);
    CHECK(bp1 && bp1->sibling_order_rank);
    CHECK(*bp0->sibling_order_rank < *occ->sibling_order_rank);
    CHECK(*bp1->sibling_order_rank < *occ->sibling_order_rank);

    auto bp0_events = eventsOn(trace, bp0->uuid);
    auto bp1_events = eventsOn(trace, bp1->uuid);
    CHECK(bp0_events.size() == 4);
    CHECK(bp1_events.size() == 4);
    CHECK(bp0_events[2].type == 1 && bp0_events[2].timestamp == 13);
    CHECK(bp1_events[2].type == 1 && bp1_events[2].timestamp == 14);
    CHECK(bp0_events[2].name.starts_with("0"));
    CHECK(bp1_events[2].name.starts_with("0"));
    CHECK(bp0_events[2].flow_ids == std::vector<uint64_t>{0});
    CHECK(bp1_events[2].flow_ids == std::vector<uint64_t>{0});

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_disabled_pipeline_skips_track_registration() {
    std::cout << "Testing disabled pipeline skips track registration... ";

    ObservationQueue queue(256 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);

    TestUnit unit;
    unit.setObservationContext(&ctx);
    unit.cycle = 20;

    const size_t tracks_before = TimelineTrackRegistry::instance().size();
    unit.pipe<98, 7>(LANE_CAT, 100, arg<"pc">(0x100ULL));
    unit.pipeStage<98, "DISABLED">(LANE_CAT, 101, arg<"pc">(0x104ULL));
    unit.pipeStageHex<98, "DISABLED_HEX">(LANE_CAT, 102, arg<"pc">(0x108ULL));

    CHECK(TimelineTrackRegistry::instance().size() == tracks_before);

    std::cout << "PASSED\n";
}

void test_timeline_event_producer_gate() {
    std::cout << "Testing timeline event producer gate... ";

    ObservationQueue queue(256 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);
    ctx.enableCategory(category::TRACE | LANE_CAT.mask());

    TestUnit unit;
    unit.setObservationContext(&ctx);

    CHECK(ctx.timelineEventsEnabled());
    ctx.setTimelineEventsEnabled(false);
    CHECK(!ctx.timelineEventsEnabled());

    unit.cycle = 20;
    CHECK(!unit.mshr.begin(0, LANE_CAT, "disabled"_ev, flow(7), arg<"addr">(0x100ULL)));
    unit.cycle = 21;
    CHECK(!unit.mshr.end(0));
    unit.cycle = 22;
    CHECK(!unit.port.instant(0, LANE_CAT, "disabled_instant"_ev));

    const auto& trace_stats = ctx.observationStats().get<ObservationChannel::Trace>();
    CHECK(trace_stats.emitted == 0);
    CHECK(trace_stats.dropped == 0);

    std::cout << "PASSED\n";
}

void test_lookahead_commit_rollback() {
    std::cout << "Testing lookahead commit/rollback of timeline events... ";

    const std::string out_dir = "/tmp/chronon_timeline_api_lookahead";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(256 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_compress = false;
    // First-class timeline records must flow even when the legacy trace<>()
    // instant mirror is disabled (#43 review).
    cfg.timeline_trace_events = false;

    ObservationBackend backend(queue, cfg);
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "u", 1);
        ctx.enableCategory(category::TRACE | LANE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);

        ctx.setLookaheadMode(true);

        // Speculative span discarded by rollback.
        unit.cycle = 300;
        unit.mshr.begin(0, LANE_CAT, "speculative"_ev);
        unit.stall.update(true, LANE_CAT, "spec_stall"_ev, arg<"reason">(uint64_t{1}));
        CHECK(unit.stall.isOpen());
        unit.member_gauge.sampleOnChange(LANE_CAT, 7);
        unit.member_capacity.sampleOnChange(LANE_CAT, 3, 8);
        unit.cycle = 310;
        unit.mshr.end(0);
        ctx.rollbackEpoch();
        CHECK(!unit.stall.isOpen());

        // Committed span survives.
        unit.cycle = 320;
        unit.mshr.begin(0, LANE_CAT, "committed"_ev);
        unit.stall.update(true, LANE_CAT, "committed_stall"_ev, arg<"reason">(uint64_t{1}));
        unit.member_gauge.sampleOnChange(LANE_CAT, 7);
        unit.member_capacity.sampleOnChange(LANE_CAT, 3, 8);
        unit.cycle = 325;
        unit.stall.update(false, LANE_CAT, "committed_stall"_ev);
        unit.cycle = 330;
        unit.mshr.end(0);
        ctx.commitEpoch();

        // A helper can be idle across a committed epoch and then see a later
        // rollback before its next use. The committed helper cache must win.
        unit.cycle = 340;
        unit.stall.update(true, LANE_CAT, "cross_epoch_stall"_ev, arg<"reason">(uint64_t{2}));
        unit.member_gauge.sampleOnChange(LANE_CAT, 9);
        unit.member_capacity.sampleOnChange(LANE_CAT, 4, 8);
        ctx.commitEpoch();
        for (int i = 0; i < 80; ++i) {
            ctx.commitEpoch();
        }
        CHECK(unit.stall.isOpen());
        ctx.rollbackEpoch();
        CHECK(unit.stall.isOpen());

        unit.cycle = 350;
        unit.stall.update(false, LANE_CAT, "cross_epoch_stall"_ev);
        unit.member_gauge.sampleOnChange(LANE_CAT, 9);
        unit.member_capacity.sampleOnChange(LANE_CAT, 4, 8);
        ctx.commitEpoch();

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);
    CHECK(trace.events.size() == 12);

    const DecodedTrack* slot0 = findTrackByName(trace, "mshr[0]");
    CHECK(slot0 != nullptr);
    auto slot0_events = eventsOn(trace, slot0->uuid);
    CHECK(slot0_events.size() == 2);
    CHECK(slot0_events[0].type == 1 && slot0_events[0].timestamp == 320);
    CHECK(slot0_events[0].name == "committed");
    CHECK(slot0_events[1].type == 2 && slot0_events[1].timestamp == 330);

    const DecodedTrack* stall = findTrackByName(trace, "stall");
    CHECK(stall != nullptr);
    auto stall_events = eventsOn(trace, stall->uuid);
    CHECK(stall_events.size() == 4);
    CHECK(stall_events[0].type == 1 && stall_events[0].timestamp == 320);
    CHECK(stall_events[0].name == "committed_stall");
    CHECK(stall_events[0].uint_annotations.at("reason") == 1);
    CHECK(stall_events[1].type == 2 && stall_events[1].timestamp == 325);
    CHECK(stall_events[2].type == 1 && stall_events[2].timestamp == 340);
    CHECK(stall_events[2].name == "cross_epoch_stall");
    CHECK(stall_events[2].uint_annotations.at("reason") == 2);
    CHECK(stall_events[3].type == 2 && stall_events[3].timestamp == 350);

    const DecodedTrack* member_gauge = findTrackByName(trace, "member_gauge");
    CHECK(member_gauge != nullptr && member_gauge->is_counter);
    auto gauge_events = eventsOn(trace, member_gauge->uuid);
    CHECK(gauge_events.size() == 2);
    CHECK(gauge_events[0].timestamp == 320 && gauge_events[0].counter_value.value() == 7);
    CHECK(gauge_events[1].timestamp == 340 && gauge_events[1].counter_value.value() == 9);

    const DecodedTrack* capacity_used = findTrackByName(trace, "member_capacity.used");
    const DecodedTrack* capacity_free = findTrackByName(trace, "member_capacity.free");
    CHECK(capacity_used != nullptr && capacity_used->is_counter);
    CHECK(capacity_free != nullptr && capacity_free->is_counter);
    auto capacity_used_events = eventsOn(trace, capacity_used->uuid);
    auto capacity_free_events = eventsOn(trace, capacity_free->uuid);
    CHECK(capacity_used_events.size() == 2);
    CHECK(capacity_free_events.size() == 2);
    CHECK(capacity_used_events[0].timestamp == 320 &&
          capacity_used_events[0].counter_value.value() == 3);
    CHECK(capacity_free_events[0].timestamp == 320 &&
          capacity_free_events[0].counter_value.value() == 5);
    CHECK(capacity_used_events[1].timestamp == 340 &&
          capacity_used_events[1].counter_value.value() == 4);
    CHECK(capacity_free_events[1].timestamp == 340 &&
          capacity_free_events[1].counter_value.value() == 4);

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

/// Informational only: producer-side cost of lane events vs the trace<>
/// instant path, with the backend live and draining.
void measure_producer_cost() {
    std::cout << "Measuring producer-side event cost (informational)...\n";

    const std::string out_dir = "/tmp/chronon_timeline_api_bench";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(1024 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.timeline_compress = true;

    ObservationBackend backend(queue, cfg);
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "u", 1);
        ctx.enableCategory(category::TRACE | LANE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);

        constexpr uint64_t N = 200000;

        auto t0 = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < N; ++i) {
            unit.cycle = i;
            unit.mshr.begin(static_cast<uint16_t>(i & 3), LANE_CAT, "op"_ev, flow(i),
                            arg<"addr">(i * 64));
            unit.mshr.end(static_cast<uint16_t>(i & 3));
        }
        auto t1 = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < N; ++i) {
            unit.cycle = N + i;
            unit.trace<"op addr=0x{:x}">(LANE_CAT, i * 64);
        }
        auto t2 = std::chrono::steady_clock::now();

        auto ns = [](auto a, auto b) {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count();
        };
        std::cout << "  lane begin+end (flow + 1 arg): " << ns(t0, t1) / (2.0 * N) << " ns/event\n";
        std::cout << "  trace<> instant (1 arg):       " << ns(t1, t2) / static_cast<double>(N)
                  << " ns/event\n";

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    backend.stop();
    std::filesystem::remove_all(out_dir);
}

}  // namespace

int main() {
    std::cout << "=== Timeline API Tests ===\n";

    test_lanes_end_to_end();
    test_path_track_hierarchy();
    test_high_bit_category();
    test_trace_pipe_category_is_plain_trace();
    test_temporal_filter_span_semantics();
    test_timeline_observe_convenience_api();
    test_pipeline_counter_track_ordering();
    test_disabled_pipeline_skips_track_registration();
    test_timeline_event_producer_gate();
    test_lookahead_commit_rollback();
    measure_producer_cost();

    std::cout << "\nAll tests PASSED\n";
    return 0;
}

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
