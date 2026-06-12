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
#include "observe/TimelineTrack.hpp"

using namespace chronon::observe;
using namespace pftrace_test;

namespace {

inline const auto LANE_CAT = Category<"lane_cat", "Timeline API test category">{};

struct TestUnit : public ObservableUnit {
    TimelineLane mshr{this, "mshr", 4};
    TimelineLane port{this, "port"};
    TimelineCounter occ{this, "occ", "entries"};

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
    backend.start();

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "u", 1);
        ctx.enableCategory(category::TRACE | LANE_CAT.mask());
        ctx.filter().addCycleRange(0, 100);  // Observe cycles [0, 100] only.

        TestUnit unit;
        unit.setObservationContext(&ctx);

        // Begin inside the window, end outside: the end must still close the
        // span (ends are not temporally filtered).
        unit.cycle = 50;
        unit.mshr.begin(0, LANE_CAT, "inside"_ev);
        unit.cycle = 150;
        unit.mshr.end(0);

        // Begin outside the window: suppressed at the producer; its end is
        // dropped by the backend's open-span table.
        unit.cycle = 120;
        unit.mshr.begin(1, LANE_CAT, "outside"_ev);
        unit.cycle = 130;
        unit.mshr.end(1);

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

    std::filesystem::remove_all(out_dir);
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
        unit.cycle = 310;
        unit.mshr.end(0);
        ctx.rollbackEpoch();

        // Committed span survives.
        unit.cycle = 320;
        unit.mshr.begin(0, LANE_CAT, "committed"_ev);
        unit.cycle = 330;
        unit.mshr.end(0);
        ctx.commitEpoch();

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);
    CHECK(trace.events.size() == 2);
    CHECK(trace.events[0].type == 1 && trace.events[0].timestamp == 320);
    CHECK(trace.events[0].name == "committed");
    CHECK(trace.events[1].type == 2 && trace.events[1].timestamp == 330);

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
    test_temporal_filter_span_semantics();
    test_lookahead_commit_rollback();
    measure_producer_cost();

    std::cout << "\nAll tests PASSED\n";
    return 0;
}
