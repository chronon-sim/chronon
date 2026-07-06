// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
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

namespace {

inline const auto LANE_CAT = Category<"lane_cat", "Timeline API string arg test category">{};

struct TestUnit : public ObservableUnit {
    TimelineLane port{this, "port"};

    uint64_t cycle = 0;
    uint64_t getObserveCycle() const noexcept override { return cycle; }
};

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

void test_string_and_char_pointer_annotations() {
    std::cout << "Testing string and char pointer annotations... ";

    const std::string out_dir = "/tmp/chronon_timeline_string_args";
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

    char raw_buffer[] = "raw";
    const auto raw_addr = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(raw_buffer));

    {
        ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "u", 1);
        ctx.enableCategory(category::TRACE | LANE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);

        unit.cycle = 10;
        CHECK(unit.port.instant(0, LANE_CAT, "annotated"_ev, arg<"text">(std::string_view("ok")),
                                arg<"raw">(raw_buffer)));

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);
    const DecodedTrack* unit_track = findTrackByName(trace, "u");
    CHECK(unit_track != nullptr);
    const DecodedTrack* port = childTrack(trace, "port", unit_track->uuid);
    CHECK(port != nullptr);
    auto events = eventsOn(trace, port->uuid);
    CHECK(events.size() == 1);
    CHECK(events[0].string_annotations.at("text") == "ok");
    CHECK(events[0].pointer_annotations.at("raw") == raw_addr);

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_filtered_string_args_do_not_intern_values() {
    std::cout << "Testing filtered string args do not intern values... ";

    ObservationQueue queue(256 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);
    ctx.enableCategory(category::TRACE | LANE_CAT.mask());

    TestUnit unit;
    unit.setObservationContext(&ctx);
    unit.cycle = 0;

    const size_t values_before = AnnotationValueRegistry::instance().size();

    ctx.setTimelineEventsEnabled(false);
    unit.event<"filtered">(LANE_CAT, arg<"text">(std::string_view("timeline-disabled")));
    CHECK(AnnotationValueRegistry::instance().size() == values_before);

    ctx.setTimelineEventsEnabled(true);
    ctx.disableCategory(category::TRACE | LANE_CAT.mask());
    unit.event<"filtered">(LANE_CAT, arg<"text">(std::string_view("category-disabled")));
    CHECK(AnnotationValueRegistry::instance().size() == values_before);

    ctx.enableCategory(category::TRACE | LANE_CAT.mask());
    ctx.filter().addCycleRange(10, 20);
    unit.cycle = 30;
    unit.event<"filtered">(LANE_CAT, arg<"text">(std::string_view("out-of-window")));
    unit.pipeStage<7, "FILTERED">(LANE_CAT, 1, arg<"text">(std::string_view("pipeline-filtered")));
    CHECK(AnnotationValueRegistry::instance().size() == values_before);

    std::cout << "PASSED\n";
}

void test_lookahead_string_args_intern_on_commit_only() {
    std::cout << "Testing lookahead string args intern on commit only... ";

    ObservationQueue queue(256 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);
    ctx.enableCategory(category::TRACE | LANE_CAT.mask());
    ctx.setLookaheadMode(true);

    TestUnit unit;
    unit.setObservationContext(&ctx);

    const size_t values_before = AnnotationValueRegistry::instance().size();

    unit.cycle = 10;
    unit.event<"lookahead_rollback">(LANE_CAT,
                                     arg<"text">(std::string_view("lookahead-rollback-value")));
    CHECK(AnnotationValueRegistry::instance().size() == values_before);
    ctx.rollbackEpoch();
    CHECK(AnnotationValueRegistry::instance().size() == values_before);

    unit.cycle = 20;
    unit.event<"lookahead_commit">(LANE_CAT,
                                   arg<"text">(std::string_view("lookahead-commit-value")));
    CHECK(AnnotationValueRegistry::instance().size() == values_before);
    ctx.commitEpoch();
    CHECK(AnnotationValueRegistry::instance().size() == values_before + 1);

    std::cout << "PASSED\n";
}

void test_dropped_lookahead_events_restore_pending_strings() {
    std::cout << "Testing dropped lookahead events restore pending strings... ";

    ObservationQueue queue(256 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);
    ctx.enableCategory(category::TRACE | LANE_CAT.mask());
    ctx.setLookaheadMode(true);

    TestUnit unit;
    unit.setObservationContext(&ctx);

    const size_t values_before = AnnotationValueRegistry::instance().size();
    size_t emitted = 0;
    bool saw_drop = false;

    for (size_t i = 0; i < 200; ++i) {
        std::string value = "lookahead-buffer-fill-value-" + std::to_string(i);
        unit.cycle = i;
        if (unit.port.instant(0, LANE_CAT, "fill"_ev, arg<"text">(std::string_view(value)))) {
            ++emitted;
        } else {
            saw_drop = true;
            break;
        }
    }

    CHECK(saw_drop);
    CHECK(emitted > 0);
    CHECK(AnnotationValueRegistry::instance().size() == values_before);
    ctx.commitEpoch();
    CHECK(AnnotationValueRegistry::instance().size() == values_before + emitted);

    std::cout << "PASSED\n";
}

void test_dropped_non_lookahead_events_do_not_intern_values() {
    std::cout << "Testing dropped non-lookahead events do not intern values... ";

    ObservationQueue queue(256 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);
    ctx.enableCategory(category::TRACE | LANE_CAT.mask());

    TestUnit unit;
    unit.setObservationContext(&ctx);

    const size_t values_before = AnnotationValueRegistry::instance().size();
    size_t emitted = 0;
    bool saw_drop = false;

    for (size_t i = 0; i < 10000; ++i) {
        std::string value = "non-lookahead-buffer-fill-value-" + std::to_string(i);
        unit.cycle = 1000 + i;
        if (unit.port.instant(0, LANE_CAT, "queue_fill"_ev, arg<"text">(std::string_view(value)))) {
            ++emitted;
        } else {
            saw_drop = true;
            break;
        }
    }

    CHECK(saw_drop);
    CHECK(emitted > 0);
    CHECK(AnnotationValueRegistry::instance().size() == values_before + emitted);

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Timeline String Arg Tests ===\n";

    test_string_and_char_pointer_annotations();
    test_filtered_string_args_do_not_intern_values();
    test_lookahead_string_args_intern_on_commit_only();
    test_dropped_lookahead_events_restore_pending_strings();
    test_dropped_non_lookahead_events_do_not_intern_values();

    std::cout << "\nAll tests PASSED\n";
    return 0;
}
