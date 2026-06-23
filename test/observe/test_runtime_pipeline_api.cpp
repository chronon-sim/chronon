// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string_view>
#include <thread>
#include <vector>

#include "PftraceTestDecoder.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/ObservationBackend.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationQueue.hpp"
#include "observe/ThreadContextManager.hpp"

using namespace chronon::observe;
using namespace pftrace_test;

namespace {

inline const auto PIPE_CAT = Category<"runtime_pipe_cat", "Runtime pipeline API test">{};

struct TestUnit : public ObservableUnit {
    uint64_t cycle = 0;
    uint64_t getObserveCycle() const noexcept override { return cycle; }
};

const DecodedTrack* childTrack(const DecodedTrace& trace, std::string_view name,
                               uint64_t parent_uuid) {
    for (const auto& track : trace.tracks) {
        if (track.name == name && track.parent_uuid == parent_uuid) {
            return &track;
        }
    }
    return nullptr;
}

std::vector<DecodedTrackEvent> eventsOn(const DecodedTrace& trace, uint64_t track_uuid) {
    std::vector<DecodedTrackEvent> out;
    for (const auto& event : trace.events) {
        if (event.track_uuid == track_uuid) {
            out.push_back(event);
        }
    }
    return out;
}

void test_runtime_pipe_tracks() {
    std::cout << "Testing runtime pipeline pipe tracks... ";

    const std::string out_dir = "/tmp/chronon_runtime_pipeline_api";
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
        ctx.enableCategory(category::TRACE | PIPE_CAT.mask());

        TestUnit unit;
        unit.setObservationContext(&ctx);
        unit.cycle = 10;
        unit.pipeStage<"RT">(2, PIPE_CAT, 200, arg<"src">(1u));
        unit.cycle = 11;
        unit.pipeStageHex<"RT">(3, PIPE_CAT, 0x300, arg<"src">(2u));

        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();

    DecodedTrace trace = decodeFile(timeline);
    const DecodedTrack* fetch = findTrackByName(trace, "fetch");
    CHECK(fetch != nullptr);
    const DecodedTrack* rt2 = childTrack(trace, "pipe 2 stage RT", fetch->uuid);
    const DecodedTrack* rt3 = childTrack(trace, "pipe 3 stage RT", fetch->uuid);
    CHECK(rt2 != nullptr);
    CHECK(rt3 != nullptr);

    auto rt2_events = eventsOn(trace, rt2->uuid);
    auto rt3_events = eventsOn(trace, rt3->uuid);
    CHECK(rt2_events.size() == 2);
    CHECK(rt3_events.size() == 2);
    CHECK(rt2_events[0].timestamp == 10 && rt2_events[0].name.starts_with("200"));
    CHECK(rt3_events[0].timestamp == 11 && rt3_events[0].name.starts_with("0x300"));
    CHECK(rt2_events[0].flow_ids == std::vector<uint64_t>{200});
    CHECK(rt3_events[0].flow_ids == std::vector<uint64_t>{0x300});

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_disabled_runtime_pipe_skips_track_registration() {
    std::cout << "Testing disabled runtime pipeline skips track registration... ";

    ObservationQueue queue(256 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);
    TestUnit unit;
    unit.setObservationContext(&ctx);

    const size_t tracks_before = TimelineTrackRegistry::instance().size();
    unit.pipeStage<"DISABLED_RT">(2, PIPE_CAT, 1);
    unit.pipeStageHex<"DISABLED_RT_HEX">(3, PIPE_CAT, 2);
    CHECK(TimelineTrackRegistry::instance().size() == tracks_before);

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Runtime Pipeline API Tests ===\n";
    test_runtime_pipe_tracks();
    test_disabled_runtime_pipe_skips_track_registration();
    std::cout << "\nAll tests PASSED\n";
    return 0;
}
