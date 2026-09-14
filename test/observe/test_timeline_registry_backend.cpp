// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "PftraceTestDecoder.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/ObservationBackend.hpp"
#include "observe/TimelineTrack.hpp"

using namespace chronon::observe;
using namespace pftrace_test;

namespace {
inline const auto CAT = Category<"registry_backend">{};

struct Unit : ObservableUnit {
    TimelineLane first{this, "same"};
    uint64_t cycle = 0;
    uint64_t getObserveCycle() const noexcept override { return cycle; }
};

std::vector<DecodedTrackEvent> eventsOn(const DecodedTrace& trace, uint64_t track) {
    std::vector<DecodedTrackEvent> events;
    for (const auto& event : trace.events) {
        if (event.track_uuid == track) events.push_back(event);
    }
    return events;
}

void checkTrace(const DecodedTrace& trace, const std::string& source, uint64_t session) {
    const auto* parent = findTrackByName(trace, source);
    CHECK(parent != nullptr);
    size_t same_tracks = 0, pipeline_tracks = 0;
    bool saw_first = false, saw_second = false;
    for (const auto& track : trace.tracks) {
        if (track.parent_uuid != parent->uuid) continue;
        const auto events = eventsOn(trace, track.uuid);
        if (track.name == "same") {
            ++same_tracks;
            CHECK(events.size() == 2);
            CHECK(events[0].type == 1 && events[1].type == 2);
            CHECK(events[0].uint_annotations.at("session") == session);
            if (events[0].name == "first") {
                saw_first = true;
                CHECK(events[0].timestamp == 10 && events[1].timestamp == 20);
            } else {
                saw_second = true;
                CHECK(events[0].name == "second");
                CHECK(events[0].timestamp == 11 && events[1].timestamp == 30);
            }
        } else if (track.name == "events") {
            CHECK(events.size() == 1 && events[0].timestamp == 31);
            CHECK(events[0].name == "after_destroy");
        } else if (track.name.starts_with("pipe ")) {
            ++pipeline_tracks;
            CHECK(events.size() == 2);
            CHECK(events[0].type == 1 && events[1].type == 2);
            const uint64_t cycle = track.name == "pipe 2 stage RT"    ? 32
                                   : track.name == "pipe 17 stage RT" ? 33
                                                                      : 34;
            if (cycle == 34) CHECK(track.name == "pipe 0 stage STATIC");
            CHECK(events[0].timestamp == cycle && events[1].timestamp == cycle + 1);
        }
    }
    CHECK(same_tracks == 2 && pipeline_tracks == 3);
    CHECK(saw_first && saw_second);
    CHECK(trace.events.size() == 11);
}
}  // namespace

int main(int argc, char** argv) {
    CHECK(argc <= 2);
    const auto root =
        argc == 2
            ? std::filesystem::path(argv[1])
            : std::filesystem::temp_directory_path() /
                  ("chronon-registry-backend-" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    auto& registry = TimelineTrackRegistry::instance();
    const size_t before = registry.size();
    std::array<uint32_t, 2> expected_ids{};
    for (uint64_t session = 0; session < 8; ++session) {
        ObservationQueue queue(256 * 1024);
        const std::string source = "source_" + std::to_string(session);
        {
            ObservationContext ctx(&queue, [] { return 0ULL; }, 0, source, 1);
            ctx.enableCategory(CAT.mask());
            ctx.setTraceChannelEnabled(session % 2 == 0);
            ctx.setLookaheadMode(true);
            Unit unit;
            unit.setObservationContext(&ctx);
            TimelineLane second(&unit, "same");  // Same name; independent spans.
            ctx.setTraceChannelEnabled(true);
            unit.cycle = 10;
            CHECK(unit.first.begin(0, CAT, "first"_ev, arg<"session">(session)));
            unit.cycle = 11;
            CHECK(second.begin(0, CAT, "second"_ev, arg<"session">(session)));
            const std::array ids{unit.first.trackId(), second.trackId()};
            CHECK(ids[0] != ids[1]);
            if (session == 0) expected_ids = ids;
            CHECK(ids == expected_ids);
            unit.cycle = 20;
            CHECK(unit.first.end(0));
            unit.cycle = 30;
            CHECK(second.end(0));
            unit.cycle = 31;
            unit.event<"after_destroy">(CAT);
            unit.cycle = 32;
            unit.pipeStage<"RT">(2, CAT, 100 + session);
            unit.cycle = 33;
            unit.pipeStage<"RT">(17, CAT, 200 + session);
            unit.cycle = 34;
            unit.pipeStage<0, "STATIC">(CAT, 300 + session);
            CHECK(ctx.observationStats().get<ObservationChannel::Trace>().emitted == 8);
            ctx.commitEpoch();
        }
        // Deliberately start the consumer only after both unit and context are
        // destroyed, so queued records must resolve retained metadata safely.
        ObservationBackend::Config cfg;
        cfg.output_dir = (root / std::to_string(session)).string();
        cfg.enable_counter_csv = false;
        cfg.enable_reordering = false;
        cfg.timeline_enabled = true;
        cfg.timeline_compress = session % 2 != 0;
        ObservationBackend backend(queue, cfg);
        backend.setSourceNameLookup([&](uint16_t id) -> std::string_view {
            return id == 1 ? std::string_view(source) : std::string_view{};
        });
        backend.start();
        const auto path = backend.outputDir() / "timeline.pftrace";
        backend.stop();
        checkTrace(decodeFile(path), source, session);
        CHECK(registry.size() == before + 6);
        if (argc == 2) std::cout << path.string() << '\n';
    }
    if (argc != 2) std::filesystem::remove_all(root);
    std::cout << "Timeline registry backend: PASSED (8 sessions, 6 retained tracks)\n";
}
