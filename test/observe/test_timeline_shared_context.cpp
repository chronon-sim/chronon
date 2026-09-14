// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <chrono>
#include <filesystem>
#include <memory>
#include <set>
#include <string>

#include "PftraceTestDecoder.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/ObservationBackend.hpp"
#include "observe/TimelineTrack.hpp"

using namespace chronon::observe;
using namespace pftrace_test;

namespace {
inline const auto CAT = Category<"shared_context">{};

struct Alu : ObservableUnit {
    TimelineLane ex{this, "ex"};
    uint64_t cycle = 0;
    uint64_t getObserveCycle() const noexcept override { return cycle; }
};
}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string mode = argv[1];
    CHECK(mode == "enabled" || mode == "late_enable");
    const auto root = std::filesystem::temp_directory_path() /
                      ("chronon-shared-context-" + mode + "-" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    auto& registry = TimelineTrackRegistry::instance();
    const size_t before = registry.size();
    std::array<uint32_t, 4> expected{};
    for (size_t session = 0; session < 8; ++session) {
        ObservationQueue queue(256 * 1024);
        {
            ObservationContext ctx(&queue, [] { return 0ULL; }, 0, "cpu", 1);
            ctx.enableCategory(CAT.mask());
            ctx.setTraceChannelEnabled(mode == "enabled");
            ctx.setLookaheadMode(true);
            std::array<std::unique_ptr<Alu>, 4> alus;
            for (auto& alu : alus) {
                alu = std::make_unique<Alu>();
                alu->setObservationContext(&ctx);
            }
            ctx.setTraceChannelEnabled(true);
            std::set<uint32_t> ids;
            for (size_t i = 0; i < alus.size(); ++i) {
                alus[i]->cycle = 10 + i;
                CHECK(alus[i]->ex.begin(0, CAT, "execute"_ev, arg<"alu">(i)));
                const uint32_t id = alus[i]->ex.trackId();
                CHECK(ids.insert(id).second);
                if (session == 0) expected[i] = id;
                CHECK(id == expected[i]);
            }
            for (size_t i = 0; i < alus.size(); ++i) {
                alus[i]->cycle = 30 + i;
                CHECK(alus[i]->ex.end(0));
            }
            ctx.commitEpoch();
        }
        ObservationBackend::Config config;
        config.output_dir = (root / std::to_string(session)).string();
        config.enable_counter_csv = false;
        config.enable_reordering = false;
        config.timeline_enabled = true;
        config.timeline_compress = false;
        ObservationBackend backend(queue, config);
        backend.setSourceNameLookup(
            [](uint16_t id) -> std::string_view { return id == 1 ? "cpu" : ""; });
        backend.start();
        const auto path = backend.outputDir() / "timeline.pftrace";
        backend.stop();
        const auto trace = decodeFile(path);
        std::set<uint64_t> seen_alus;
        size_t tracks = 0;
        for (const auto& track : trace.tracks) {
            if (track.name != "ex") continue;
            ++tracks;
            std::vector<DecodedTrackEvent> events;
            for (const auto& event : trace.events) {
                if (event.track_uuid == track.uuid) events.push_back(event);
            }
            CHECK(events.size() == 2);
            CHECK(events[0].type == 1 && events[1].type == 2);
            const uint64_t alu = events[0].uint_annotations.at("alu");
            CHECK(alu < 4 && seen_alus.insert(alu).second);
            CHECK(events[0].timestamp == 10 + alu && events[1].timestamp == 30 + alu);
        }
        CHECK(tracks == 4 && trace.events.size() == 8);
        CHECK(registry.size() == before + 4);
    }
    std::filesystem::remove_all(root);
    std::cout << "Shared-context " << mode << ": PASSED (4 independent ALUs, 8 sessions)\n";
}
