// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "../TestAssertions.hpp"
#include "observe/EventCounter.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/PipelineApi.hpp"
#include "observe/TimelineTrack.hpp"

using namespace chronon::observe;

namespace {
inline const auto CAT = Category<"registry_lifecycle">{};

struct Unit : ObservableUnit {
    TimelineLane lane{this, "mshr", 4};
    EventCounter ops{this, "ops"};
    uint64_t cycle = 0;
    mutable size_t cycle_reads = 0;

    uint64_t getObserveCycle() const noexcept override {
        ++cycle_reads;
        return cycle;
    }
};

void disabled() {
    auto& registry = TimelineTrackRegistry::instance();
    const size_t before = registry.size();
    for (int gate = 0; gate < 3; ++gate) {
        for (int i = 0; i < 1000; ++i) {
            ObservationContext ctx(nullptr, [] { return 0ULL; }, 0, "unit", 1);
            ctx.enableCategory(CAT.mask());
            ctx.setTraceChannelEnabled(gate == 1);
            ctx.setTimelineEventsEnabled(gate == 0);
            Unit unit;
            unit.setObservationContext(&ctx);
            ++unit.ops;
            CHECK(unit.ops.get() == 1);
            CHECK(unit.lane.observationContext() == &ctx);
            CHECK(!unit.lane.isRegistered());
            CHECK(unit.lane.trackId() == 0);
            CHECK(!unit.lane.begin(0, CAT, "disabled"_ev));
            CHECK(!unit.lane.instant(0, CAT, "disabled"_ev));
            CHECK(!unit.lane.end(0));
            CHECK(unit.cycle_reads == 0);
        }
        CHECK(registry.size() == before);
    }
}

void enabled() {
    auto& registry = TimelineTrackRegistry::instance();
    const size_t before = registry.size();
    ObservationContext keeper_ctx(nullptr, [] { return 0ULL; }, 0, "keeper", 2);
    Unit keeper;
    keeper.setObservationContext(&keeper_ctx);
    const uint32_t keeper_id = keeper.lane.trackId();
    const auto* keeper_info = &registry.get(keeper_id);
    uint32_t repeated_id = 0;
    for (int i = 0; i < 2000; ++i) {
        {
            ObservationContext ctx(nullptr, [] { return 0ULL; }, 0, "unit", 1);
            Unit unit;
            unit.setObservationContext(&ctx);
            CHECK(unit.lane.isRegistered());
            const uint32_t id = unit.lane.trackId();
            if (i == 0) repeated_id = id;
            CHECK(id == repeated_id);
            CHECK(id != keeper_id);
            CHECK(registry.get(id).source_id == 1);
            CHECK(registry.get(id).lanes == 4);
        }
        CHECK(registry.size() == before + 2);
        CHECK(&registry.get(keeper_id) == keeper_info);
        CHECK(keeper_info->source_id == 2 && keeper_info->name == "mshr");
    }
}

void lateEnable() {
    auto& registry = TimelineTrackRegistry::instance();
    const size_t before = registry.size();
    ObservationContext ctx(nullptr, [] { return 0ULL; }, 0, "unit", 1);
    ctx.enableCategory(CAT.mask());
    ctx.setTraceChannelEnabled(false);
    ctx.setTimelineEventsEnabled(false);
    ctx.setLookaheadMode(true);
    Unit unit;
    unit.setObservationContext(&ctx);
    // The pending member list was consumed by context attachment. This late
    // declaration must still get a different identity from the first member.
    TimelineLane late(&unit, "mshr", 4);
    CHECK(registry.size() == before);
    CHECK(!late.isRegistered() && late.observationContext() == &ctx);
    ctx.setTraceChannelEnabled(true);
    CHECK(!unit.lane.begin(0, CAT, "disabled"_ev));
    CHECK(registry.size() == before);
    ctx.setTimelineEventsEnabled(true);
    CHECK(!unit.lane.end(0));  // An orphan end must not register a track.
    ctx.filter().addCycleRange(10, 20);
    CHECK(!unit.lane.instant(0, CAT, "filtered"_ev));
    CHECK(registry.size() == before);
    unit.cycle = 10;
    CHECK(unit.lane.begin(0, CAT, "enabled_later"_ev));
    const uint32_t id = unit.lane.trackId();
    CHECK(id != 0 && registry.size() == before + 1);
    unit.cycle = 12;
    CHECK(unit.lane.end(0));
    CHECK(late.instant(0, CAT, "late_member"_ev));
    CHECK(late.trackId() != id && registry.size() == before + 2);
    ctx.setTraceChannelEnabled(false);
    CHECK(!unit.lane.instant(0, CAT, "off_again"_ev));
    ctx.setTraceChannelEnabled(true);
    CHECK(unit.lane.instant(0, CAT, "on_again"_ev));
    CHECK(unit.lane.trackId() == id && registry.size() == before + 2);
    CHECK(ctx.observationStats().get<ObservationChannel::Trace>().emitted == 4);
    ctx.rollbackEpoch();
}

void identity() {
    auto& registry = TimelineTrackRegistry::instance();
    const size_t before = registry.size();
    const TimelineTrackInfo info{"a long track name with owned string storage", 1, 4};
    const uint32_t id = registry.registerTrack(info);
    const auto* saved = &registry.get(id);
    CHECK(registry.registerTrack(info) == id);
    auto different = info;
    different.name += " other";
    CHECK(registry.registerTrack(different) != id);
    different = info;
    different.source_id = 2;
    CHECK(registry.registerTrack(different) != id);
    different = info;
    different.lanes = 8;
    CHECK(registry.registerTrack(different) != id);
    different = info;
    different.layout = TimelineTrackInfo::Layout::Pipeline;
    CHECK(registry.registerTrack(different) != id);
    CHECK(registry.size() == before + 5);

    for (int i = 0; i < 1000; ++i) {
        registry.registerTrack({"churn " + std::to_string(i), 3, 1});
    }
    CHECK(&registry.get(id) == saved);
    CHECK(saved->name == info.name && saved->lanes == 4);
    CHECK(registry.registerTrack(info) == id);
    CHECK(registry.get(0).name.empty());

    ObservationContext ctx(nullptr, [] { return 0ULL; }, 0, "unit", 1);
    Unit unit;
    TimelineLane before_attach(&unit, "mshr", 4);
    unit.setObservationContext(&ctx);
    TimelineLane after_attach(&unit, "mshr", 4);
    CHECK(unit.lane.trackId() != before_attach.trackId());
    CHECK(unit.lane.trackId() != after_attach.trackId());
    CHECK(before_attach.trackId() != after_attach.trackId());
    const uint32_t shared = registry.registerTrack({"mshr", 1, 4});
    CHECK(shared != unit.lane.trackId());
    CHECK(shared != before_attach.trackId() && shared != after_attach.trackId());
}

struct CachedSite {
    static std::string trackName() { return "cached timeline"; }
};

std::array<uint32_t, 4> cachedIds(ObservationContext& ctx) {
    using StaticStage = pipeline_detail::NamedPipelineStage<0, "STATIC">;
    return {timeline_detail::resolveTrackForSource<CachedSite>(ctx.sourceId(), 4),
            pipeline_detail::resolvePipelineTrack<StaticStage>(&ctx),
            pipeline_detail::resolveRuntimePipelineTrack<"RUNTIME">(&ctx, 2),
            pipeline_detail::resolveRuntimePipelineTrack<"RUNTIME">(&ctx, 17)};
}

void caches() {
    auto& registry = TimelineTrackRegistry::instance();
    const size_t before = registry.size();
    std::array<std::array<uint32_t, 4>, 2> expected{};
    for (int i = 0; i < 1000; ++i) {
        const size_t source = i % 2;
        ObservationContext ctx(
            nullptr, [] { return 0ULL; }, 0, "unit", static_cast<uint16_t>(source + 1));
        const auto ids = cachedIds(ctx);
        if (i < 2) expected[source] = ids;
        CHECK(ids == expected[source]);
        Unit unit;
        unit.setObservationContext(&ctx);
        for (uint32_t id : ids) {
            CHECK(registry.get(id).source_id == source + 1);
            CHECK(!registry.get(id).name.empty());
        }
        CHECK(registry.get(ids[1]).layout == TimelineTrackInfo::Layout::Pipeline);
        CHECK(registry.get(ids[2]).name == "pipe 2 stage RUNTIME");
        CHECK(registry.get(ids[3]).name == "pipe 17 stage RUNTIME");
        if (i >= 1) CHECK(registry.size() == before + 10);
    }
}

void concurrent() {
    auto& registry = TimelineTrackRegistry::instance();
    const size_t before = registry.size();
    constexpr size_t threads = 8, keys = 64;
    std::array<std::array<uint32_t, keys>, threads> ids{};
    std::vector<std::thread> workers;
    for (size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            for (size_t i = 0; i < 1000; ++i) {
                const size_t key = i % keys;
                const auto name = "concurrent track " + std::to_string(key);
                const uint32_t id = registry.registerTrack({name, 1, 4});
                if (i < keys) ids[t][key] = id;
                CHECK(ids[t][key] == id);
                CHECK(registry.get(id).name == name);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    for (size_t t = 1; t < threads; ++t) CHECK(ids[t] == ids[0]);
    CHECK(registry.size() == before + keys);
}
}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string mode = argv[1];
    if (mode == "disabled")
        disabled();
    else if (mode == "enabled")
        enabled();
    else if (mode == "late_enable")
        lateEnable();
    else if (mode == "identity")
        identity();
    else if (mode == "caches")
        caches();
    else if (mode == "concurrent")
        concurrent();
    else
        CHECK(false);
    std::cout << "Timeline registry " << mode << ": PASSED\n";
}
