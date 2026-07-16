// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <cstdint>
#include <iostream>

#include "../TestAssertions.hpp"
#include "observe/ObservableUnit.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationQueue.hpp"
#include "observe/TimelineTrack.hpp"

using namespace chronon::observe;

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

namespace {

inline const auto GATE_CAT = Category<"gate_cat">{};

struct GateTestUnit : public ObservableUnit {
    TimelineLane lane{this, "lane"};
    TimelineCounter counter{this, "counter", "entries"};

    uint64_t cycle = 0;
    mutable uint64_t cycle_reads = 0;
    uint64_t getObserveCycle() const noexcept override {
        ++cycle_reads;
        return cycle;
    }
};

}  // namespace

void test_timeline_event_producer_gate() {
    std::cout << "Testing timeline event producer gate... ";
    ObservationQueue queue(256 * 1024);
    ObservationContext ctx(&queue, []() { return 0ULL; }, 0, "fetch", 1);
    ctx.enableCategory(category::TRACE | GATE_CAT.mask());
    ctx.setTraceChannelEnabled(false);
    const size_t tracks_before = TimelineTrackRegistry::instance().size();
    GateTestUnit unit;
    unit.setObservationContext(&ctx);
    CHECK(unit.lane.isRegistered() && unit.counter.isRegistered());
    CHECK(unit.lane.trackId() == tracks_before + 1);
    CHECK(unit.counter.trackId() == tracks_before + 2);
    CHECK(TimelineTrackRegistry::instance().size() == tracks_before + 2);
    unit.cycle = 20;
    CHECK(!unit.lane.instant(0, GATE_CAT, "disabled"_ev));
    CHECK(!unit.lane.end(0));
    CHECK(!unit.counter.sample(3));
    CHECK(unit.cycle_reads == 0);
    const auto& trace_stats = ctx.observationStats().get<ObservationChannel::Trace>();
    CHECK(trace_stats.emitted == 0 && trace_stats.dropped == 0);

    ctx.setLookaheadMode(true);
    ctx.setTraceChannelEnabled(true);
    CHECK(ctx.filter().shouldObserve(category::TRACE));
    CHECK(TimelineTrackRegistry::instance().size() == tracks_before + 2);
    unit.cycle = 24;
    CHECK(unit.lane.instant(0, GATE_CAT, "enabled_later"_ev));
    unit.cycle = 25;
    CHECK(unit.counter.sample(3));
    CHECK(TimelineTrackRegistry::instance().size() == tracks_before + 2);
    CHECK(unit.cycle_reads == 2 && trace_stats.emitted == 2 && trace_stats.dropped == 0);
    ctx.rollbackEpoch();
    std::cout << "PASSED\n";
}

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
