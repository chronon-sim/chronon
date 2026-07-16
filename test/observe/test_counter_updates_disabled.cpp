// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <cstdint>

#include "observe/EventCounter.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationQueue.hpp"

#ifndef CHRONON_ENABLE_COUNTER_UPDATES
#error "chronon_observe must publish CHRONON_ENABLE_COUNTER_UPDATES"
#endif

static_assert(CHRONON_ENABLE_COUNTER_UPDATES == 0);

using namespace chronon::observe;

namespace {

inline const auto TEST_EVENT = Category<"counter_updates_disabled", "Counter elision test">{};

class TestUnit : public ObservableUnit {
public:
    Counter raw{counter_detail::InternalConstructionTag{}, this, "raw"};
    EventCounter event{this, "event"};
};

}  // namespace

int main() {
    ObservationQueue queue;
    ObservationContext ctx(&queue, []() { return uint64_t{17}; }, 0, "counter_test");
    ctx.enableCategory(TEST_EVENT.mask());

    TestUnit unit;
    unit.setObservationContext(&ctx);

    ++unit.raw;
    unit.raw += 4;
    ++unit.event;
    unit.event += 2;
    unit.event.add(3);

    if (unit.raw.get() != 0 || unit.event.get() != 0) return 1;

    const uint64_t emitted_before = unit.channelStats<ObservationChannel::Trace>().emitted;
    unit.event.mark<"timeline_survives">(5, TEST_EVENT);

    if (unit.event.get() != 0) return 2;
    if (unit.channelStats<ObservationChannel::Trace>().emitted != emitted_before + 1) return 3;
    return 0;
}
