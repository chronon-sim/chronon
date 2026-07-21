// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// TimelineTrack.cpp
//
// Out-of-line pieces of the declarative timeline members (they need the
// complete ObservableUnit type).

#include "TimelineTrack.hpp"

#include "ObservableUnit.hpp"

namespace chronon::observe {

TimelineTrackBase::TimelineTrackBase(ObservableUnit* owner, std::string_view name, uint16_t lanes)
    : owner_(owner), name_(name), lanes_(lanes) {
    if (owner_) {
        owner_->registerTimelineTrack(this);
        // Late declaration: if the unit's context is already attached,
        // register immediately (mirrors Counter).
        if (owner_->observationContext()) {
            onContextAttached(owner_->observationContext());
        }
    }
}

void TimelineTrackBase::onContextAttached(ObservationContext* ctx) {
    if (registered_ || !ctx) {
        return;
    }
    ctx_ = ctx;
    track_id_ = TimelineTrackRegistry::instance().registerTrack({name_, ctx_->sourceId(), lanes_});
    registered_ = track_id_ != 0;
}

void TimelineTrackBase::stampCycle_() noexcept {
    ctx_->setCurrentCycleValue(owner_->getObserveCycle());
}

void ObservableUnit::initializePendingTimelineTracks() { attachPending_(pending_timeline_tracks_); }

}  // namespace chronon::observe
