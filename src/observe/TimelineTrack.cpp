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
        // attach immediately (mirrors Counter).
        if (owner_->observationContext()) {
            onContextAttached(owner_->observationContext());
        }
    }
}

TimelineTrackBase::~TimelineTrackBase() noexcept {
    if (ctx_ && !registered_) ctx_->removePendingTimelineTrack_(this);
}

void TimelineTrackBase::onContextAttached(ObservationContext* ctx) {
    if (registered_ || !ctx || ctx_ == ctx) {
        return;
    }
    if (ctx_) ctx_->removePendingTimelineTrack_(this);
    ctx_ = ctx;
    track_id_ = ctx_->attachTimelineTrack_(this);
    if (ctx_->timelineProducerEnabled()) {
        register_();
    }
}

void TimelineTrackBase::register_() {
    const uint32_t declaration_index = track_id_;
    track_id_ = TimelineTrackRegistry::instance().registerTrack({name_, ctx_->sourceId(), lanes_},
                                                                declaration_index);
    registered_ = track_id_ != 0;
}

uint32_t ObservationContext::attachTimelineTrack_(TimelineTrackBase* track) {
    if (timeline_declaration_count_ == std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("timeline declaration indices exhausted");
    }
    if (!timelineProducerEnabled()) pending_timeline_tracks_.push_back(track);
    return ++timeline_declaration_count_;
}

void ObservationContext::removePendingTimelineTrack_(TimelineTrackBase* track) noexcept {
    std::erase(pending_timeline_tracks_, track);
}

void ObservationContext::initializePendingTimelineTracks_() {
    for (auto* track : pending_timeline_tracks_) track->register_();
    pending_timeline_tracks_.clear();
}

void ObservationContext::detachPendingTimelineTracks_() noexcept {
    for (auto* track : pending_timeline_tracks_) {
        track->ctx_ = nullptr;
        track->track_id_ = 0;
    }
    pending_timeline_tracks_.clear();
}

void TimelineTrackBase::stampCycle_() noexcept {
    ctx_->setCurrentCycleValue(owner_->getObserveCycle());
}

void ObservableUnit::initializePendingTimelineTracks() { attachPending_(pending_timeline_tracks_); }

}  // namespace chronon::observe
