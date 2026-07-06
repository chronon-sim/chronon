// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// LocalCounter.cpp
//
// Implementation of Counter (per-instance counters).

#include "LocalCounter.hpp"

#include "ObservableUnit.hpp"

namespace chronon::observe {

void ObservableUnit::initializePendingCounters() { attachPending_(pending_counters_); }

Counter::Counter(ObservableUnit* owner, std::string_view name, std::string_view description,
                 std::string_view unit)
    : Counter(counter_detail::InternalConstructionTag{}, owner, name, description, unit) {}

Counter::Counter(counter_detail::InternalConstructionTag, ObservableUnit* owner,
                 std::string_view name, std::string_view description, std::string_view unit)
    : owner_(owner), name_(name), description_(description), unit_(unit) {
    if (owner_) {
        owner_->registerCounter(this);
    }
}

Counter::Counter(Counter&& other) noexcept
    : owner_(other.owner_),
      name_(std::move(other.name_)),
      description_(std::move(other.description_)),
      unit_(std::move(other.unit_)),
      ctx_(other.ctx_),
      id_(other.id_),
      registered_(other.registered_) {
    other.owner_ = nullptr;
    other.ctx_ = nullptr;
    other.registered_ = false;
}

Counter& Counter::operator=(Counter&& other) noexcept {
    if (this != &other) {
        owner_ = other.owner_;
        name_ = std::move(other.name_);
        description_ = std::move(other.description_);
        unit_ = std::move(other.unit_);
        ctx_ = other.ctx_;
        id_ = other.id_;
        registered_ = other.registered_;

        other.owner_ = nullptr;
        other.ctx_ = nullptr;
        other.registered_ = false;
    }
    return *this;
}

void Counter::onContextAttached(ObservationContext* ctx) {
    if (!ctx || registered_) {
        return;
    }

    ctx_ = ctx;

    // Register the counter with the context and get its ID
    id_ = ctx_->counters().addCounter(name_, description_, unit_);
    registered_ = true;
}

}  // namespace chronon::observe
