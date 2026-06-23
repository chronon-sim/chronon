// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "DerivedCounter.hpp"

#include "LocalCounter.hpp"
#include "ObservableUnit.hpp"
#include "ObservationContext.hpp"

namespace chronon::observe {

void ObservableUnit::initializePendingDerivedCounters() {
    attachPending_(pending_derived_counters_);
}

DerivedCounter::DerivedCounter(ObservableUnit* owner, std::string_view name,
                               std::string_view description,
                               std::initializer_list<std::reference_wrapper<const Counter>> sources,
                               ComputeFn compute)
    : owner_(owner),
      name_(name),
      description_(description),
      sources_(sources),
      compute_(std::move(compute)) {
    if (owner_) {
        owner_->registerDerivedCounter(this);
    }
}

void DerivedCounter::onContextAttached(ObservationContext* ctx) {
    if (!ctx) {
        return;
    }

    DerivedCounterDef def;
    def.unit_name = ctx->unitName();
    def.derived_name = name_;
    def.description = description_;
    def.source_names.reserve(sources_.size());
    for (const auto& src : sources_) {
        def.source_names.push_back(src.get().name());
    }
    def.compute = compute_;

    ctx->addDerivedCounterDef(std::move(def));
}

}  // namespace chronon::observe
