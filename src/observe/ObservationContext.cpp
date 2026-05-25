// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// ObservationContext.cpp
//
// Implementation of ObservationContext methods.

#include "ObservationContext.hpp"

#include "ObservationManager.hpp"

namespace chronon::observe {

void ObservationContext::registerAllCounters(ObservationManager* manager) {
    if (!manager) {
        return;
    }

    const std::string& unit_name = unit_name_;
    auto& counter_array = counters_.counters();

    // Register each counter address along with its name
    for (size_t i = 0; i < counter_array.size(); ++i) {
        CounterId id = makeCounterId(static_cast<uint32_t>(i));
        const auto& info = counters_.info(id);
        manager->registerCounter(unit_name, id, &counter_array[i], info.name);
    }
}

}  // namespace chronon::observe
