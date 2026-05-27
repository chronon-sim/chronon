// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "ObservationContext.hpp"

#include "CounterRegistry.hpp"

namespace chronon::observe {

void ObservationContext::registerAllCounters(CounterRegistry* registry) {
    if (!registry) {
        return;
    }

    const std::string& unit_name = unit_name_;
    auto& counter_array = counters_.counters();

    for (size_t i = 0; i < counter_array.size(); ++i) {
        CounterId id = makeCounterId(static_cast<uint32_t>(i));
        const auto& info = counters_.info(id);
        registry->registerCounter(unit_name, id, &counter_array[i], info.name);
    }
}

}  // namespace chronon::observe
