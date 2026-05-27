// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "Counter.hpp"
#include "DerivedCounter.hpp"
#include "Types.hpp"

namespace chronon::observe {
class ObservationContext;
}
namespace chronon::observe {
class ObservationQueue;
}

namespace chronon::observe {

class CounterRegistry {
public:
    struct CounterKey {
        std::string unit_name;
        CounterId id;
        std::string counter_name;

        bool operator==(const CounterKey& other) const {
            return unit_name == other.unit_name && id == other.id;
        }
    };

    struct CounterKeyHash {
        size_t operator()(const CounterKey& key) const {
            return std::hash<std::string>{}(key.unit_name) ^
                   (std::hash<uint32_t>{}(toIndex(key.id)) << 1);
        }
    };

    void registerCounter(const std::string& unit_name, CounterId id, SimpleCounter* counter_ptr,
                         const std::string& counter_name = "") {
        CounterKey key{unit_name, id, counter_name};
        counters_[key] = counter_ptr;
    }

    void reregisterAll(const std::vector<std::unique_ptr<ObservationContext>>& contexts);

    void dumpSnapshots(uint64_t cycle, ObservationQueue* queue,
                       const std::vector<std::unique_ptr<ObservationContext>>& contexts);

    const std::vector<DerivedCounterDef>& derivedDefs() const noexcept { return derived_defs_; }

    void clear() {
        counters_.clear();
        derived_defs_.clear();
    }

private:
    std::unordered_map<CounterKey, SimpleCounter*, CounterKeyHash> counters_;
    std::vector<DerivedCounterDef> derived_defs_;
};

}  // namespace chronon::observe
