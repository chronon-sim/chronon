// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdint>
#include <functional>
#include <initializer_list>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace chronon::observe {

class Counter;
class ObservableUnit;
class ObservationContext;

/// Receives snapshotted delta values for each source counter (declaration order) and
/// returns the derived value. Invoked on the backend thread at CSV dump time.
using ComputeFn = std::function<double(std::span<const uint64_t>)>;

/** @brief Transport struct carrying a derived-counter formula from unit to backend. */
struct DerivedCounterDef {
    std::string unit_name;
    std::string derived_name;
    std::string description;
    std::vector<std::string> source_names;
    ComputeFn compute;
};

/**
 * @brief Convenience formulas for common derived-counter patterns.
 *
 * Example:
 * @code
 *   DerivedCounter hit_rate_{this, "hit_rate", "Hit rate",
 *       {hits_, misses_}, DerivedFormula::Ratio};
 * @endcode
 */
namespace DerivedFormula {

/// a / (a + b) — hit rate, miss rate, etc.
inline constexpr auto Ratio = [](std::span<const uint64_t> v) -> double {
    uint64_t d = v[0] + v[1];
    return d > 0 ? static_cast<double>(v[0]) / d : 0.0;
};

/// a / b — IPC, throughput, etc.
inline constexpr auto Divide = [](std::span<const uint64_t> v) -> double {
    return v[1] > 0 ? static_cast<double>(v[0]) / v[1] : 0.0;
};

/// a * 1000 / b — MPKI and similar per-kilo-instruction metrics.
inline constexpr auto PerKilo = [](std::span<const uint64_t> v) -> double {
    return v[1] > 0 ? static_cast<double>(v[0]) * 1000.0 / v[1] : 0.0;
};

}  // namespace DerivedFormula

/**
 * @brief Computed counter declared as a unit member alongside raw Counters.
 *
 * Computation runs on the backend thread at CSV dump time, so there is no
 * simulation hot-path overhead.
 *
 * @code
 *   class Fetch : public TickableUnit, public ObservableUnit {
 *       Counter hits_{this, "hits", "Cache hits"};
 *       Counter misses_{this, "misses", "Cache misses"};
 *       DerivedCounter hit_rate_{this, "hit_rate", "Cache hit rate",
 *           {hits_, misses_}, DerivedFormula::Ratio};
 *   };
 * @endcode
 */
class DerivedCounter {
public:
    /**
     * @param owner       Owning ObservableUnit.
     * @param name        Derived counter name (appears as unit.name in CSV).
     * @param description Human-readable description.
     * @param sources     Source counters whose delta values are passed to @p compute.
     * @param compute     Function computing the derived value from source deltas.
     */
    DerivedCounter(ObservableUnit* owner, std::string_view name, std::string_view description,
                   std::initializer_list<std::reference_wrapper<const Counter>> sources,
                   ComputeFn compute);

    DerivedCounter(const DerivedCounter&) = delete;
    DerivedCounter& operator=(const DerivedCounter&) = delete;

    const std::string& name() const noexcept { return name_; }
    const std::string& description() const noexcept { return description_; }

private:
    friend class ObservableUnit;

    void onContextAttached(ObservationContext* ctx);

    ObservableUnit* owner_;
    std::string name_;
    std::string description_;
    std::vector<std::reference_wrapper<const Counter>> sources_;
    ComputeFn compute_;
};

}  // namespace chronon::observe
