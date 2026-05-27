// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// CRTP base that auto-dispatches tick() to tickPhase<Phase0|Phase1>() by cycle parity.

#pragma once

#include "../util/PipelinePhase.hpp"
#include "TickableUnit.hpp"

namespace chronon::sender {

/**
 * CRTP base for units using phase-aware pipeline registers.
 *
 * Eliminates the boilerplate phase-dispatch code. Override tickPhase<P>()
 * instead of tick().
 *
 * @code
 * class MyUnit : public PhasedTickableUnit<MyUnit> {
 *     StageReg<Data> reg_;
 * public:
 *     MyUnit() : PhasedTickableUnit("my_unit") {}
 *     template<ValidPhase P>
 *     void tickPhase() {
 *         if (reg_.valid<P>()) process(reg_.get<P>());
 *         reg_.set<P>(new_data);
 *     }
 * };
 * @endcode
 */
template <typename Derived>
class PhasedTickableUnit : public TickableUnit {
public:
    explicit PhasedTickableUnit(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override final {
        if ((localCycle() & 1) == 0) {
            static_cast<Derived*>(this)->template tickPhase<Phase0>();
        } else {
            static_cast<Derived*>(this)->template tickPhase<Phase1>();
        }
    }

protected:
    template <ValidPhase P>
    void tickPhase() {}
};

}  // namespace chronon::sender
