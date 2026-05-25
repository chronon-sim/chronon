// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <tuple>

#include "PipelinePhase.hpp"

namespace chronon::sender {

/** @brief Groups heterogeneous StageReg / SingleStageReg instances for batch lifecycle calls. */
template <typename... Stages>
class StagePipeline {
    std::tuple<Stages*...> stages_;

public:
    explicit StagePipeline(Stages&... s) : stages_{&s...} {}

    template <ValidPhase P>
    void beginCycle() {
        std::apply([](auto*... s) { (s->template beginCycle<P>(), ...); }, stages_);
    }

    template <ValidPhase P, typename Pred>
    void flushIf(Pred&& pred) {
        std::apply([&](auto*... s) { (s->template flushIf<P>(pred), ...); }, stages_);
    }

    /// Flush across read and write slots — for mid-tick flushes where upstream has already written.
    template <ValidPhase P, typename Pred>
    void flushIfAnySlot(Pred&& pred) {
        std::apply([&](auto*... s) { (s->template flushIfAnySlot<P>(pred), ...); }, stages_);
    }

    void reset() {
        std::apply([](auto*... s) { (s->reset(), ...); }, stages_);
    }
};

}  // namespace chronon::sender
