// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstddef>
#include <utility>

#include "PipelinePhase.hpp"

namespace chronon::sender {

/// Simple stall-aware forward for a single pipe.
/// If dst already has a write pending for this pipe, retains in src.
/// Returns true if forwarded, false if retained (stalled).
template <ValidPhase P, typename Src, typename Dst>
bool simpleForward(Src& src, Dst& dst, std::size_t pipe) {
    if (!src.template valid<P>(pipe)) return false;
    if (dst.written(pipe)) {
        src.template retain<P>(pipe);
        return false;
    }
    dst.template write<P>(pipe, src.template consume<P>(pipe));
    return true;
}

/// Forward all pipes from src to dst with stall-aware retention.
template <ValidPhase P, std::size_t N, typename Src, typename Dst>
void simpleForwardAll(Src& src, Dst& dst) {
    for (std::size_t pipe = 0; pipe < N; ++pipe) {
        simpleForward<P>(src, dst, pipe);
    }
}

/// Forward with processing: fn(pipe, data&) called on consumed data
/// before writing to dst. Returns true if forwarded, false if stalled.
template <ValidPhase P, typename Src, typename Dst, typename Fn>
bool processForward(Src& src, Dst& dst, std::size_t pipe, Fn&& fn) {
    if (!src.template valid<P>(pipe)) return false;
    if (dst.written(pipe)) {
        src.template retain<P>(pipe);
        return false;
    }
    auto data = src.template consume<P>(pipe);
    fn(pipe, data);
    dst.template write<P>(pipe, std::move(data));
    return true;
}

/// Cross-type forward: convert(pipe, src_data) returns dst_data.
/// Returns true if forwarded, false if stalled.
template <ValidPhase P, typename Src, typename Dst, typename Convert>
bool convertForward(Src& src, Dst& dst, std::size_t pipe, Convert&& convert) {
    if (!src.template valid<P>(pipe)) return false;
    if (dst.written(pipe)) {
        src.template retain<P>(pipe);
        return false;
    }
    auto src_data = src.template consume<P>(pipe);
    dst.template write<P>(pipe, convert(pipe, std::move(src_data)));
    return true;
}

/// Single-entry overload (for StageReg&lt;T, 1&gt;).
template <ValidPhase P, typename Src, typename Dst>
bool simpleForward(Src& src, Dst& dst) {
    if (!src.template valid<P>()) return false;
    if (dst.written()) {
        src.template retain<P>();
        return false;
    }
    dst.template write<P>(src.template consume<P>());
    return true;
}

/// Forward with processing (single-entry): fn(data&) called before write.
template <ValidPhase P, typename Src, typename Dst, typename Fn>
bool processForward(Src& src, Dst& dst, Fn&& fn) {
    if (!src.template valid<P>()) return false;
    if (dst.written()) {
        src.template retain<P>();
        return false;
    }
    auto data = src.template consume<P>();
    fn(data);
    dst.template write<P>(std::move(data));
    return true;
}

/// Cross-type forward (single-entry): convert(src_data) returns dst_data.
template <ValidPhase P, typename Src, typename Dst, typename Convert>
bool convertForward(Src& src, Dst& dst, Convert&& convert) {
    if (!src.template valid<P>()) return false;
    if (dst.written()) {
        src.template retain<P>();
        return false;
    }
    dst.template write<P>(convert(src.template consume<P>()));
    return true;
}

}  // namespace chronon::sender
