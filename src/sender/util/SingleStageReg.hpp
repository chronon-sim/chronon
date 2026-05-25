// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cassert>
#include <cstddef>
#include <utility>

#include "PipelinePhase.hpp"

namespace chronon::sender {

/** @brief Single-entry pipeline register with ping-pong slots and runtime write tracking. */
template <typename T>
class SingleStageReg {
    struct Slot {
        T data{};
        bool valid = false;
    };

    Slot slots_[2];
    bool written_ = false;

public:
    SingleStageReg() = default;
    SingleStageReg(SingleStageReg&&) = default;
    SingleStageReg& operator=(SingleStageReg&&) = default;
    SingleStageReg(const SingleStageReg&) = delete;
    SingleStageReg& operator=(const SingleStageReg&) = delete;

    template <ValidPhase P>
    void beginCycle() {
        constexpr std::size_t widx = write_slot_index<P>();
        slots_[widx].valid = false;
        written_ = false;
    }

    template <ValidPhase P>
    bool valid() const {
        constexpr std::size_t ridx = read_slot_index<P>();
        return slots_[ridx].valid;
    }

    template <ValidPhase P>
    const T& read() const {
        constexpr std::size_t ridx = read_slot_index<P>();
        return slots_[ridx].data;
    }

    template <ValidPhase P>
    T& read() {
        constexpr std::size_t ridx = read_slot_index<P>();
        return slots_[ridx].data;
    }

    template <ValidPhase P>
    void write(T data) {
        assert(!written_ && "SingleStageReg: double-write");
        constexpr std::size_t widx = write_slot_index<P>();
        slots_[widx].data = std::move(data);
        slots_[widx].valid = true;
        written_ = true;
    }

    template <ValidPhase P>
    void retain() {
        assert(!written_ && "SingleStageReg: retain conflicts with prior write");
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        if (slots_[ridx].valid) {
            slots_[widx] = slots_[ridx];
        }
        written_ = true;
    }

    template <ValidPhase P>
    T consume() {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        T data = std::move(slots_[ridx].data);
        slots_[ridx].valid = false;
        slots_[widx].valid = false;
        return data;
    }

    bool written() const { return written_; }

    template <ValidPhase P, typename Func>
    std::size_t forEachValidConsume(Func&& fn) {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        bool was_valid = slots_[ridx].valid;
        if (was_valid) {
            fn(slots_[ridx].data);
        }
        slots_[ridx].valid = false;
        slots_[widx].valid = false;
        return was_valid ? 1 : 0;
    }

    template <ValidPhase P, typename Func>
    bool ifValidConsume(Func&& fn) {
        return forEachValidConsume<P>(std::forward<Func>(fn)) != 0;
    }

    void reset() {
        slots_[0].valid = false;
        slots_[1].valid = false;
        written_ = false;
    }
};

}  // namespace chronon::sender
