// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <utility>

#include "PipelinePhase.hpp"

namespace chronon::sender {

/**
 * @brief N-pipe pipeline register with ping-pong slots and runtime write tracking.
 *
 * When NumPipes == 1, convenience overloads without a pipe index are available,
 * making this a drop-in replacement for the former SingleStageReg.
 */
template <typename T, std::size_t NumPipes = 1>
class StageReg {
    struct Slot {
        T data{};
        bool valid = false;
    };

    std::array<Slot, NumPipes> slots_[2];
    std::array<bool, NumPipes> written_{};

public:
    StageReg() = default;
    StageReg(StageReg&&) = default;
    StageReg& operator=(StageReg&&) = default;
    StageReg(const StageReg&) = delete;
    StageReg& operator=(const StageReg&) = delete;

    /// Clear all write slots and reset per-pipe write tracking.
    template <ValidPhase P>
    void beginCycle() {
        constexpr std::size_t widx = write_slot_index<P>();
        for (std::size_t i = 0; i < NumPipes; ++i) {
            slots_[widx][i].valid = false;
        }
        written_.fill(false);
    }

    template <ValidPhase P>
    bool valid(std::size_t pipe) const {
        constexpr std::size_t ridx = read_slot_index<P>();
        return slots_[ridx][pipe].valid;
    }

    template <ValidPhase P>
    const T& read(std::size_t pipe) const {
        constexpr std::size_t ridx = read_slot_index<P>();
        return slots_[ridx][pipe].data;
    }

    template <ValidPhase P>
    T& read(std::size_t pipe) {
        constexpr std::size_t ridx = read_slot_index<P>();
        return slots_[ridx][pipe].data;
    }

    /// Asserts no prior write this cycle.
    template <ValidPhase P>
    void write(std::size_t pipe, T data) {
        assert(!written_[pipe] && "StageReg: double-write to same pipe");
        constexpr std::size_t widx = write_slot_index<P>();
        slots_[widx][pipe].data = std::move(data);
        slots_[widx][pipe].valid = true;
        written_[pipe] = true;
    }

    /// Persist current read data into write slot across phase swap.
    template <ValidPhase P>
    void retain(std::size_t pipe) {
        assert(!written_[pipe] && "StageReg: retain conflicts with prior write");
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        if (slots_[ridx][pipe].valid) {
            slots_[widx][pipe] = slots_[ridx][pipe];
        }
        written_[pipe] = true;
    }

    /// Move data out of read slot — does NOT set written_.
    template <ValidPhase P>
    T consume(std::size_t pipe) {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        T data = std::move(slots_[ridx][pipe].data);
        slots_[ridx][pipe].valid = false;
        slots_[widx][pipe].valid = false;
        return data;
    }

    bool written(std::size_t pipe) const { return written_[pipe]; }

    template <ValidPhase P, typename Func>
    std::size_t forEachValidConsume(Func&& fn)
        requires(NumPipes > 1)
    {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        std::size_t count = 0;
        for (std::size_t i = 0; i < NumPipes; ++i) {
            if (slots_[ridx][i].valid) {
                fn(i, slots_[ridx][i].data);
                ++count;
            }
            slots_[ridx][i].valid = false;
            slots_[widx][i].valid = false;
        }
        return count;
    }

    template <ValidPhase P, typename Func>
    std::size_t forEachValidConsume(Func&& fn)
        requires(NumPipes == 1)
    {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        bool was_valid = slots_[ridx][0].valid;
        if (was_valid) {
            fn(slots_[ridx][0].data);
        }
        slots_[ridx][0].valid = false;
        slots_[widx][0].valid = false;
        return was_valid ? 1 : 0;
    }

    /**
     * @brief Flush pipes whose read slot matches @p pred; invalidates both slots.
     *
     * Why read-only: when earlier-running stages have already written new correct-path
     * entries into write slots, those new entries share the same monotonic identifier
     * space as the old-path entries being flushed. The predicate cannot distinguish
     * them, so checking the write slot would kill the new entries. Use
     * flushIfAnySlot only when write-slot entries are guaranteed to be old-path.
     */
    template <ValidPhase P, typename Pred>
    std::size_t flushIf(Pred&& pred)
        requires(NumPipes > 1)
    {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        std::size_t flushed = 0;
        for (std::size_t i = 0; i < NumPipes; ++i) {
            if (slots_[ridx][i].valid && pred(i, slots_[ridx][i].data)) {
                slots_[ridx][i].valid = false;
                slots_[widx][i].valid = false;
                ++flushed;
            }
        }
        return flushed;
    }

    template <ValidPhase P, typename Pred>
    std::size_t flushIf(Pred&& pred)
        requires(NumPipes == 1)
    {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        if (slots_[ridx][0].valid && pred(slots_[ridx][0].data)) {
            slots_[ridx][0].valid = false;
            slots_[widx][0].valid = false;
            return 1;
        }
        return 0;
    }

    /**
     * @brief Flush pipes matching @p pred in either read or write slot.
     *
     * @warning Caller must guarantee write-slot entries are from the old path. Otherwise
     * monotonic IDs cause new-path data to match and be killed — use flushIf instead.
     */
    template <ValidPhase P, typename Pred>
    std::size_t flushIfAnySlot(Pred&& pred)
        requires(NumPipes > 1)
    {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        std::size_t flushed = 0;
        for (std::size_t i = 0; i < NumPipes; ++i) {
            bool match_read = slots_[ridx][i].valid && pred(i, slots_[ridx][i].data);
            bool match_write = slots_[widx][i].valid && pred(i, slots_[widx][i].data);
            if (match_read || match_write) {
                slots_[ridx][i].valid = false;
                slots_[widx][i].valid = false;
                ++flushed;
            }
        }
        return flushed;
    }

    template <ValidPhase P, typename Pred>
    std::size_t flushIfAnySlot(Pred&& pred)
        requires(NumPipes == 1)
    {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        bool match_read = slots_[ridx][0].valid && pred(slots_[ridx][0].data);
        bool match_write = slots_[widx][0].valid && pred(slots_[widx][0].data);
        if (match_read || match_write) {
            slots_[ridx][0].valid = false;
            slots_[widx][0].valid = false;
            return 1;
        }
        return 0;
    }

    void reset() {
        for (std::size_t i = 0; i < NumPipes; ++i) {
            slots_[0][i].valid = false;
            slots_[1][i].valid = false;
        }
        written_.fill(false);
    }

    static constexpr std::size_t size() { return NumPipes; }

    template <ValidPhase P>
    bool hasWritePending(std::size_t pipe) const {
        constexpr std::size_t widx = write_slot_index<P>();
        return slots_[widx][pipe].valid;
    }

    // --- Single-pipe convenience API (available when NumPipes == 1) ---

    template <ValidPhase P>
    bool valid() const
        requires(NumPipes == 1)
    {
        return valid<P>(0);
    }

    template <ValidPhase P>
    const T& read() const
        requires(NumPipes == 1)
    {
        return read<P>(0);
    }

    template <ValidPhase P>
    T& read()
        requires(NumPipes == 1)
    {
        return read<P>(0);
    }

    template <ValidPhase P>
    void write(T data)
        requires(NumPipes == 1)
    {
        write<P>(0, std::move(data));
    }

    template <ValidPhase P>
    void retain()
        requires(NumPipes == 1)
    {
        retain<P>(0);
    }

    template <ValidPhase P>
    T consume()
        requires(NumPipes == 1)
    {
        return consume<P>(0);
    }

    bool written() const
        requires(NumPipes == 1)
    {
        return written_[0];
    }

    template <ValidPhase P>
    bool hasWritePending() const
        requires(NumPipes == 1)
    {
        return hasWritePending<P>(0);
    }

    template <ValidPhase P, typename Func>
    bool ifValidConsume(Func&& fn)
        requires(NumPipes == 1)
    {
        constexpr std::size_t ridx = read_slot_index<P>();
        constexpr std::size_t widx = write_slot_index<P>();
        bool was_valid = slots_[ridx][0].valid;
        if (was_valid) {
            fn(slots_[ridx][0].data);
        }
        slots_[ridx][0].valid = false;
        slots_[widx][0].valid = false;
        return was_valid;
    }
};

template <typename T>
using SingleStageReg = StageReg<T, 1>;

}  // namespace chronon::sender
