// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <queue>
#include <span>
#include <vector>

#include "../../time/ClockDomain.hpp"

namespace chronon::sender {

struct ClockEdge {
    const ClockDomain* domain;
    uint64_t cycle;
    SimTime time;
    size_t calendar_index = 0;  // Dense calendar-local metadata, independent of hardware ID.
};

/// Calendar over real edges, never over the LCM's fine-grained time lattice.
class ClockCalendar {
public:
    explicit ClockCalendar(std::span<const ClockDomain* const> clocks) {
        batch_.reserve(clocks.size());
        for (size_t i = 0; i < clocks.size(); ++i)
            heap_.push({clocks[i], 0, clocks[i]->edge(0), i});
    }
    bool empty() const noexcept { return heap_.empty(); }
    SimTime nextTime() const {
        if (empty()) throw std::logic_error("empty clock calendar");
        return heap_.top().time;
    }
    /// A batch contains every domain at this physical time, ordered by stable ID.
    std::span<const ClockEdge> pop() {
        batch_.clear();
        if (heap_.empty()) return batch_;
        const auto time = heap_.top().time;
        do {
            auto edge = heap_.top();
            // Check the successor before changing the calendar or evaluating hardware.
            if (edge.cycle == UINT64_MAX) throw std::overflow_error("clock edge index overflow");
            auto successor = ClockEdge{edge.domain, edge.cycle + 1,
                                       edge.domain->edge(edge.cycle + 1), edge.calendar_index};
            heap_.pop();
            heap_.push(successor);
            batch_.push_back(edge);
        } while (!heap_.empty() && heap_.top().time == time);
        return batch_;
    }

private:
    struct Later {
        bool operator()(const ClockEdge& a, const ClockEdge& b) const noexcept {
            return a.time == b.time ? a.domain->id() > b.domain->id() : a.time > b.time;
        }
    };
    std::priority_queue<ClockEdge, std::vector<ClockEdge>, Later> heap_;
    std::vector<ClockEdge> batch_;
};

}  // namespace chronon::sender
