// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

namespace chronon::sender::detail {

/**
 * Producer-owned cycle-strict admission bookkeeping for one Connection.
 *
 * The carried occupancy is a conservative upper bound: publications can
 * increase it, while receiver pops, flushes, and cancellations only lower the
 * true occupancy. Near capacity, Connection refreshes the exact queue view.
 * No field is shared between producer threads and no synchronization is
 * required.
 */
class ConnectionAdmissionState {
public:
    void beginCycle(uint64_t cycle) noexcept {
        if (cycle == push_cycle_) return;

        if (upper_bound_valid_) {
            const size_t published = publishedPushes_();
            const size_t max = std::numeric_limits<size_t>::max();
            occupancy_upper_bound_ =
                published > max - occupancy_upper_bound_ ? max : occupancy_upper_bound_ + published;
        }

        pushes_ = 0;
        bounded_reservation_pending_ = false;
        push_cycle_ = cycle;
        snapshot_valid_ = false;
    }

    void invalidateOccupancy() noexcept {
        snapshot_valid_ = false;
        upper_bound_valid_ = false;
    }

    [[nodiscard]] size_t pushes() const noexcept { return pushes_; }
    [[nodiscard]] uint64_t pushCycle() const noexcept { return push_cycle_; }

    void chargePush() noexcept { ++pushes_; }

    void reservePush(bool bounded_destination) noexcept {
        ++pushes_;
        bounded_reservation_pending_ = bounded_destination;
    }

    void releaseReservation(uint64_t cycle) noexcept {
        if (push_cycle_ == cycle && pushes_ != 0) {
            --pushes_;
        }
        bounded_reservation_pending_ = false;
    }

    void commitReservation() noexcept { bounded_reservation_pending_ = false; }

    [[nodiscard]] bool upperBoundHasSlot(size_t capacity) const noexcept {
        if (!upper_bound_valid_) return false;
        return occupancy_upper_bound_ < capacity && pushes_ < capacity - occupancy_upper_bound_;
    }

    [[nodiscard]] bool hasSnapshot(uint64_t cycle) const noexcept {
        return snapshot_valid_ && snapshot_cycle_ == cycle;
    }

    void cacheOccupancy(uint64_t cycle, size_t occupancy) noexcept {
        // A carried upper bound can defer this exact read until after current-
        // cycle publications are visible. Remove those once to recover the
        // eager algorithm's cycle-start snapshot; a bounded transaction's one
        // unpublished reservation is deliberately excluded.
        const size_t published = publishedPushes_();
        occupancy_upper_bound_ = occupancy > published ? occupancy - published : 0;
        snapshot_cycle_ = cycle;
        snapshot_valid_ = true;
        upper_bound_valid_ = true;
    }

    [[nodiscard]] size_t occupancy() const noexcept { return occupancy_upper_bound_; }

private:
    [[nodiscard]] size_t publishedPushes_() const noexcept {
        // Bounded destinations serialize transaction claims per destination,
        // so at most one charged push is not yet visible in this Connection's
        // single-writer queue or lane.
        return pushes_ - static_cast<size_t>(bounded_reservation_pending_);
    }

    size_t pushes_ = 0;
    uint64_t push_cycle_ = 0;
    bool snapshot_valid_ = false;
    bool upper_bound_valid_ = false;
    bool bounded_reservation_pending_ = false;
    uint64_t snapshot_cycle_ = 0;
    size_t occupancy_upper_bound_ = 0;
};

}  // namespace chronon::sender::detail
