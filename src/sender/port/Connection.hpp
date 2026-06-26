// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "MessageQueue.hpp"

namespace chronon::sender {

template <typename T>
class InPort;
template <typename T>
class OutPort;
class Unit;
class IArbitratablePort;
void wakeUnitAt(Unit* unit, uint64_t cycle);

/**
 * ConnectionBase - Type-erased base class for connections.
 *
 * Enables storing heterogeneous connections in containers.
 */
class ConnectionBase {
public:
    virtual ~ConnectionBase() = default;

    virtual uint32_t delay() const noexcept = 0;
    virtual Unit* source() const noexcept = 0;
    virtual Unit* destination() const noexcept = 0;
    virtual void* destPortPtr() const noexcept = 0;

    /**
     * Optimize destination port for same-thread access.
     *
     * Switches InPort to use lock-free SingleThreadMessageQueue.
     * Call this during initialization when both source and destination
     * are determined to be on the same thread (same cluster).
     *
     * This eliminates mutex overhead (~18% of execution time) for
     * intra-cluster connections.
     */
    virtual void optimizeForSameThread() = 0;

    /**
     * Optimize destination port for cross-thread SPSC access.
     *
     * Switches InPort to use lock-free LockFreeMessageQueue.
     * Call this during initialization when there is exactly ONE source thread
     * writing to the destination port on a different thread.
     */
    virtual void optimizeForSPSC() = 0;

    /**
     * Optimize destination port for cross-thread MPSC access.
     *
     * Switches InPort to use MultiProducerQueueAdapter with per-thread
     * producer queues.
     */
    virtual void optimizeForMPSC() = 0;

    /**
     * Register a producer thread for MPSC mode.
     *
     * @param thread_id Source thread identifier
     * @return Queue ID for this producer thread, or SIZE_MAX on failure
     */
    virtual size_t registerProducerThread(size_t thread_id) = 0;

    /**
     * Set the thread queue ID for multi-producer mode.
     *
     * Called during initialization when the destination InPort is in
     * multi-producer mode (multiple source threads writing to it).
     * All connections from the same source thread share the same queue_id.
     *
     * @param queue_id The queue ID for this connection's source thread
     */
    virtual void setThreadQueueId(size_t queue_id) = 0;

    /// True if this connection uses thread-specific queue (MPSC mode).
    virtual bool hasThreadQueueId() const noexcept = 0;

    /**
     * Stable connection identifier assigned at simulation build time.
     *
     * Equal to this connection's index in TickSimulation::connections_.
     * Used by MultiProducerQueueAdapter as a cross-num_workers-stable
     * tiebreaker in the k-way merge, replacing the partition-dependent
     * queue_id. The value is deterministic given a fixed topology,
     * regardless of thread count / cluster assignment.
     */
    virtual void setConnId(uint32_t conn_id) noexcept = 0;
    virtual uint32_t connId() const noexcept = 0;

    /**
     * Cancel all in-flight messages on this connection.
     *
     * Advances the cancellation epoch so pending messages are dropped
     * when the receiver tries to consume them.
     */
    virtual void cancelInFlight() noexcept = 0;

    /**
     * Max producer run-ahead (in cycles) this connection's cross-thread buffer
     * (MPSC staging ring or SPSC lock-free ring) can absorb while the epoch-free
     * path keeps results identical to the reference: roughly
     * `threshold / rate - delay`, where `threshold` is the entry count at which
     * transfer() stops accepting (the ring, or the InPort capacity if smaller),
     * `rate` is the source's per-cycle send cap, and `delay` accounts for
     * not-yet-due entries the consumer cannot drain. Returns SIZE_MAX for
     * connections with no bounded cross-thread ring (same-thread / unbounded) and
     * 0 when no finite run-ahead is provably safe (uncapped source rate). Used to
     * gate the epoch-free lookahead path, which removes the per-epoch drain.
     */
    virtual size_t crossThreadHeadroom() const noexcept { return SIZE_MAX; }

    /**
     * Type-erased MPSC admission helper. Called by the destination InPort's
     * arbiter at cycle boundary. Pops up to `budget` entries from the
     * per-connection staging ring and forwards them into the shared queue
     * in FIFO order. Returns the number of entries admitted. Stops on the
     * first forwarding failure (physical ring full, extremely rare).
     *
     * Only the MPSC path uses staging; non-MPSC connections return 0.
     */
    virtual size_t arbitrateAdmitErased(size_t budget) = 0;

    /**
     * Cycle-bounded MPSC admission. Drains up to `budget` entries whose
     * enqueue_cycle <= max_send_cycle. Used by consumer-tick-driven
     * arbitration under the lookahead scheduler: the consumer passes
     * S = min(predecessor-thread completed_cycle) so that only entries
     * every producer has already published are admitted. See
     * docs/mpsc-atomic-publish.md.
     */
    virtual size_t arbitrateAdmitBoundedErased(size_t budget, uint64_t max_send_cycle) = 0;

    /// Earliest arrival currently staged on this MPSC connection, if any.
    virtual std::optional<uint64_t> minStagedArrivalCycle() const { return std::nullopt; }

    /**
     * Register this MPSC connection on its destination InPort and return
     * the InPort's type-erased IArbitratablePort interface so the
     * TickSimulation can drive cycle-boundary arbitration without ever
     * knowing the Connection's message type. Returns nullptr when the
     * connection is not in MPSC mode.
     */
    virtual class IArbitratablePort* registerOnDestMPSC() = 0;

    /// True if this is a zero-delay (tight) connection.
    bool isTight() const noexcept { return delay() == 0; }
};

/**
 * Connection - Typed connection between OutPort and InPort.
 *
 * Connections specify the communication delay:
 * - delay=0: Tight coupling, same-cycle delivery on acyclic paths
 * - delay>0: Loose coupling, future delivery (lookahead possible)
 *
 * Usage:
 *   auto conn = sim.connect(producer->out, consumer->in, 5);
 *   // Messages sent at cycle N arrive at cycle N+5
 */
template <typename T>
class Connection : public ConnectionBase {
public:
    /**
     * Create a connection with specified delay.
     *
     * @param from Source output port
     * @param to Destination input port
     * @param delay Number of cycles for message delivery
     */
    Connection(OutPort<T>* from, InPort<T>* to, uint32_t delay)
        : from_(from), to_(to), delay_(delay) {}

    /**
     * Transfer data through the connection.
     *
     * The data will arrive at the destination port after the configured delay.
     * Uses thread-specific queue if in multi-producer mode.
     *
     * @param data The data to transfer
     * @param send_cycle The current cycle when sending
     * @return true if transfer succeeded, false if destination full (back pressure)
     */
    bool transfer(T data, uint64_t send_cycle) {
        uint64_t epoch_snapshot = cancel_epoch_.load(std::memory_order_acquire);
        const uint64_t arrive_cycle = send_cycle + delay_;
        maybeResetPushesCycle_(send_cycle);
        // enqueue_cycle = sender's localCycle at push time. For StageSelective
        // predicates this is the basis of the "was this in flight at flush?"
        // decision. For LegacyFastPath policy ports the field is set but ignored.
        if (thread_queue_id_ != SIZE_MAX) {
            // MPSC mode: stage for the InPort arbiter. The arbiter runs on
            // the consumer thread at the start of its next tick (under the
            // lookahead scheduler) or at scheduler sync points (Sequential /
            // Barrier), forwarding from staging into the shared per-thread
            // queue in deterministic conn_id order. See
            // docs/mpsc-atomic-publish.md.
            if (to_->capacity() != InPort<T>::UNLIMITED_CAPACITY &&
                stagingSize_() >= to_->capacity()) {
                return false;  // staging full -> producer sees back-pressure
            }
            if (!stagingTryPush_(
                    Staged{std::move(data), arrive_cycle, send_cycle, epoch_snapshot})) {
                // Ring physically full (should be rare: ring is sized >= user cap).
                return false;
            }
            ++pushes_this_cycle_;
            wakeUnitAt(destination(), arrive_cycle);
            return true;
        }
        // SPSC/SingleThread mode: enforce the per-cycle admission bound
        // (same as canTransfer()) so that callers who bypass canSend()
        // never exceed the model-side capacity. This keeps push() and
        // canAccept() agreeing on the same rate limit without reading
        // the live queue size (which races with the consumer thread).
        if (pushes_this_cycle_ >= to_->admissionCapacity()) {
            return false;
        }
        const bool ok = to_->enqueueCancelable(std::move(data), arrive_cycle, &cancel_epoch_,
                                               epoch_snapshot, send_cycle, conn_id_);
        if (ok) {
            ++pushes_this_cycle_;
            wakeUnitAt(destination(), arrive_cycle);
        }
        return ok;
    }

    /**
     * Cancel all in-flight messages previously sent on this connection.
     *
     * Only bumps the cancellation epoch. Staged entries are dropped lazily
     * by the arbiter on drain (it compares each entry's epoch_snapshot
     * against the current epoch). This matters under consumer-tick-driven
     * arbitration (Option 1): the producer thread and the consumer-thread
     * arbiter may access staging_ concurrently, so a producer-side
     * staging_.clear() would race with the arbiter's drain. See R3 in
     * docs/mpsc-atomic-publish.md.
     */
    void cancelInFlight() noexcept override {
        cancel_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }

    /**
     * Type-erased MPSC admission helper invoked by the InPort arbiter.
     *
     * Legacy unbounded variant: drains every staging entry whose epoch
     * matches the current cancel_epoch_. Called by the main-thread
     * arbiter path (Sequential per-cycle loop, Barrier sync_wait,
     * lookahead epoch-end flush).
     */
    size_t arbitrateAdmitErased(size_t budget) override {
        return arbitrateAdmitBoundedErased(budget, std::numeric_limits<uint64_t>::max());
    }

    /**
     * Cycle-bounded admission: drains every staging entry whose epoch
     * matches AND whose enqueue_cycle <= max_send_cycle. Used by the
     * consumer-tick-driven arbiter (Option 1): the consumer at its own
     * localCycle computes S = min(predecessor-thread completed_cycle)
     * and passes S here, so only entries that every producer has
     * finished writing are admitted this tick.
     */
    size_t arbitrateAdmitBoundedErased(size_t budget, uint64_t max_send_cycle) override {
        if (thread_queue_id_ == SIZE_MAX) {
            return 0;
        }
        size_t admitted = 0;
        const uint64_t cur_epoch = cancel_epoch_.load(std::memory_order_acquire);
        while (admitted < budget) {
            Staged* front = stagingPeekFront_();
            if (!front) break;  // staging empty (observed head == tail under acquire)
            if (front->enqueue_cycle > max_send_cycle) {
                // Head entry is for a cycle the slowest predecessor thread
                // hasn't completed yet. Entries behind it are strictly >=
                // this cycle (producer pushes cycles in monotonic order), so
                // stop here — they too are not yet safe to admit.
                break;
            }
            if (front->epoch_snapshot != cur_epoch) {
                // Staged before a cancelInFlight -> drop without forwarding.
                stagingPopFront_();
                continue;
            }
            if (!transferToSharedQueue_(*front)) {
                break;  // physical ring of destination is full (rare)
            }
            stagingPopFront_();
            ++admitted;
        }
        return admitted;
    }

    std::optional<uint64_t> minStagedArrivalCycle() const override {
        if (thread_queue_id_ == SIZE_MAX) {
            return std::nullopt;
        }
        const size_t head = staging_head_.load(std::memory_order_relaxed);
        if (head == staging_tail_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        return staging_buf_[head].arrive_cycle;
    }

    /// True if the destination can accept data (back-pressure preflight).
    bool canTransfer() const {
        // MPSC path: producer sees back-pressure only when its own staging
        // ring is full (bounded by the destination InPort's user_cap).
        // Admission into the shared queue is performed by the InPort arbiter
        // (either on the consumer thread at tick start or at a scheduler
        // sync point) in deterministic conn_id order, so the push path
        // itself does not race on wall-clock ordering across worker threads.
        if (thread_queue_id_ != SIZE_MAX) {
            return to_->capacity() == InPort<T>::UNLIMITED_CAPACITY ||
                   stagingSize_() < to_->capacity();
        }
        // Rate-based admission: producer may push up to effectiveCapacity_()
        // items per simulated cycle, tracked locally via pushes_this_cycle_.
        // The receiver's live occupancy is never read from the producer thread,
        // which matches a hardware pipeline register whose per-edge admission
        // is bounded by width rather than downstream FIFO depth.
        const size_t pending = currentPendingPushes_();
        return to_->canAccept(pending);
    }

    bool isDestinationFull() const { return !canTransfer(); }

    uint32_t delay() const noexcept override { return delay_; }
    Unit* source() const noexcept override;
    Unit* destination() const noexcept override;
    void* destPortPtr() const noexcept override { return static_cast<void*>(to_); }
    IArbitratablePort* registerOnDestMPSC() override;

    void optimizeForSameThread() override {
        thread_queue_id_ = SIZE_MAX;
        to_->useSingleThreadQueue();
    }

    void optimizeForSPSC() override {
        thread_queue_id_ = SIZE_MAX;
        to_->useLockFreeQueue();
    }

    void optimizeForMPSC() override {
        to_->useMultiProducerQueue();
        // Size the SPSC staging ring now that to_->capacity() is finalized.
        // The producer back-pressures on stagingSize() >= to_->capacity(),
        // so the physical ring only needs room for the user cap plus one
        // reserved slot (tail+1 == head signals full). Round up to a power
        // of 2 for fast masking. Bounded user capacities must not be capped
        // below the model-visible limit, otherwise canTransfer() and the
        // physical push path disagree for large ports.
        const size_t user_cap = to_->capacity();
        const size_t requested = (user_cap == InPort<T>::UNLIMITED_CAPACITY)
                                     ? kDefaultUnlimitedStagingRing
                                     : (user_cap + 1);
        size_t phys = 1;
        const size_t target = std::max(requested, kStagingRingMin);
        while (phys < target) {
            if (phys > (std::numeric_limits<size_t>::max() / 2)) {
                throw std::length_error("MPSC staging ring capacity is too large");
            }
            phys <<= 1;
        }
        staging_buf_.assign(phys, Staged{});
        staging_mask_ = phys - 1;
        staging_head_.store(0, std::memory_order_relaxed);
        staging_tail_.store(0, std::memory_order_relaxed);
    }

    size_t registerProducerThread(size_t thread_id) override {
        return to_->registerProducerThread(thread_id);
    }

    void setThreadQueueId(size_t queue_id) override { thread_queue_id_ = queue_id; }

    bool hasThreadQueueId() const noexcept override { return thread_queue_id_ != SIZE_MAX; }

    size_t crossThreadHeadroom() const noexcept override {
        // Identify the bounded cross-thread buffer this connection fills:
        //   MPSC (thread_queue_id_ set) -> the per-connection staging ring,
        //   SPSC (lock-free ring)       -> the InPort's lock-free queue (finite
        //                                  even for an unlimited-capacity port),
        //   same-thread / unbounded     -> no ring to overflow (SIZE_MAX).
        size_t ring_usable;
        if (thread_queue_id_ != SIZE_MAX) {
            ring_usable = staging_mask_;
        } else if (to_->usesLockFreeQueue()) {
            ring_usable = to_->capacity();
        } else {
            return SIZE_MAX;  // single-thread queue drains synchronously each tick
        }
        const size_t rate = from_->perCycleCapacity();
        // An uncapped per-cycle send rate can stage unboundedly per cycle, so no
        // finite run-ahead is provably safe.
        if (rate == OutPort<T>::UNLIMITED_CAPACITY) return 0;
        // transfer() stops accepting once buffered entries reach this many: a
        // bounded port back-pressures at to_->capacity(); an unlimited port fills
        // the ring. Past it the epoch-free path either drops (unlimited: overflow)
        // or back-pressures where the per-epoch barrier flush would not (bounded),
        // so results would diverge from the reference.
        const size_t threshold = std::min(to_->capacity(), ring_usable);
        const size_t cycles = threshold / rate;
        // The consumer drains only *due* entries (arrive_cycle <= k, i.e.
        // send_cycle <= k - delay_), so delay_ cycles of not-yet-due entries always
        // sit buffered. The supportable producer run-ahead is cycles - delay_.
        return (cycles > delay_) ? (cycles - delay_) : 0;
    }

    void setConnId(uint32_t conn_id) noexcept override { conn_id_ = conn_id; }
    uint32_t connId() const noexcept override { return conn_id_; }

    OutPort<T>* from() const noexcept { return from_; }
    InPort<T>* to() const noexcept { return to_; }

private:
    /// Per-connection staging entry. Holds the original arrive_cycle and
    /// send_cycle (as enqueue_cycle) so that even if the InPort arbiter
    /// defers admission by back-pressure, downstream predicates and
    /// delivery timing still see the *intended* values.
    struct Staged {
        T data;
        uint64_t arrive_cycle;
        uint64_t enqueue_cycle;
        uint64_t epoch_snapshot;
    };

    bool transferToSharedQueue_(Staged& entry) {
        return to_->pushToThreadQueueCancelable(
            thread_queue_id_, std::move(entry.data), entry.arrive_cycle, &cancel_epoch_,
            entry.epoch_snapshot, entry.enqueue_cycle, conn_id_);
    }

    OutPort<T>* from_;
    InPort<T>* to_;
    uint32_t delay_;
    size_t thread_queue_id_ = SIZE_MAX;  ///< SIZE_MAX means not in MPSC mode
    uint32_t conn_id_ = 0;               ///< Stable topology-based tiebreaker

    /// Per-connection staging ring (SPSC). Written exclusively by the
    /// producer thread (Connection is owned by a single OutPort which is
    /// owned by a single Unit which ticks on a single thread). Drained
    /// exclusively by the consumer-side InPort arbiter — either on the
    /// main thread at scheduler sync points (Sequential, Barrier,
    /// lookahead epoch-end flush) or on the consumer thread at the start
    /// of its own tick under the lookahead scheduler (see
    /// docs/mpsc-atomic-publish.md).
    ///
    /// The SPSC ring (std::vector<Staged> + power-of-2 mask, head/tail
    /// atomics with release-acquire ordering) is the concurrency primitive
    /// — a plain std::deque would race on node pointers when the producer
    /// appends concurrently with the arbiter drains. Sizing is chosen at
    /// optimizeForMPSC() time based on to_->capacity().
    static constexpr size_t kStagingRingMin = 16;
    static constexpr size_t kDefaultUnlimitedStagingRing = 4096;
    std::vector<Staged> staging_buf_;
    size_t staging_mask_ = 0;                          ///< buffer size - 1 (power of 2)
    alignas(64) std::atomic<size_t> staging_head_{0};  ///< consumer reads/advances
    alignas(64) std::atomic<size_t> staging_tail_{0};  ///< producer writes/advances

    size_t stagingSize_() const noexcept { return stagingRingSize_(); }

    size_t stagingRingSize_() const noexcept {
        const size_t head = staging_head_.load(std::memory_order_acquire);
        const size_t tail = staging_tail_.load(std::memory_order_acquire);
        return (tail - head) & staging_mask_;
    }

    bool stagingTryPush_(Staged&& e) { return stagingRingTryPush_(std::move(e)); }

    bool stagingRingTryPush_(Staged&& e) {
        const size_t tail = staging_tail_.load(std::memory_order_relaxed);
        const size_t next = (tail + 1) & staging_mask_;
        if (next == staging_head_.load(std::memory_order_acquire)) {
            return false;  // full (one slot reserved to distinguish full vs empty)
        }
        staging_buf_[tail] = std::move(e);
        staging_tail_.store(next, std::memory_order_release);
        return true;
    }

    Staged* stagingPeekFront_() noexcept { return stagingRingPeekFront_(); }

    Staged* stagingRingPeekFront_() noexcept {
        const size_t head = staging_head_.load(std::memory_order_relaxed);
        if (head == staging_tail_.load(std::memory_order_acquire)) {
            return nullptr;
        }
        return &staging_buf_[head];
    }

    void stagingPopFront_() noexcept { stagingRingPopFront_(); }

    void stagingRingPopFront_() noexcept {
        const size_t head = staging_head_.load(std::memory_order_relaxed);
        staging_buf_[head] = Staged{};  // release owned T early
        staging_head_.store((head + 1) & staging_mask_, std::memory_order_release);
    }

    /// Producer-side cycle-local push counter (Track H v2 / RTL-strict
    /// backpressure). Touched only by the producer thread for this
    /// connection. No atomicity needed.
    ///
    /// Reset to 0 whenever transfer() observes a new producer cycle. Read by
    /// canTransfer() to compute "snapshot + my pending pushes this cycle"
    /// — the RTL-correct view of "would my next push exceed the destination
    /// FIFO bound?" without ever consulting the destination's mid-cycle pop
    /// activity (which a parallel-thread implementation can otherwise expose
    /// and break cycle-count reproducibility across num_workers).
    mutable size_t pushes_this_cycle_ = 0;
    mutable uint64_t last_pushes_cycle_ = 0;

    void maybeResetPushesCycle_(uint64_t send_cycle) const noexcept {
        if (send_cycle != last_pushes_cycle_) {
            pushes_this_cycle_ = 0;
            last_pushes_cycle_ = send_cycle;
        }
    }
    size_t currentPendingPushes_() const noexcept {
        if (!from_) return pushes_this_cycle_;
        const uint64_t now = from_->getCurrentCycle();
        if (now != last_pushes_cycle_) {
            pushes_this_cycle_ = 0;
            last_pushes_cycle_ = now;
        }
        return pushes_this_cycle_;
    }

    /// Cancellation epoch for sender-side flush/squash. Each message is
    /// stamped with the epoch value at send time. If the epoch changes
    /// before the receiver consumes it, the message is dropped.
    std::atomic<uint64_t> cancel_epoch_{0};
};

}  // namespace chronon::sender
