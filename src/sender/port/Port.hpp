// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../core/Fwd.hpp"
#include "Connection.hpp"
#include "MessageQueue.hpp"
#include "PortDirectory.hpp"

namespace chronon::sender {

/**
 * StageSelective cancellation tracing (CHRONON_STAGE_TRACE env).
 *
 * Permanent diagnostic feature. Emits one line per install/retire/
 * shouldCancel on a StageSelective InPort, for investigating flush
 * and cancellation behavior in production builds.
 *
 * Enable: CHRONON_STAGE_TRACE=1 (or any non-empty value)
 * Optional filter: CHRONON_STAGE_TRACE_PORT=<substring of port name>
 */
inline bool stageTraceEnabled_() noexcept {
    static const bool enabled = []() {
        const char* v = std::getenv("CHRONON_STAGE_TRACE");
        return v != nullptr && v[0] != '\0' && v[0] != '0';
    }();
    return enabled;
}
inline bool stageTracePortMatches_(const std::string& name) noexcept {
    static const char* filter = std::getenv("CHRONON_STAGE_TRACE_PORT");
    if (!filter || filter[0] == '\0') {
        return true;
    }
    return name.find(filter) != std::string::npos;
}

/**
 * PortPolicy - Selects InPort cancellation/dispatch behavior.
 *
 * - LegacyFastPath (default): legacy behavior. cancelYoungerThan/cancelOlderThan
 *   use the receiver-side generation/min-key/max-key bound mechanism. The
 *   sender path (enqueueStored_, pushFromThread) early-rejects messages that
 *   already look canceled. This is the backward-compatible default. Carries
 *   the known #7 overlapping-flush gap and #8 generation race in parallel
 *   mode; new code that needs strong cancellation guarantees should use
 *   StageSelective.
 *
 * - StageSelective: optimized for delay=1 stage-register ports with selective
 *   cancellation. Each message carries `enqueue_cycle` (producer's localCycle
 *   at push time). cancelYoungerThan(key) installs a StagePredicate
 *   {flush_cycle = receiver localCycle, max_keep = key}; predicates are
 *   stored in a small fixed-size slot array and retired when the receiver
 *   advances past flush_cycle. The sender side does NOT consult receiver
 *   state — the filter is applied receiver-side only on pop. This eliminates
 *   the cross-thread receiver-atomic read that races in parallel mode (#8)
 *   and the overlapping-flush zombie escape (#7).
 */
enum class PortPolicy : uint8_t {
    LegacyFastPath = 0,
    General [[deprecated("Use LegacyFastPath")]] = LegacyFastPath,  ///< Backward-compatible alias.
    StageSelective = 1,
};

namespace detail {

template <typename T>
struct PortEnvelope {
    T data;
    const std::atomic<uint64_t>* cancel_epoch = nullptr;  ///< nullptr => not cancelable
    uint64_t epoch_snapshot = 0;
    uint64_t receiver_generation_snapshot = 0;
    /// Producer's localCycle at push time (arrive_cycle - delay). Used by
    /// StageSelective predicates to decide whether a message was in-flight
    /// at the time of a flush.
    uint64_t enqueue_cycle = 0;
    /// Stable producer tiebreaker for mutex-backed multi-producer queues.
    /// Connection::conn_id is deterministic for a fixed topology, so same-cycle
    /// fan-in does not depend on wall-clock mutex acquisition order.
    uint32_t sender_id = 0;
};

template <typename T>
[[nodiscard]] inline bool isCanceled(const PortEnvelope<T>& msg) noexcept {
    if (msg.cancel_epoch &&
        msg.cancel_epoch->load(std::memory_order_acquire) != msg.epoch_snapshot) {
        return true;
    }
    return false;
}

/**
 * StagePredicate - A single live "cancel everything younger than max_keep that
 * was enqueued before flush_cycle" rule.
 */
struct StagePredicate {
    uint64_t flush_cycle = 0;
    uint64_t max_keep = 0;
};

/**
 * InPortStageCancelState - Holds the active StagePredicates for a
 * StageSelective InPort.
 *
 * Retirement is pop-driven (not time-driven): shouldCancel() retires a slot
 * in place as soon as a pop arrives whose enqueue_cycle is >= the slot's
 * flush_cycle. Since enqueue_cycle is monotonically non-decreasing on a
 * port (SPSC order; MPSC k-way-merge on arrive_cycle), that first
 * post-flush pop proves no later message can match the predicate either,
 * so the slot is dead and safe to reclaim. This makes the predicate
 * lifetime a function of actual pop traffic rather than wall-clock cycles,
 * which matches RTL: a DFF-backed skid buffer keeps its flow-control
 * commitment until traffic actually moves through it, not at some arbitrary
 * "next clock edge."
 *
 * The ring holds a few slots so overlapping flushes (issue #7) each get
 * their own predicate. install() merges same-cycle flushes into one slot.
 */
struct InPortStageCancelState {
    /// Live predicates. Elastic so flush storms can't overflow a fixed ring.
    /// Each entry is 16 bytes; typical depth is single-digit, worst case
    /// observed is ~16 (comprehensive_test section 53 BTB-miss burst).
    /// shouldCancel retires slots pop-driven: a single post-flush pop drops
    /// all obsolete slots in one pass.
    std::vector<StagePredicate> slots;
    /// High-water mark tracking for diagnostics (CHRONON_STAGE_TRACE).
    size_t high_water = 0;

    void install(uint64_t flush_cycle, uint64_t max_keep) {
        // Merge same-cycle installs — a single flush broadcast fans out to
        // multiple cancel calls per tick; collapse them into one slot and
        // take the stricter max_keep.
        for (auto& p : slots) {
            if (p.flush_cycle == flush_cycle) {
                if (max_keep < p.max_keep) {
                    p.max_keep = max_keep;
                }
                return;
            }
        }
        slots.push_back({flush_cycle, max_keep});
        if (slots.size() > high_water) {
            high_water = slots.size();
        }
    }

    void clear() noexcept { slots.clear(); }

    /// Non-const because we retire obsolete slots in place. Safe to mutate
    /// because stage_state_ is touched only from the receiver thread.
    [[nodiscard]] bool shouldCancel(uint64_t enqueue_cycle, uint64_t key) noexcept {
        bool cancel = false;
        size_t i = 0;
        while (i < slots.size()) {
            const auto& p = slots[i];
            if (enqueue_cycle >= p.flush_cycle) {
                // Post-flush pop. enqueue_cycle is monotone on this port,
                // so no later message can match this predicate; retire.
                slots[i] = slots.back();
                slots.pop_back();
                continue;
            }
            if (key > p.max_keep) {
                cancel = true;
            }
            ++i;
        }
        return cancel;
    }

    [[nodiscard]] size_t active_slot_count() const noexcept { return slots.size(); }
};

}  // namespace detail

template <typename T>
class Connection;
class Unit;

/// Defined in Unit.hpp; declared here so port constructors can register
/// auto-registration callbacks without including Unit.hpp.
void addPortRegistrationToUnit(Unit* unit, std::function<void(const std::string&)> registration);
class PortBase;
void recordPortOnOwnerUnit(Unit* unit, PortBase* port);

/**
 * IArbitratablePort - Type-erased interface exposing the per-cycle MPSC
 * arbitration hook to the TickSimulation scheduler.
 *
 * Only InPorts that have at least one MPSC connection registered implement
 * meaningful behavior; other ports can ignore. TickSimulation keeps a flat
 * list of ports that declared MPSC interest during initialize() and calls
 * arbitrateMPSC() on each at every cycle boundary (sequential tick loop,
 * executeEpochBarrier sync_wait, or the progress-based lookahead scheduler's
 * epoch-end flush).
 */
class IArbitratablePort {
public:
    virtual ~IArbitratablePort() = default;

    /**
     * Drain every staged entry whose epoch matches. Called at scheduler
     * sync points (Sequential per-cycle, Barrier sync_wait, lookahead
     * epoch-end flush) where every producer has finished its cycle.
     */
    virtual void arbitrateMPSC() = 0;

    /**
     * Cycle-bounded arbitration for consumer-tick-driven execution
     * (see docs/mpsc-atomic-publish.md). The owning InPort computes
     *   S = min over producer clusters of completed_cycle.load(acquire)
     * and drains only entries with enqueue_cycle <= S, in conn_id order.
     * A no-op if the port has no MPSC connections or if S has not
     * advanced since the last arbitration.
     *
     * Default implementation calls arbitrateMPSC() — safe fallback for
     * ports that don't need the bounded variant.
     */
    virtual void arbitrateMPSCConsumerDriven() noexcept { arbitrateMPSC(); }

    /**
     * Opaque identity for this arbitrable port. Returned by InPort as its
     * `this` pointer (same value as destPortPtr() on connections). Used
     * by TickSimulation to join MPSC InPorts against per-port producer-
     * thread tables at init time.
     */
    virtual void* arbitratablePortKey() noexcept = 0;

    /**
     * Install the predecessor-thread completed_cycle atomics for
     * consumer-tick-driven arbitration. Called once during
     * TickSimulation::initialize() for the lookahead scheduler. Ignored
     * if empty or if the port has no MPSC connections.
     */
    virtual void setArbitrationProgressPointers(std::vector<const std::atomic<uint64_t>*> ptrs) = 0;

    /**
     * Install PER-CONNECTION producer completed_cycle atomics for
     * consumer-tick-driven arbitration. `src_progress` maps each producer
     * Unit* to its cluster's completed_cycle atomic. The InPort resolves one
     * atomic per MPSC connection (by connection source), enabling each
     * connection to be drained up to its OWN producer's progress rather than
     * the min across producers — required for correctness under heterogeneous
     * edge delays (a low-delay producer's message must not be held back by a
     * lagging high-delay producer on the same InPort). Default: no-op.
     */
    virtual void setArbitrationConnProgress(
        const std::unordered_map<Unit*, const std::atomic<uint64_t>*>& /*src_progress*/) {}

    /**
     * True iff every MPSC connection on this port has a resolved per-connection
     * producer progress atomic, i.e. the heterogeneous-delay-correct
     * consumer-driven drain in arbitrateMPSCConsumerDriven() fully covers this
     * port. When false, at least one connection relies on the central per-epoch
     * arbitrateMPSC() flush to deliver its tail (an unresolved producer is
     * skipped by the consumer-driven path), so the epoch-free scheduler — which
     * has no per-epoch flush — must NOT be used. Conservative default: false, so
     * unknown port types veto epoch-free execution. InPort overrides.
     */
    virtual bool mpscConnProgressFullyResolved() const noexcept { return false; }

    /**
     * Number of staged pushes dropped because the physical MPSC ring was full.
     * Nonzero indicates the lookahead window outran the ring capacity — a
     * correctness failure. Surfaced for the epoch-free A/B watchdog. Default 0
     * for ports with no multi-producer staging.
     */
    virtual uint64_t stagingOverflowEvents() const noexcept { return 0; }
};

/**
 * PortBase - Type-erased base class for all ports.
 */
class PortBase {
public:
    virtual ~PortBase() = default;

    const std::string& name() const noexcept { return name_; }
    Unit* owner() const noexcept { return owner_; }

    /**
     * Consumer-tick-driven MPSC arbitration hook
     * (Option 1, see docs/mpsc-atomic-publish.md). Called at the start of
     * the owning receiver unit's tick(). Default no-op; InPort overrides
     * to drain staging for ports with registered MPSC connections.
     * OutPorts always no-op.
     */
    virtual void arbitrateMPSCConsumerDriven() noexcept {}

    /// True if this port owns any MPSC staging queues that need consumer-side drains.
    virtual bool hasMPSCConnections() const noexcept { return false; }

    /// Earliest pending arrival visible through this port, if any.
    virtual std::optional<uint64_t> minArrivalCycle() const { return std::nullopt; }

protected:
    PortBase(Unit* owner, std::string name) : owner_(owner), name_(std::move(name)) {
        recordPortOnOwnerUnit(owner_, this);
    }

    Unit* owner_;
    std::string name_;
};

}  // namespace chronon::sender

#include "InPort.hpp"
#include "OutPort.hpp"
