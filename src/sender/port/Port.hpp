// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#ifndef CHRONON_ENABLE_OUTPORT_CANCELLATION
#define CHRONON_ENABLE_OUTPORT_CANCELLATION 1
#endif

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
 * - StageSelective: optimized for fixed-delay stage-register ports with selective
 *   cancellation. Each message carries `enqueue_cycle` (producer's localCycle
 *   at push time). cancelYoungerThan(key) installs a StagePredicate
 *   {flush_cycle = receiver localCycle, min_keep, max_keep}; predicates are
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
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    const std::atomic<uint64_t>* cancel_epoch = nullptr;  ///< nullptr => not cancelable
    uint64_t epoch_snapshot = 0;
#endif
    uint64_t receiver_generation_snapshot = 0;
    /// Producer's localCycle at push time (arrive_cycle - delay). Used by
    /// StageSelective predicates to decide whether a message was in-flight
    /// at the time of a flush.
    uint64_t enqueue_cycle = 0;
    /// Stable producer tiebreaker for topology-keyed multi-producer queues.
    /// Connection::conn_id is deterministic for a fixed topology, so same-cycle
    /// fan-in does not depend on runtime thread placement.
    uint32_t sender_id = 0;
};

template <typename T>
[[nodiscard]] inline bool isCanceled(const PortEnvelope<T>& msg) noexcept {
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    if (msg.cancel_epoch &&
        msg.cancel_epoch->load(std::memory_order_acquire) != msg.epoch_snapshot) {
        return true;
    }
#else
    (void)msg;
#endif
    return false;
}

/**
 * StagePredicate - A single live "keep only [min_keep, max_keep] among messages
 * enqueued before flush_cycle" rule.
 */
struct StagePredicate {
    uint64_t flush_cycle = 0;
    uint64_t min_keep = 0;
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

    void install(uint64_t flush_cycle, uint64_t min_keep, uint64_t max_keep) {
        // Merge same-cycle installs — a single flush broadcast fans out to
        // multiple cancel calls per tick. Collapse them into one slot and
        // intersect the keep ranges, preserving both older-than and
        // younger-than constraints without a second predicate walk.
        for (auto& p : slots) {
            if (p.flush_cycle == flush_cycle) {
                if (min_keep > p.min_keep) {
                    p.min_keep = min_keep;
                }
                if (max_keep < p.max_keep) {
                    p.max_keep = max_keep;
                }
                return;
            }
        }
        slots.push_back({flush_cycle, min_keep, max_keep});
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
            if (key < p.min_keep || key > p.max_keep) {
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
 * IMultiProducerPort - type-erased MPSC metadata interface.
 *
 * Payload arbitration disappeared with direct per-Connection SPSC lanes: the
 * receiver merges lane heads when it reads the InPort.  The scheduler retains
 * this narrow interface only to prove producer-progress coverage and surface a
 * physical-ring overflow as a correctness failure.
 */
class IMultiProducerPort {
public:
    virtual ~IMultiProducerPort() = default;

    /**
     * Install PER-CONNECTION producer completed_cycle atomics for
     * `src_progress` maps each producer Unit* to its cluster's completed-cycle
     * publication. The InPort resolves one atomic per direct lane. The
     * epoch-free scheduler requires complete coverage before allowing clusters
     * to run independently.
     */
    virtual void setProducerProgress(
        const std::unordered_map<Unit*, const std::atomic<uint64_t>*>& /*src_progress*/) {}

    /**
     * True iff every MPSC lane has a resolved producer-progress atomic. An
     * unresolved lane vetoes epoch-free execution. Conservative default: false.
     */
    virtual bool producerProgressFullyResolved() const noexcept { return false; }

    void setArbitrationConnProgress(
        const std::unordered_map<Unit*, const std::atomic<uint64_t>*>& src_progress) {
        setProducerProgress(src_progress);
    }
    bool mpscConnProgressFullyResolved() const noexcept { return producerProgressFullyResolved(); }

    /**
     * Number of direct-lane pushes rejected by the physical ring. Nonzero means
     * scheduler run-ahead exceeded provisioned transport storage.
     */
    virtual uint64_t transportOverflowEvents() const noexcept { return 0; }

    /// Backward-compatible name retained for diagnostics clients.
    uint64_t stagingOverflowEvents() const noexcept { return transportOverflowEvents(); }
};

using IArbitratablePort [[deprecated("use IMultiProducerPort")]] = IMultiProducerPort;

/**
 * PortBase - Type-erased base class for all ports.
 */
class PortBase {
public:
    virtual ~PortBase() = default;

    const std::string& name() const noexcept { return name_; }
    Unit* owner() const noexcept { return owner_; }

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
