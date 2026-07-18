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
 * Selective-flush tracing (CHRONON_STAGE_TRACE env, retained for compatibility).
 *
 * Permanent diagnostic feature. Emits one line per install/retire/
 * receiver-side predicate checks for investigating flush behavior in
 * production builds.
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
 * PortPolicy - Source-compatible InPort policy tag.
 *
 * Both values use the same receiver-owned selective-flush engine. The tag is
 * retained so existing model declarations remain source compatible while the
 * public FlushRange contract is independent of queue policy and topology.
 */
enum class PortPolicy : uint8_t {
    LegacyFastPath = 0,
    General [[deprecated("Use LegacyFastPath")]] = LegacyFastPath,  ///< Backward-compatible alias.
    StageSelective = 1,
};

/**
 * Explicit keep range for an in-flight selective flush.
 *
 * UID order follows the conventional CPU meaning: a larger UID is younger.
 * Bounds retain their inclusive/exclusive form, so full-width keys and UID
 * zero never require caller-side `uid - 1` or `uid + 1` arithmetic.
 */
class FlushRange {
public:
    template <typename K>
    [[nodiscard]] static constexpr FlushRange youngerThan(K key) noexcept {
        return {0, static_cast<uint64_t>(key), true, true};
    }

    template <typename K>
    [[nodiscard]] static constexpr FlushRange atAndYounger(K key) noexcept {
        return {0, static_cast<uint64_t>(key), true, false};
    }

    template <typename K>
    [[nodiscard]] static constexpr FlushRange olderThan(K key) noexcept {
        return {static_cast<uint64_t>(key), std::numeric_limits<uint64_t>::max(), true, true};
    }

    template <typename K>
    [[nodiscard]] static constexpr FlushRange atAndOlder(K key) noexcept {
        return {static_cast<uint64_t>(key), std::numeric_limits<uint64_t>::max(), false, true};
    }

    template <typename MinK, typename MaxK>
    [[nodiscard]] static constexpr FlushRange outsideInclusive(MinK min_keep,
                                                               MaxK max_keep) noexcept {
        return {static_cast<uint64_t>(min_keep), static_cast<uint64_t>(max_keep), true, true};
    }

    [[nodiscard]] constexpr bool keeps(uint64_t key) const noexcept {
        if (key < min_keep_ || key > max_keep_) return false;
        if (key == min_keep_ && !min_inclusive_) return false;
        if (key == max_keep_ && !max_inclusive_) return false;
        return true;
    }

    [[nodiscard]] constexpr FlushRange intersectedWith(FlushRange other) const noexcept {
        FlushRange result = *this;
        if (other.min_keep_ > result.min_keep_) {
            result.min_keep_ = other.min_keep_;
            result.min_inclusive_ = other.min_inclusive_;
        } else if (other.min_keep_ == result.min_keep_) {
            result.min_inclusive_ = result.min_inclusive_ && other.min_inclusive_;
        }

        if (other.max_keep_ < result.max_keep_) {
            result.max_keep_ = other.max_keep_;
            result.max_inclusive_ = other.max_inclusive_;
        } else if (other.max_keep_ == result.max_keep_) {
            result.max_inclusive_ = result.max_inclusive_ && other.max_inclusive_;
        }
        return result;
    }

    [[nodiscard]] constexpr uint64_t minKeep() const noexcept { return min_keep_; }
    [[nodiscard]] constexpr uint64_t maxKeep() const noexcept { return max_keep_; }
    [[nodiscard]] constexpr bool minInclusive() const noexcept { return min_inclusive_; }
    [[nodiscard]] constexpr bool maxInclusive() const noexcept { return max_inclusive_; }

    friend constexpr bool operator==(const FlushRange&, const FlushRange&) = default;

private:
    constexpr FlushRange(uint64_t min_keep, uint64_t max_keep, bool min_inclusive,
                         bool max_inclusive) noexcept
        : min_keep_(min_keep),
          max_keep_(max_keep),
          min_inclusive_(min_inclusive),
          max_inclusive_(max_inclusive) {}

    uint64_t min_keep_ = 0;
    uint64_t max_keep_ = std::numeric_limits<uint64_t>::max();
    bool min_inclusive_ = true;
    bool max_inclusive_ = true;
};

namespace detail {

template <typename T>
struct PortEnvelope {
    T data;
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    const std::atomic<uint64_t>* cancel_epoch = nullptr;  ///< nullptr => not cancelable
    uint64_t epoch_snapshot = 0;
#endif
    /// Producer's localCycle at push time (arrive_cycle - delay). Used by
    /// receiver-owned predicates to decide whether a message was in flight at
    /// the start of a flush. The sender never reads receiver flush state.
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
 * A single live "keep only range among messages enqueued before flush_cycle"
 * rule. The extractor is part of the predicate, allowing independent flushes
 * over different message fields to compose without mutable global extractor
 * state or a reset sequence.
 */
template <typename T>
struct SelectiveFlushPredicate {
    using KeyExtractor = uint64_t (*)(const T&);

    uint64_t flush_cycle = 0;
    FlushRange keep_range =
        FlushRange::outsideInclusive(uint64_t{0}, std::numeric_limits<uint64_t>::max());
    KeyExtractor key_extractor = nullptr;
};

/**
 * Receiver-owned active selective-flush predicates for an InPort.
 *
 * Incoming messages are ordered by arrival cycle, not enqueue cycle. With
 * heterogeneous MPSC delays enqueue cycles can regress, so retirement uses
 * the queue's stable arrival frontier. A predicate for flush cycle F can only
 * affect arrivals through F - 1 + max_incoming_delay. Once the next queue head
 * is beyond that bound, epoch-free dependency progress proves that no producer
 * can publish another affected message.
 *
 * State is installed, evaluated, and retired exclusively by the destination
 * Unit. There are no locks or atomics in this control plane.
 */
template <typename T>
struct InPortSelectiveFlushState {
    using Predicate = SelectiveFlushPredicate<T>;
    using KeyExtractor = typename Predicate::KeyExtractor;

    std::vector<Predicate> slots;
    /// High-water mark tracking for diagnostics (CHRONON_STAGE_TRACE).
    size_t high_water = 0;

    void install(uint64_t flush_cycle, FlushRange keep_range, KeyExtractor key_extractor) {
        // Calls for the same field in one cycle describe one architectural
        // flush. Intersect them in place; different fields remain independent.
        for (auto& p : slots) {
            if (p.flush_cycle == flush_cycle && p.key_extractor == key_extractor) {
                p.keep_range = p.keep_range.intersectedWith(keep_range);
                return;
            }
        }
        slots.push_back({flush_cycle, keep_range, key_extractor});
        if (slots.size() > high_water) {
            high_water = slots.size();
        }
    }

    [[nodiscard]] bool hasRetirementCandidate(uint64_t front_arrival,
                                              uint32_t max_incoming_delay) const noexcept {
        for (const auto& p : slots) {
            if (p.flush_cycle == 0 ||
                front_arrival > lastAffectedArrival_(p.flush_cycle, max_incoming_delay)) {
                return true;
            }
        }
        return false;
    }

    void retireBeforeArrival(uint64_t front_arrival, uint32_t max_incoming_delay,
                             std::optional<uint64_t> producer_progress_floor) noexcept {
        size_t i = 0;
        while (i < slots.size()) {
            const auto& p = slots[i];
            // In epoch-free MPSC mode an empty lane can still publish an older
            // arrival after another lane exposes a future head. Wait until
            // every producer has completed all enqueue cycles covered by this
            // predicate before treating the visible head as a stable frontier.
            if (producer_progress_floor && *producer_progress_floor < p.flush_cycle) {
                ++i;
                continue;
            }
            if (p.flush_cycle == 0 ||
                front_arrival > lastAffectedArrival_(p.flush_cycle, max_incoming_delay)) {
                slots[i] = slots.back();
                slots.pop_back();
                continue;
            }
            ++i;
        }
    }

    [[nodiscard]] bool shouldCancel(const PortEnvelope<T>& message) const noexcept {
        for (const auto& p : slots) {
            if (message.enqueue_cycle >= p.flush_cycle) continue;
            if (!p.keep_range.keeps(p.key_extractor(message.data))) return true;
        }
        return false;
    }

    [[nodiscard]] bool empty() const noexcept { return slots.empty(); }
    [[nodiscard]] size_t active_slot_count() const noexcept { return slots.size(); }

private:
    [[nodiscard]] static uint64_t lastAffectedArrival_(uint64_t flush_cycle,
                                                       uint32_t max_incoming_delay) noexcept {
        const uint64_t last_enqueue = flush_cycle == 0 ? 0 : flush_cycle - 1;
        const uint64_t max_cycle = std::numeric_limits<uint64_t>::max();
        return max_incoming_delay > max_cycle - last_enqueue
                   ? max_cycle
                   : last_enqueue + static_cast<uint64_t>(max_incoming_delay);
    }
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
