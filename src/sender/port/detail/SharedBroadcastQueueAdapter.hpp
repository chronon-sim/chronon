// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "../Connection.hpp"
#include "../Port.hpp"

namespace chronon::sender::detail {

/**
 * Consumer-side queue view over one or more shared-broadcast connections.
 *
 * Delay-one scheduler dependencies guarantee that every producer has completed
 * source cycle C before a consumer starts cycle C+1. The adapter therefore
 * finds the oldest ready source cycle once and replays producer lanes directly
 * in stable connection order. Multiple messages from one producer stay on the
 * same lane, so the hot path neither rescans every producer after each message
 * nor copies a whole cycle into an intermediate sortable batch.
 *
 * Sender cancellation remains lazy in Connection::peekSharedBroadcast(). A
 * receiver flush resets the replay cursor and discards transport-resident
 * future entries.
 */
template <typename T>
class SharedBroadcastQueueAdapter final : public IMessageQueue<PortEnvelope<T>> {
public:
    using StoredMessage = PortEnvelope<T>;
    using SharedBroadcastView = typename Connection<T>::SharedBroadcastView;
    using SharedBroadcast = typename Connection<T>::SharedBroadcast;
    using SharedBroadcastCursor = typename Connection<T>::SharedBroadcastCursor;
    static constexpr bool resolves_sender_cancellation = true;

    void registerConnection(Connection<T>* connection) {
        if (!connection) {
            throw std::invalid_argument("shared broadcast connection is null");
        }
        auto it = connections_.begin();
        for (; it != connections_.end(); ++it) {
            if (*it == connection) return;
            if ((*it)->connId() > connection->connId()) break;
        }
        connections_.insert(it, connection);
        invalidateReadyGroup_();
    }

    /**
     * Compile a complete delay-one destination fan-in into direct typed lanes.
     * The owning OutPorts and Connections retain storage and lifetime; this
     * adapter only snapshots their stable initialization-time addresses.
     */
    bool finalizeCompleteGroup(size_t producer_count) {
        if (producer_count == 0 || connections_.size() != producer_count || fused_replay_) {
            return fused_replay_ && replay_lanes_.size() == producer_count;
        }
        replay_lanes_.clear();
        replay_lanes_.reserve(connections_.size());
        for (auto* connection : connections_) {
            ReplayLane lane{.transport = connection->sharedBroadcastTransport(),
                            .cursor = connection->sharedBroadcastCursor(),
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
                            .cancel_before = connection->sharedBroadcastCancelBefore(),
#endif
                            .sender_id = connection->connId()};
            if (!lane.transport || !lane.cursor) {
                replay_lanes_.clear();
                return false;
            }
            replay_lanes_.push_back(lane);
        }
        fused_replay_ = true;
        invalidateReadyGroup_();
        return true;
    }

    // Shared payloads are published by OutPort directly into the transport.
    bool push(StoredMessage, uint64_t) override {
        throw std::logic_error("shared broadcast payloads must be published through OutPort");
    }

    template <typename Visitor>
    [[gnu::always_inline]] inline bool consumeReady(uint64_t current_cycle, Visitor&& visitor) {
        if constexpr (!std::is_copy_constructible_v<T>) {
            (void)current_cycle;
            (void)visitor;
            throw std::logic_error("shared broadcast requires copy-constructible payloads");
        } else if (!cycle_scan_enabled_) {
            auto candidate = peekBest_(current_cycle);
            if (!candidate) return false;

            StoredMessage message{.data = *candidate->data};
            message.enqueue_cycle = candidate->enqueue_cycle;
            message.sender_id = candidate->sender_id;
            std::forward<Visitor>(visitor)(message);
            popCandidate_(*candidate);
            return true;
        } else {
            auto candidate = fused_replay_ ? nextFusedReady_(current_cycle)
                                           : nextConnectionReady_(current_cycle);
            if (!candidate) return false;

            StoredMessage message{.data = *candidate->data};
            message.enqueue_cycle = candidate->enqueue_cycle;
            message.sender_id = candidate->sender_id;
            std::forward<Visitor>(visitor)(message);
            popCandidate_(*candidate);
            return true;
        }
    }

    /** Fast default receive path when the InPort has no receiver-side filter. */
    [[gnu::always_inline]] inline std::optional<T> tryPopData(uint64_t current_cycle) {
        if constexpr (!std::is_copy_constructible_v<T>) {
            (void)current_cycle;
            return std::nullopt;
        } else {
            auto candidate = !cycle_scan_enabled_
                                 ? peekBest_(current_cycle)
                                 : (fused_replay_ ? nextFusedReady_(current_cycle)
                                                  : nextConnectionReady_(current_cycle));
            if (!candidate) return std::nullopt;
            std::optional<T> result{*candidate->data};
            popCandidate_(*candidate);
            return result;
        }
    }

    std::optional<StoredMessage> tryPop(uint64_t current_cycle) override {
        std::optional<StoredMessage> result;
        consumeReady(current_cycle,
                     [&](StoredMessage& message) { result.emplace(std::move(message)); });
        return result;
    }

    std::vector<StoredMessage> popAll(uint64_t current_cycle) override {
        std::vector<StoredMessage> result;
        popAllInto(result, current_cycle);
        return result;
    }

    void popAllInto(std::vector<StoredMessage>& out, uint64_t current_cycle) override {
        out.clear();
        while (auto message = tryPop(current_cycle)) {
            out.push_back(std::move(*message));
        }
    }

    [[nodiscard]] bool hasReady(uint64_t current_cycle) const override {
        if (!cycle_scan_enabled_) return peekBest_(current_cycle).has_value();
        return (fused_replay_ ? nextFusedReady_(current_cycle)
                              : nextConnectionReady_(current_cycle))
            .has_value();
    }

    [[nodiscard]] std::optional<uint64_t> minArrivalCycle() const override {
        std::optional<uint64_t> earliest;
        if (fused_replay_) {
            for (auto& lane : replay_lanes_) {
                if (auto replay_view = readLane_(lane)) {
                    const auto& view = replay_view->view;
                    if (!earliest || view.arrive_cycle < *earliest) {
                        earliest = view.arrive_cycle;
                    }
                }
            }
        } else {
            for (auto* connection : connections_) {
                auto view = connection->peekSharedBroadcast();
                if (!view) continue;
                if (!earliest || view->arrive_cycle < *earliest) {
                    earliest = view->arrive_cycle;
                }
            }
        }
        return earliest;
    }

    [[nodiscard]] bool empty() const override { return size() == 0; }
    [[nodiscard]] bool full() const override { return false; }

    [[nodiscard]] size_t size() const override {
        size_t count = 0;
        if (fused_replay_) {
            for (const auto& lane : replay_lanes_) count += laneSize_(lane);
        } else {
            for (const auto* connection : connections_) {
                count += connection->sharedBroadcastQueuedCount();
            }
        }
        return count;
    }

    [[nodiscard]] size_t capacity() const noexcept override { return unlimitedCapacity_(); }
    [[nodiscard]] size_t storageCapacity() const noexcept override { return unlimitedCapacity_(); }
    [[nodiscard]] size_t available() const override { return unlimitedCapacity_(); }

    void setCapacity(size_t capacity) override {
        if (capacity != unlimitedCapacity_()) {
            throw std::logic_error("shared broadcast requires an unlimited InPort");
        }
    }

    void clear() override {
        invalidateReadyGroup_();
        if (fused_replay_) {
            for (auto& lane : replay_lanes_) flushLane_(lane);
        } else {
            for (auto* connection : connections_) {
                connection->flushSharedBroadcast();
            }
        }
    }

private:
    struct ReplayLane {
        SharedBroadcast* transport = nullptr;
        SharedBroadcastCursor* cursor = nullptr;
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        const std::atomic<uint64_t>* cancel_before = nullptr;
#endif
        uint32_t sender_id = 0;
    };

    struct Candidate {
        Connection<T>* connection = nullptr;
        size_t lane_index = std::numeric_limits<size_t>::max();
        const T* data = nullptr;
        uint64_t sequence = 0;
        uint64_t arrive_cycle = 0;
        uint64_t enqueue_cycle = 0;
        uint32_t sender_id = 0;
    };

    struct ReplayView {
        SharedBroadcastView view{};
    };

    static constexpr uint64_t kInvalidReadyCycle = std::numeric_limits<uint64_t>::max();

    [[nodiscard]] static bool cycleScanEnabled_() noexcept {
        const char* value = std::getenv("CHRONON_EXPERIMENTAL_SHARED_BROADCAST_CYCLE_BATCH");
        return value == nullptr || value[0] != '0' || value[1] != '\0';
    }

    void invalidateReadyGroup_() const noexcept {
        ready_arrival_cycle_ = kInvalidReadyCycle;
        ready_connection_index_ = 0;
    }

    [[nodiscard, gnu::always_inline]] inline std::optional<Candidate> nextConnectionReady_(
        uint64_t current_cycle) const noexcept {
        for (;;) {
            if (ready_arrival_cycle_ == kInvalidReadyCycle) {
                for (auto* connection : connections_) {
                    auto view = connection->peekSharedBroadcast();
                    if (!view || view->arrive_cycle > current_cycle) continue;
                    ready_arrival_cycle_ = std::min(ready_arrival_cycle_, view->arrive_cycle);
                }
                if (ready_arrival_cycle_ == kInvalidReadyCycle) return std::nullopt;
                ready_connection_index_ = 0;
            }

            while (ready_connection_index_ < connections_.size()) {
                auto* connection = connections_[ready_connection_index_];
                auto view = connection->peekSharedBroadcast();
                if (view && view->arrive_cycle == ready_arrival_cycle_) {
                    return Candidate{.connection = connection,
                                     .lane_index = std::numeric_limits<size_t>::max(),
                                     .data = view->data,
                                     .sequence = view->sequence,
                                     .arrive_cycle = view->arrive_cycle,
                                     .enqueue_cycle = view->enqueue_cycle,
                                     .sender_id = connection->connId()};
                }
                ++ready_connection_index_;
            }
            const uint64_t exhausted_cycle = ready_arrival_cycle_;
            invalidateReadyGroup_();
            // Per-lane arrival cycles are monotonic. Once the current cycle is exhausted,
            // no later group can be ready; the next API call still rescans for late publishes.
            if (exhausted_cycle == current_cycle) return std::nullopt;
        }
    }

    [[nodiscard, gnu::always_inline]] inline std::optional<ReplayView> readLane_(
        ReplayLane& lane) const noexcept {
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
        uint64_t head = lane.transport->consumerSequence(*lane.cursor);
        const uint64_t cancel_before = lane.cancel_before->load(std::memory_order_acquire);
        if (head < cancel_before) {
            head = cancel_before;
            lane.transport->advance(*lane.cursor, head);
        }
#endif
        auto view = lane.transport->peek(*lane.cursor);
        if (!view) return std::nullopt;
        return ReplayView{.view = *view};
    }

    [[nodiscard, gnu::always_inline]] inline std::optional<Candidate> nextFusedReady_(
        uint64_t current_cycle) const noexcept {
        for (;;) {
            if (ready_arrival_cycle_ == kInvalidReadyCycle) {
                for (size_t index = 0; index < replay_lanes_.size(); ++index) {
                    const auto replay_view = readLane_(replay_lanes_[index]);
                    if (!replay_view || replay_view->view.arrive_cycle > current_cycle) continue;
                    ready_arrival_cycle_ =
                        std::min(ready_arrival_cycle_, replay_view->view.arrive_cycle);
                }
                if (ready_arrival_cycle_ == kInvalidReadyCycle) return std::nullopt;
                ready_connection_index_ = 0;
            }

            while (ready_connection_index_ < replay_lanes_.size()) {
                const size_t index = ready_connection_index_;
                const auto replay_view = readLane_(replay_lanes_[index]);
                if (replay_view && replay_view->view.arrive_cycle == ready_arrival_cycle_) {
                    const auto& view = replay_view->view;
                    return Candidate{.connection = nullptr,
                                     .lane_index = index,
                                     .data = view.data,
                                     .sequence = view.sequence,
                                     .arrive_cycle = view.arrive_cycle,
                                     .enqueue_cycle = view.enqueue_cycle,
                                     .sender_id = replay_lanes_[index].sender_id};
                }
                ++ready_connection_index_;
            }
            const uint64_t exhausted_cycle = ready_arrival_cycle_;
            invalidateReadyGroup_();
            if (exhausted_cycle == current_cycle) return std::nullopt;
        }
    }

    [[gnu::always_inline]] inline void popCandidate_(const Candidate& candidate) noexcept {
        if (candidate.connection) {
            candidate.connection->popSharedBroadcast(candidate.sequence);
            return;
        }
        auto& lane = replay_lanes_[candidate.lane_index];
        const uint64_t head = lane.transport->consumerSequence(*lane.cursor);
        if (head == candidate.sequence) {
            lane.transport->advance(*lane.cursor, candidate.sequence + 1);
        }
    }

    static void flushLane_(ReplayLane& lane) noexcept {
        const uint64_t tail = lane.transport->publishedExclusive();
        lane.transport->advance(*lane.cursor, tail);
    }

    [[nodiscard]] static size_t laneSize_(const ReplayLane& lane) noexcept {
        const uint64_t head = lane.transport->consumerSequence(*lane.cursor);
        const uint64_t tail = lane.transport->publishedExclusive();
        return static_cast<size_t>(tail - std::min(head, tail));
    }

    [[nodiscard]] std::optional<Candidate> peekBest_(uint64_t current_cycle) const noexcept {
        std::optional<Candidate> best;
        for (auto* connection : connections_) {
            auto view = connection->peekSharedBroadcast();
            if (!view || view->arrive_cycle > current_cycle) continue;
            if (!best || view->arrive_cycle < best->arrive_cycle ||
                (view->arrive_cycle == best->arrive_cycle &&
                 connection->connId() < best->connection->connId())) {
                best = Candidate{.connection = connection,
                                 .lane_index = std::numeric_limits<size_t>::max(),
                                 .data = view->data,
                                 .sequence = view->sequence,
                                 .arrive_cycle = view->arrive_cycle,
                                 .enqueue_cycle = view->enqueue_cycle,
                                 .sender_id = connection->connId()};
            }
        }
        return best;
    }

    [[nodiscard]] static constexpr size_t unlimitedCapacity_() noexcept {
        return std::numeric_limits<size_t>::max();
    }

    std::vector<Connection<T>*> connections_;
    mutable std::vector<ReplayLane> replay_lanes_;
    bool fused_replay_ = false;
    const bool cycle_scan_enabled_ = cycleScanEnabled_();
    mutable size_t ready_connection_index_ = 0;
    mutable uint64_t ready_arrival_cycle_ = kInvalidReadyCycle;
};

}  // namespace chronon::sender::detail
