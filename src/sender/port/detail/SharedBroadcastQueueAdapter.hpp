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
            message.sender_id = candidate->connection->connId();
            std::forward<Visitor>(visitor)(message);
            candidate->connection->popSharedBroadcast(candidate->sequence);
            return true;
        } else {
            auto candidate = nextReady_(current_cycle);
            if (!candidate) return false;

            StoredMessage message{.data = *candidate->data};
            message.enqueue_cycle = candidate->enqueue_cycle;
            message.sender_id = candidate->connection->connId();
            std::forward<Visitor>(visitor)(message);
            candidate->connection->popSharedBroadcast(candidate->sequence);
            return true;
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
        return nextReady_(current_cycle).has_value();
    }

    [[nodiscard]] std::optional<uint64_t> minArrivalCycle() const override {
        std::optional<uint64_t> earliest;
        for (auto* connection : connections_) {
            if (auto view = connection->peekSharedBroadcast()) {
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
        for (const auto* connection : connections_) {
            count += connection->sharedBroadcastQueuedCount();
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
        for (auto* connection : connections_) {
            connection->flushSharedBroadcast();
        }
    }

private:
    struct Candidate {
        Connection<T>* connection = nullptr;
        const T* data = nullptr;
        uint64_t sequence = 0;
        uint64_t arrive_cycle = 0;
        uint64_t enqueue_cycle = 0;
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

    [[nodiscard, gnu::always_inline]] inline std::optional<Candidate> nextReady_(
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
                                     .data = view->data,
                                     .sequence = view->sequence,
                                     .arrive_cycle = view->arrive_cycle,
                                     .enqueue_cycle = view->enqueue_cycle};
                }
                ++ready_connection_index_;
            }
            invalidateReadyGroup_();
        }
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
                                 .data = view->data,
                                 .sequence = view->sequence,
                                 .arrive_cycle = view->arrive_cycle,
                                 .enqueue_cycle = view->enqueue_cycle};
            }
        }
        return best;
    }

    [[nodiscard]] static constexpr size_t unlimitedCapacity_() noexcept {
        return std::numeric_limits<size_t>::max();
    }

    std::vector<Connection<T>*> connections_;
    const bool cycle_scan_enabled_ = cycleScanEnabled_();
    mutable size_t ready_connection_index_ = 0;
    mutable uint64_t ready_arrival_cycle_ = kInvalidReadyCycle;
};

}  // namespace chronon::sender::detail
