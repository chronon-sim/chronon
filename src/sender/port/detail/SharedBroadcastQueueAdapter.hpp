// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "../Connection.hpp"
#include "../Port.hpp"

namespace chronon::sender::detail {

/**
 * Consumer-side queue view over one or more shared-broadcast connections.
 *
 * SharedBroadcastTransport remains the producer-owned, one-write/many-reader
 * data plane. This adapter only presents a destination's independent cursors
 * through IMessageQueue, so InPort can use the same ready/filter/cancel/flush
 * control plane as every other transport.
 *
 * The connection registry is immutable after simulation initialization and is
 * touched only by the destination unit while running. No lock or extra shared
 * cache line is introduced on the receive path.
 */
template <typename T>
class SharedBroadcastQueueAdapter final : public IMessageQueue<PortEnvelope<T>> {
public:
    using StoredMessage = PortEnvelope<T>;
    static constexpr bool resolves_sender_cancellation = true;

    explicit SharedBroadcastQueueAdapter(
        const std::atomic<uint64_t>* receiver_filter_generation) noexcept
        : receiver_filter_generation_(receiver_filter_generation) {}

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
    }

    void captureReceiverCancellationScope(uint64_t generation) noexcept {
        for (auto* connection : connections_) {
            connection->captureSharedReceiverCancellationScope(generation);
        }
    }

    // Shared payloads are published by OutPort directly into the transport.
    bool push(StoredMessage, uint64_t) override {
        throw std::logic_error("shared broadcast payloads must be published through OutPort");
    }

    template <typename Visitor>
    bool consumeReady(uint64_t current_cycle, Visitor&& visitor) {
        const uint64_t generation =
            receiver_filter_generation_
                ? receiver_filter_generation_->load(std::memory_order_acquire)
                : std::numeric_limits<uint64_t>::max();
        return consumeReady(current_cycle, generation, std::forward<Visitor>(visitor));
    }

    template <typename Visitor>
    bool consumeReady(uint64_t current_cycle, uint64_t receiver_filter_generation,
                      Visitor&& visitor) {
        auto candidate = peekBest_(current_cycle);
        if (!candidate) return false;

        StoredMessage message{.data = *candidate->data};
        message.enqueue_cycle = candidate->enqueue_cycle;
        message.sender_id = candidate->connection->connId();
        if (receiver_filter_generation != std::numeric_limits<uint64_t>::max()) {
            message.receiver_generation_snapshot =
                candidate->connection->sharedReceiverGenerationSnapshot(candidate->sequence,
                                                                        receiver_filter_generation);
        }
        std::forward<Visitor>(visitor)(message);
        candidate->connection->popSharedBroadcast(candidate->sequence);
        return true;
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
        return peekBest_(current_cycle).has_value();
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

    const std::atomic<uint64_t>* receiver_filter_generation_ = nullptr;
    std::vector<Connection<T>*> connections_;
};

}  // namespace chronon::sender::detail
