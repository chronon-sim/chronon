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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "InPort.hpp"
#include "OutPort.hpp"

namespace chronon::sender {

/**
 * Shared delay-one broadcast transport.
 *
 * A conventional P-producer/C-consumer port bus stores each payload C times
 * and executes P*C queue pushes. This fabric stores each producer payload
 * once in a cycle-indexed lane and lets every consumer replay the source cycle
 * one tick later. The original OutPort -> InPort connections remain in the
 * dependency graph, but sealPortTopology() marks their physical queues as
 * dependency-only.
 *
 * This intentionally is not a drop-in general queue:
 *
 * - every bound connection must have delay=1;
 * - every consumer must call consume() on each scheduled tick and drain the
 *   callback synchronously; publish() preserves normal port wakeups, and
 *   forward cycle gaps are treated as activity-scheduled empty cycles;
 * - model-visible bounded capacity, InPort selective cancellation and
 *   OutPort::cancelInFlight() are outside this transport's contract.
 *
 * Those restrictions keep the hot path to one producer-owned write and
 * read-only replay by consumers. Models opt in explicitly and retain their
 * existing OutPort/InPort graph solely for scheduler ordering.
 */
template <typename T, size_t ProducerCount, size_t ConsumerCount, size_t RingDepth = 512,
          size_t MaxMessagesPerProducerCycle = 8>
    requires std::default_initializable<T> && std::is_copy_assignable_v<T>
class DelayOneBroadcastFabric {
public:
    static constexpr size_t PRODUCER_COUNT = ProducerCount;
    static constexpr size_t CONSUMER_COUNT = ConsumerCount;
    static constexpr size_t RING_DEPTH = RingDepth;
    static constexpr size_t MAX_MESSAGES_PER_PRODUCER_CYCLE = MaxMessagesPerProducerCycle;

    static_assert(ProducerCount > 0, "broadcast fabric needs at least one producer");
    static_assert(ConsumerCount > 0, "broadcast fabric needs at least one consumer");
    static_assert(RingDepth > 0 && (RingDepth & (RingDepth - 1)) == 0,
                  "broadcast fabric ring depth must be a power of two");
    static_assert(MaxMessagesPerProducerCycle > 0,
                  "broadcast fabric cycle bucket must hold at least one message");
    static_assert(MaxMessagesPerProducerCycle <= std::numeric_limits<uint16_t>::max(),
                  "broadcast fabric uses a 16-bit per-cycle message count");

    /** Bind a stable producer id to its declared OutPort. */
    void bindProducer(size_t producer, OutPort<T>& port) {
        checkProducer_(producer);
        if (sealed_) {
            throw std::logic_error("cannot bind broadcast producer after topology is sealed");
        }
        if (producer_ports_[producer] && producer_ports_[producer] != &port) {
            throw std::logic_error("broadcast producer id is already bound");
        }
        for (size_t i = 0; i < ProducerCount; ++i) {
            if (i != producer && producer_ports_[i] == &port) {
                throw std::logic_error("OutPort is already bound to another broadcast producer");
            }
        }
        producer_ports_[producer] = &port;
    }

    /** Bind a stable consumer id to its declared InPort. */
    void bindConsumer(size_t consumer, InPort<T>& port) {
        checkConsumer_(consumer);
        if (sealed_) {
            throw std::logic_error("cannot bind broadcast consumer after topology is sealed");
        }
        if (consumer_ports_[consumer] && consumer_ports_[consumer] != &port) {
            throw std::logic_error("broadcast consumer id is already bound");
        }
        for (size_t i = 0; i < ConsumerCount; ++i) {
            if (i != consumer && consumer_ports_[i] == &port) {
                throw std::logic_error("InPort is already bound to another broadcast consumer");
            }
        }
        consumer_ports_[consumer] = &port;
    }

    /**
     * Validate the complete P x C, delay=1 bus and disable its payload queues.
     * Call after all graph connections are created and before simulation
     * initialization. A failure leaves every connection in its original
     * transport mode.
     */
    void sealPortTopology() {
        if (sealed_) return;

        for (size_t producer = 0; producer < ProducerCount; ++producer) {
            if (!producer_ports_[producer]) {
                throw std::logic_error("broadcast fabric has an unbound producer id");
            }
        }
        for (size_t consumer = 0; consumer < ConsumerCount; ++consumer) {
            if (!consumer_ports_[consumer]) {
                throw std::logic_error("broadcast fabric has an unbound consumer id");
            }
        }

        // Complete validation before mutating any connection, so a malformed
        // topology cannot leave a partially dependency-only bus behind.
        for (size_t producer = 0; producer < ProducerCount; ++producer) {
            const auto* out = producer_ports_[producer];
            if (out->connectionCount() != ConsumerCount) {
                throw std::logic_error("broadcast OutPort does not have complete fanout");
            }
            for (size_t consumer = 0; consumer < ConsumerCount; ++consumer) {
                const auto* conn = out->connectionTo(consumer_ports_[consumer]);
                if (!conn) {
                    throw std::logic_error("broadcast OutPort is missing a bound consumer");
                }
                if (conn->delay() != 1) {
                    throw std::logic_error("broadcast fabric only supports delay=1 connections");
                }
            }
        }

        for (auto* out : producer_ports_) {
            out->setDependencyOnlyTransport(true);
        }
        sealed_ = true;
    }

    [[nodiscard]] bool portTopologySealed() const noexcept { return sealed_; }

    /// Single-producer operation for one producer lane.
    void publish(size_t producer, uint64_t send_cycle, const T& value) {
        publishImpl_(producer, send_cycle, value);
    }

    /// Move-enabled publish for models that construct a payload in place.
    void publish(size_t producer, uint64_t send_cycle, T&& value) {
        publishImpl_(producer, send_cycle, std::move(value));
    }

    /**
     * Replay send_cycle=(consumer_cycle-1), first by stable producer id and
     * then by original send order. The scheduler's dependency-only edges are
     * the synchronization contract: every producer must have completed the
     * source cycle before its consumer calls this function.
     */
    template <typename Fn>
    void consume(size_t consumer, uint64_t consumer_cycle, Fn&& fn) {
        checkConsumer_(consumer);
        if (consumer_cycle == 0) return;

        const uint64_t consumed_exclusive =
            consumers_[consumer].consumed_exclusive.load(std::memory_order_relaxed);
        if (consumer_cycle == consumed_exclusive) {
            return;  // Idempotent protection against a duplicate same-cycle drain.
        }
        if (consumer_cycle < consumed_exclusive) {
            throw std::logic_error("broadcast consumer cycle moved backwards");
        }

        // A sleeping/interval consumer may legitimately jump over cycles in
        // which no payload was published. publish() schedules a port wake for
        // every non-empty source cycle, so a consumer that calls consume() on
        // each scheduled tick cannot skip an eventful cycle.
        const uint64_t send_cycle = consumer_cycle - 1;
        for (size_t producer = 0; producer < ProducerCount; ++producer) {
            const auto& bucket = producers_[producer].buckets[send_cycle & (RingDepth - 1)];
            if (bucket.published_cycle.load(std::memory_order_acquire) != send_cycle) {
                continue;
            }
            const uint16_t count = bucket.count;
            for (uint16_t index = 0; index < count; ++index) {
                fn(bucket.messages[index]);
            }
        }
        consumers_[consumer].consumed_exclusive.store(consumer_cycle, std::memory_order_release);
    }

    [[nodiscard]] uint64_t consumedExclusive(size_t consumer) const {
        checkConsumer_(consumer);
        return consumers_[consumer].consumed_exclusive.load(std::memory_order_acquire);
    }

private:
    static constexpr uint64_t EMPTY_CYCLE = std::numeric_limits<uint64_t>::max();

    struct Bucket {
        std::array<T, MaxMessagesPerProducerCycle> messages{};
        uint16_t count = 0;
        std::atomic<uint64_t> published_cycle{EMPTY_CYCLE};
    };

    struct alignas(64) ProducerLane {
        std::array<Bucket, RingDepth> buckets{};
        // Refreshed only on ring reuse, not on the per-message hot path.
        uint64_t reuse_safe_exclusive = 0;
        uint64_t last_published_cycle = EMPTY_CYCLE;
    };

    struct alignas(64) ConsumerCursor {
        // Number of source cycles fully consumed. Zero means source cycle 0
        // has not been read; consuming at local cycle C publishes C.
        std::atomic<uint64_t> consumed_exclusive{0};
    };

    static void checkProducer_(size_t producer) {
        if (producer >= ProducerCount) {
            throw std::out_of_range("broadcast fabric producer id out of range");
        }
    }

    static void checkConsumer_(size_t consumer) {
        if (consumer >= ConsumerCount) {
            throw std::out_of_range("broadcast fabric consumer id out of range");
        }
    }

    template <typename U>
    void publishImpl_(size_t producer, uint64_t send_cycle, U&& value) {
        checkProducer_(producer);
        if (send_cycle == EMPTY_CYCLE) {
            throw std::overflow_error("broadcast fabric cannot represent UINT64_MAX cycle");
        }

        auto& lane = producers_[producer];
        if (lane.last_published_cycle != EMPTY_CYCLE && send_cycle < lane.last_published_cycle) {
            throw std::logic_error("broadcast producer cycle moved backwards");
        }
        auto& bucket = lane.buckets[send_cycle & (RingDepth - 1)];
        const uint64_t old_cycle = bucket.published_cycle.load(std::memory_order_relaxed);
        const bool first_message_for_cycle = old_cycle != send_cycle;

        if (first_message_for_cycle) {
            if (old_cycle != EMPTY_CYCLE) {
                if (send_cycle <= old_cycle || send_cycle - old_cycle < RingDepth) {
                    throw std::logic_error("broadcast producer cycle is not monotonic");
                }
                ensureOverwriteSafe_(lane, old_cycle);
            }
            bucket.count = 0;
        }

        if (bucket.count >= MaxMessagesPerProducerCycle) {
            throw std::overflow_error("broadcast per-producer cycle bucket overflow");
        }
        bucket.messages[bucket.count] = std::forward<U>(value);
        ++bucket.count;
        bucket.published_cycle.store(send_cycle, std::memory_order_release);
        if (first_message_for_cycle) {
            lane.last_published_cycle = send_cycle;
            if (sealed_) {
                for (auto* consumer_port : consumer_ports_) {
                    wakeUnitAt(consumer_port->owner(), send_cycle + 1);
                }
            }
        }
    }

    void ensureOverwriteSafe_(ProducerLane& lane, uint64_t old_cycle) {
        if (lane.reuse_safe_exclusive > old_cycle) return;

        uint64_t safe_exclusive = std::numeric_limits<uint64_t>::max();
        for (const auto& consumer : consumers_) {
            safe_exclusive = std::min(safe_exclusive,
                                      consumer.consumed_exclusive.load(std::memory_order_acquire));
        }
        if (safe_exclusive <= old_cycle) {
            throw std::overflow_error("broadcast fabric would overwrite an unread cycle bucket");
        }
        lane.reuse_safe_exclusive = safe_exclusive;
    }

    std::array<ProducerLane, ProducerCount> producers_{};
    std::array<ConsumerCursor, ConsumerCount> consumers_{};
    std::array<OutPort<T>*, ProducerCount> producer_ports_{};
    std::array<InPort<T>*, ConsumerCount> consumer_ports_{};
    bool sealed_ = false;
};

}  // namespace chronon::sender
