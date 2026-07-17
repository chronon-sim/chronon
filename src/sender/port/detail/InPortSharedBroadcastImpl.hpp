// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

// Included by InPort.hpp after InPort is fully defined, inside
// chronon::sender. Keep these templates out of the primary declaration so the
// public header remains within the repository's source-file length limit.

template <typename T>
std::optional<typename InPort<T>::SharedBroadcastCandidate> InPort<T>::peekReadySharedBroadcast_(
    uint64_t current_cycle) const noexcept {
    std::optional<SharedBroadcastCandidate> best;
    for (auto* connection : shared_broadcast_connections_) {
        auto view = connection->peekSharedBroadcast();
        if (!view || view->arrive_cycle > current_cycle) continue;
        if (!best || view->arrive_cycle < best->arrive_cycle ||
            (view->arrive_cycle == best->arrive_cycle &&
             connection->connId() < best->connection->connId())) {
            best = SharedBroadcastCandidate{.connection = connection,
                                            .data = view->data,
                                            .sequence = view->sequence,
                                            .arrive_cycle = view->arrive_cycle,
                                            .enqueue_cycle = view->enqueue_cycle};
        }
    }
    return best;
}

template <typename T>
template <typename Filter>
std::optional<T> InPort<T>::tryReceiveSharedBroadcastFiltered_(uint64_t current_cycle,
                                                               Filter& filter) {
    while (true) {
        auto candidate = peekReadySharedBroadcast_(current_cycle);
        if (!candidate) return std::nullopt;

        StoredMessage message{.data = *candidate->data};
        message.enqueue_cycle = candidate->enqueue_cycle;
        message.sender_id = candidate->connection->connId();
        if (policy_ == PortPolicy::LegacyFastPath) {
            const uint64_t filter_generation =
                receiver_filter_generation_.load(std::memory_order_acquire);
            if (filter_generation != std::numeric_limits<uint64_t>::max()) {
                message.receiver_generation_snapshot =
                    candidate->connection->sharedReceiverGenerationSnapshot(candidate->sequence,
                                                                            filter_generation);
            }
        }
        const bool canceled =
            isReceiverCanceled_(message) || !std::invoke(filter, std::as_const(message.data));
        candidate->connection->popSharedBroadcast(candidate->sequence);
        if (canceled) continue;
        return std::move(message.data);
    }
}

template <typename T>
std::optional<T> InPort<T>::tryReceiveSharedBroadcast_(uint64_t current_cycle) {
    auto accept_all = [](const T&) noexcept { return true; };
    return tryReceiveSharedBroadcastFiltered_(current_cycle, accept_all);
}
