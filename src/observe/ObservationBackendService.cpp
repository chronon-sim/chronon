// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <algorithm>
#include <cstring>

#include "ObservationBackend.hpp"

namespace chronon::observe {

size_t ObservationBackend::poll(size_t record_budget) noexcept {
    // The scheduler registration serializes queue readers, including assistance
    // from a producer blocked inside tick(). Never wait for the I/O worker here.
    if (stop_token_.stop_requested()) {
        should_stop_.store(true, std::memory_order_release);
        ThreadContextManager::instance().setBackendWakeup(nullptr, nullptr);
    }
    if (!running_.load(std::memory_order_acquire) || should_stop_.load(std::memory_order_acquire) ||
        io_in_flight_.load(std::memory_order_acquire) || record_budget == 0)
        return 0;
    try {
        const auto records = drainServiceBatch_(std::min(record_budget, size_t{256}));
        if (records) {
            service_->setRunnable(false);
            processEventsAsync_();
        }
        return records;
    } catch (...) {
        recordFailure_(std::current_exception());
        return 0;
    }
}

size_t ObservationBackend::drainServiceBatch_(size_t budget) {
    service_batch_size_ = 0;
    size_t count = 0;
    const size_t shared_id = ThreadContextManager::MAX_THREADS;
    // A bounded partial scan must not advance past the next unread event of
    // another queue. Observed maxima alone are insufficient after a budget cut.
    uint64_t next_unread = UINT64_MAX;
    const auto inspect = [&](const std::byte* ptr) {
        const auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
        if (header->total_size < sizeof(*header) + sizeof(uint64_t)) return;
        uint64_t cycle;
        std::memcpy(&cycle, ptr + sizeof(*header), sizeof(cycle));
        next_unread = std::min(next_unread, cycle);
    };
    const auto drain = [&](auto& queue, size_t id, size_t limit) {
        size_t read = 0;
        while (const auto* ptr = queue.prepareRead()) {
            const auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
            const size_t bytes = header->total_size;
            if (bytes < sizeof(*header) || bytes > queue.capacity())
                throw std::runtime_error("invalid observation record size");
            if (count == budget || read == limit ||
                bytes > service_batch_.size() - service_batch_size_) {
                inspect(ptr);
                break;
            }
            if (bytes >= sizeof(*header) + sizeof(uint64_t)) {
                uint64_t cycle;
                std::memcpy(&cycle, ptr + sizeof(*header), sizeof(cycle));
                per_queue_max_cycle_[id] = std::max(per_queue_max_cycle_[id], cycle);
            }
            std::memcpy(service_batch_.data() + service_batch_size_, ptr, bytes);
            service_batch_size_ += bytes;
            queue.finishRead(bytes);
            ++count;
            ++read;
        }
        queue.forceCommitRead();
        return read;
    };

    // Reserve a quarter of every batch for shared final snapshots/commits, and
    // rotate producer priority so a saturated queue cannot starve its peers.
    drain(queue_, shared_id, std::max(size_t{1}, budget / 4));
    auto& manager = ThreadContextManager::instance();
    const auto first = service_queue_cursor_;
    manager.forEachContextFrom(first, [&](ThreadContext* context) {
        if (drain(context->queue(), context->id(), budget))
            service_queue_cursor_ = context->id() + 1;
    });
    uint64_t minimum = UINT64_MAX;
    for (const auto cycle : per_queue_max_cycle_) {
        if (cycle) minimum = std::min(minimum, cycle);
    }
    minimum = std::min(minimum, next_unread);
    service_min_cycle_ = minimum == UINT64_MAX ? 0 : minimum;
    if (count) events_processed_.fetch_add(count, std::memory_order_relaxed);
    return count;
}

void ObservationBackend::flushServiceReorder_(bool all) {
    if (all)
        reorder_buffer_->flushAll(ready_buffer_);
    else
        reorder_buffer_->flushReady(ready_buffer_);
    for (const auto& record : ready_buffer_) {
        const auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(
            reorder_buffer_->arenaData(record.data_offset));
        processEvent_(header, reinterpret_cast<const std::byte*>(header) + sizeof(*header));
    }
    reorder_buffer_->compactArena();
}

void ObservationBackend::processServiceBatch_() {
    // Runs exclusively on the isolated I/O thread. No sort, arena growth,
    // compression or sink callbacks execute in a model worker's poll.
    if (reorder_buffer_) reorder_buffer_->updateMinCycle(service_min_cycle_);
    for (size_t offset = 0; offset < service_batch_size_;) {
        const auto* header =
            reinterpret_cast<const ObservationQueue::RecordHeader*>(service_batch_.data() + offset);
        const size_t bytes = header->total_size;
        if (reorder_buffer_ &&
            reorder_buffer_->arenaBytesUsed() + bytes > config_.service_buffer_bytes)
            flushServiceReorder_(true);
        if (header->type != ObservationQueue::EventType::SHUTDOWN) {
            const auto* data = service_batch_.data() + offset + sizeof(*header);
            if (reorder_buffer_)
                reorder_buffer_->bufferEvent(header, data, bytes - sizeof(*header));
            else
                processEvent_(header, data);
        } else {
            should_stop_.store(true, std::memory_order_release);
        }
        if (reorder_buffer_ && reorder_buffer_->size() > config_.reorder_max_events)
            flushServiceReorder_(false);
        offset += bytes;
    }
    if (reorder_buffer_) flushServiceReorder_(false);
    flush_();
}

}  // namespace chronon::observe
