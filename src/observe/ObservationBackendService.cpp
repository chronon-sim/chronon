// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <algorithm>
#include <cstring>

#include "ObservationBackend.hpp"

namespace chronon::observe {

void ObservationBackend::configureClocks(std::vector<std::optional<ClockDomain>> clocks,
                                         ClockDomain reference, size_t lookahead) {
    if (isRunning()) throw std::logic_error("configure source clocks before observation startup");
    source_clocks_ = std::move(clocks);
    reference_clock_ = std::move(reference);
    clock_admission_limit_ = std::max(size_t{2}, lookahead + 1);
    config_.enable_reordering = true;
}

const ClockDomain* ObservationBackend::sourceClock_(uint16_t source) const noexcept {
    return source < source_clocks_.size() && source_clocks_[source] ? &*source_clocks_[source]
                                                                    : nullptr;
}

uint16_t ObservationBackend::recordSource_(const ObservationQueue::RecordHeader* header,
                                           const std::byte* data, size_t size) const {
    if (header->type == ObservationQueue::EventType::LOG_EVENT &&
        size >= sizeof(StructuredRecord)) {
        StructuredRecord record;
        std::memcpy(&record, data, sizeof(record));
        return record.source_id;
    }
    if (header->type == ObservationQueue::EventType::TIMELINE_EVENT &&
        size >= sizeof(TimelineRecord)) {
        TimelineRecord record;
        std::memcpy(&record, data, sizeof(record));
        return TimelineTrackRegistry::instance().get(record.track_id).source_id;
    }
    return 0;
}

bool ObservationBackend::hostRecord_(const ObservationQueue::RecordHeader* header,
                                     const std::byte* data, size_t size) const {
    return clockMode() && header->type == ObservationQueue::EventType::LOG_EVENT &&
           !sourceClock_(recordSource_(header, data, size));
}

std::pair<SimTime, uint32_t> ObservationBackend::recordTime_(
    const ObservationQueue::RecordHeader* header, const std::byte* data, size_t size) const {
    if (header->flags & CLOCK_LIFECYCLE_FLAG) {
        if (size < sizeof(ClockLifecycleStamp))
            throw std::logic_error("invalid lifecycle timestamp");
        ClockLifecycleStamp stamp;
        std::memcpy(&stamp, data + size - sizeof(stamp), sizeof(stamp));
        return {SimTime(stamp.numerator, stamp.denominator), stamp.phase == 1 ? 0 : UINT32_MAX - 1};
    }
    uint64_t cycle = 0;
    if (size >= sizeof(cycle)) std::memcpy(&cycle, data, sizeof(cycle));
    if (header->type == ObservationQueue::EventType::COUNTER_SNAPSHOT) {
        if (!(header->flags & COUNTER_SNAPSHOT_BATCH_FLAG))
            throw std::logic_error("explicit clocks require owner counter snapshots");
        CounterSnapshotBatchHeader batch;
        if (size < sizeof(batch)) throw std::logic_error("invalid clock counter snapshot");
        std::memcpy(&batch, data, sizeof(batch));
        return {batch.time_den ? SimTime(batch.time_num, batch.time_den)
                               : reference_clock_->edge(cycle),
                (header->flags & COUNTER_SNAPSHOT_AFTER_FLAG)   ? UINT32_MAX
                : (header->flags & COUNTER_SNAPSHOT_FINAL_FLAG) ? 1
                                                                : 0};
    }
    const auto source = recordSource_(header, data, size);
    const auto* clock = sourceClock_(source);
    if (!clock) throw std::logic_error("observation source has no hardware clock");
    return {clock->edge(cycle),
            source < source_order_.size() ? source_order_[source] + 2 : source + 2};
}

bool ObservationBackend::tryAdmitClockTime(SimTime time) {
    rethrowIfFailed();
    if (should_stop_.load(std::memory_order_acquire) || !isRunning())
        throw std::runtime_error("clock observation backend stopped during simulation");
    const auto ns = time.floorNanoseconds();
    const auto acknowledged = clock_acknowledged_.load(std::memory_order_acquire);
    while (!clock_admitted_.empty() && clock_admitted_.front() < acknowledged)
        clock_admitted_.pop_front();
    if (std::binary_search(clock_admitted_.begin(), clock_admitted_.end(), ns)) return true;
    if (clock_admitted_.size() >= clock_admission_limit_) {
        wakeUp();
        return false;
    }
    clock_admitted_.insert(std::lower_bound(clock_admitted_.begin(), clock_admitted_.end(), ns),
                           ns);
    return true;
}

void ObservationBackend::publishClockProgress(uint64_t exclusive_ns) noexcept {
    auto prior = clock_frontier_.load(std::memory_order_relaxed);
    while (prior < exclusive_ns &&
           !clock_frontier_.compare_exchange_weak(prior, exclusive_ns, std::memory_order_release,
                                                  std::memory_order_relaxed)) {
    }
    wakeUp();
}

size_t ObservationBackend::drainClockServiceBatch_(size_t budget) {
    service_batch_size_ = 0;
    clock_scan_complete_ = false;
    auto& manager = ThreadContextManager::instance();
    constexpr size_t shared = ThreadContextManager::MAX_THREADS;
    if (!clock_scanning_) {
        // Progress acquire happens before capturing queue heads. Every record
        // covered by that promise has already been force-published by its owner.
        clock_scan_frontier_ = clock_frontier_.load(std::memory_order_acquire);
        clock_scan_heads_.fill(0);
        clock_scan_heads_[shared] = queue_.publishedPosition();
        manager.forEachContext([&](ThreadContext* context) {
            clock_scan_heads_[context->id()] = context->queue().publishedPosition();
        });
        clock_scanning_ = true;
    }
    size_t count = 0;
    bool complete = true;
    const auto drain = [&](auto& queue, size_t id) {
        while (queue.readPosition() < clock_scan_heads_[id]) {
            const auto* ptr = queue.prepareRead();
            if (!ptr) throw std::logic_error("published observation prefix unavailable");
            const auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
            const auto bytes = header->total_size;
            if (bytes < sizeof(*header) || bytes > queue.capacity())
                throw std::runtime_error("invalid observation record size");
            if (count == budget || bytes > service_batch_.size() - service_batch_size_) {
                complete = false;
                break;
            }
            std::memcpy(service_batch_.data() + service_batch_size_, ptr, bytes);
            service_batch_size_ += bytes;
            queue.finishRead(bytes);
            ++count;
        }
        queue.forceCommitRead();
    };
    drain(queue_, shared);
    manager.forEachContext([&](ThreadContext* context) { drain(context->queue(), context->id()); });
    if (complete) {
        clock_scanning_ = false;
        clock_scan_complete_ = true;
    }
    if (count) events_processed_.fetch_add(count, std::memory_order_relaxed);
    // Dispatch an empty completed scan too: quiet producers advance the frontier.
    return count || (complete &&
                     clock_scan_frontier_ > clock_acknowledged_.load(std::memory_order_acquire))
               ? std::max(count, size_t{1})
               : 0;
}

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
        const auto records = clockMode()
                                 ? drainClockServiceBatch_(std::min(record_budget, size_t{256}))
                                 : drainServiceBatch_(std::min(record_budget, size_t{256}));
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
    if (reorder_buffer_ && !clockMode()) reorder_buffer_->updateMinCycle(service_min_cycle_);
    for (size_t offset = 0; offset < service_batch_size_;) {
        const auto* header =
            reinterpret_cast<const ObservationQueue::RecordHeader*>(service_batch_.data() + offset);
        const size_t bytes = header->total_size;
        if (reorder_buffer_ &&
            reorder_buffer_->arenaBytesUsed() + bytes > config_.service_buffer_bytes) {
            if (clockMode())
                throw std::length_error(
                    "clock observation window exceeds service_buffer_bytes; increase its budget "
                    "or reduce the observation rate/lookahead");
            flushServiceReorder_(true);
        }
        if (header->type != ObservationQueue::EventType::SHUTDOWN) {
            const auto* data = service_batch_.data() + offset + sizeof(*header);
            if (reorder_buffer_ && !hostRecord_(header, data, bytes - sizeof(*header))) {
                if (clockMode() &&
                    recordTime_(header, data, bytes - sizeof(*header)).first.floorNanoseconds() <
                        clock_acknowledged_.load(std::memory_order_relaxed))
                    throw std::logic_error("clock observation record violates published progress");
                reorder_buffer_->bufferEvent(header, data, bytes - sizeof(*header));
            } else
                processEvent_(header, data);
        } else {
            should_stop_.store(true, std::memory_order_release);
        }
        if (reorder_buffer_ && reorder_buffer_->size() > config_.reorder_max_events)
            flushServiceReorder_(false);
        offset += bytes;
    }
    if (clockMode() && clock_scan_complete_) {
        reorder_buffer_->updateMinCycle(clock_scan_frontier_);
        flushServiceReorder_(false);
        clock_acknowledged_.store(clock_scan_frontier_, std::memory_order_release);
    } else if (reorder_buffer_ && !clockMode())
        flushServiceReorder_(false);
    flush_();
}

}  // namespace chronon::observe
