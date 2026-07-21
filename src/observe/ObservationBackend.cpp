// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file ObservationBackend.cpp
/// @brief Core backend lifecycle, event draining, reorder buffer processing,
///        async I/O dispatch, and event routing.

#include "ObservationBackend.hpp"

#include <fmt/format.h>

#include <iostream>
#include <thread>

#include "../chronon/CpuPause.hpp"
#include "FormatRegistry.hpp"
#include "SIMDOps.hpp"

namespace chronon::observe {

ObservationBackend::ObservationBackend(ObservationQueue& queue) : queue_(queue), config_() {}

ObservationBackend::ObservationBackend(ObservationQueue& queue, const Config& config)
    : queue_(queue), config_(config) {}

ObservationBackend::~ObservationBackend() { stop(); }

void ObservationBackend::start() {
    if (running_.load(std::memory_order_relaxed)) {
        return;
    }

    io_in_flight_.store(false, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(io_wait_mutex_);
        io_async_error_ = nullptr;
        io_wait_timeout_count_ = 0;
    }
    {
        std::lock_guard<std::mutex> lock(io_dispatch_mutex_);
        io_work_ready_ = false;
        io_worker_stop_ = false;
    }

    initializeOutputDir_();
    prepareCounterSnapshotPlans_();

    if (config_.enable_reordering) {
        ReorderBuffer::Config rb_config;
        rb_config.watermark_cycles = config_.reorder_watermark_cycles;
        rb_config.max_buffer_events = config_.reorder_max_events;
        reorder_buffer_ = std::make_unique<ReorderBuffer>(rb_config);
        // +1 for shared queue (counter snapshots, index MAX_THREADS)
        per_queue_max_cycle_.resize(ThreadContextManager::MAX_THREADS + 1, 0);
    }

    counter_buffer_.reserve(COUNTER_BUFFER_FLUSH_SIZE * 2);

    ThreadContextManager::instance().setBackendWakeup(
        [](void* self) { static_cast<ObservationBackend*>(self)->wakeUp(); }, this);

    should_stop_.store(false, std::memory_order_relaxed);
    running_.store(true, std::memory_order_release);
    worker_thread_ = std::thread([this]() { run_(); });

    // Dedicated I/O thread is optional; if creation fails, fall back to
    // synchronous processing in processReorderBuffer_().
    if (config_.enable_reordering) {
        try {
            io_worker_thread_ = std::thread([this]() { ioWorkerLoop_(); });
        } catch (const std::exception& e) {
            std::cerr << "[observe] failed to start dedicated I/O thread: " << e.what()
                      << " (falling back to synchronous output)\n";
        } catch (...) {
            std::cerr << "[observe] failed to start dedicated I/O thread "
                         "(falling back to synchronous output)\n";
        }
    }
}

void ObservationBackend::stop() noexcept {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }

    should_stop_.store(true, std::memory_order_release);
    // Unregister wakeup callback before join so producers stop spin-waiting
    // and fall back to drop while backend is shutting down.
    ThreadContextManager::instance().setBackendWakeup(nullptr, nullptr);
    wakeUp();

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }

    {
        std::lock_guard<std::mutex> lock(io_dispatch_mutex_);
        io_worker_stop_ = true;
    }
    io_dispatch_cv_.notify_one();
    if (io_worker_thread_.joinable()) {
        io_worker_thread_.join();
    }

    running_.store(false, std::memory_order_release);
}

void ObservationBackend::wakeUp() noexcept {
    // Set atomic flag first (Phase 1/2 check this lock-free).
    wake_flag_.store(true, std::memory_order_release);
    // Signal condvar for Phase 3 blocking wait.
    // Lock+unlock serializes with the condvar::wait predicate check,
    // preventing lost wakeups where notify fires between the predicate
    // returning false and the actual futex sleep inside wait().
    wake_mutex_.lock();
    wake_mutex_.unlock();
    wake_cv_.notify_one();
}

void ObservationBackend::predeclareTimelineSourceTracks_() {
    if (!perfetto_writer_ || sim_process_uuid_ == 0) {
        return;
    }

    for (uint16_t source_id = 1; source_id < source_name_cache_.size(); ++source_id) {
        if (!source_name_cache_[source_id].empty()) {
            (void)timelineTrackForSource_(source_id);
        }
    }
}

void ObservationBackend::run_() {
    // The optional token is an observation-lifetime token, not the simulation
    // worker token.  Simulation termination must not stop this consumer: the
    // application still has to enqueue its final partial counter interval and
    // scheduler timeline before stop() requests the final drain.
    auto stop_fn = [this]() noexcept {
        should_stop_.store(true, std::memory_order_release);
        wakeUp();
    };
    stdexec::inplace_stop_callback<decltype(stop_fn)> wake_on_stop(stop_token_, std::move(stop_fn));

    while (!should_stop_.load(std::memory_order_acquire)) {
        size_t events = 0;

        if (reorder_buffer_) {
            // Reordering enabled: buffer events then flush sorted
            events = drainToReorderBuffer_();
            processReorderBuffer_(false);  // Flush events below watermark
            // Drain again after flush processing to prevent backpressure.
            // processReorderBuffer_ may take significant time (sort/merge/
            // arena snapshot), during which producer queues fill up.
            // This extra drain keeps queues drained even under heavy load.
            events += drainToReorderBuffer_();
        } else {
            // Original behavior: process events immediately
            events = drainAllQueues_();
        }

        // Flush if we processed events
        if (events > 0) {
            // Reorder mode flushes in processReorderBuffer_ (sync) or ioWorkerLoop_ (async).
            // Immediate mode flushes here.
            if (!reorder_buffer_) {
                flush_();
            }
            // Skip spin-wait when events are pending — immediately loop
            // back to drain more. Only spin-wait when queues are empty.
            continue;
        }

        // Adaptive spin-wait: hot spin → yield → condvar wait
        // Phase 1: Hot spin with CPU pause hint (lowest latency)
        bool woken = false;
        for (int i = 0; i < SPIN_HOT_ITERS; ++i) {
            if (wake_flag_.load(std::memory_order_acquire) ||
                should_stop_.load(std::memory_order_acquire)) {
                woken = true;
                break;
            }
            cpuPause();
        }

        // Phase 2: Yield to OS scheduler (medium latency)
        if (!woken) {
            for (int i = 0; i < SPIN_YIELD_ITERS; ++i) {
                if (wake_flag_.load(std::memory_order_acquire) ||
                    should_stop_.load(std::memory_order_acquire)) {
                    woken = true;
                    break;
                }
                std::this_thread::yield();
            }
        }

        // Phase 3: Condition variable wait (zero CPU, instant wakeup).
        // Uses mutex+condvar instead of atomic::wait because GCC 12's
        // std::atomic<bool>::wait uses a shared 16-bucket proxy pool
        // (sizeof(bool) != sizeof(int)), which causes missed wakeups
        // when other atomics (stdexec run_loop, progress counters) hash
        // to the same bucket.
        if (!woken) {
            std::unique_lock<std::mutex> lk(wake_mutex_);
            wake_cv_.wait(lk, [this] {
                return wake_flag_.load(std::memory_order_relaxed) ||
                       should_stop_.load(std::memory_order_relaxed);
            });
        }

        wake_flag_.store(false, std::memory_order_relaxed);
    }

    // Flush all per-thread queues before final drain
    ThreadContextManager::instance().flushAll();

    // Final drain on shutdown
    if (reorder_buffer_) {
        drainToReorderBuffer_();
        processReorderBuffer_(true);  // flush_all = true for shutdown
    } else {
        drainAllQueues_();
    }

    // Wait for any in-flight async I/O to complete before closing files
    waitForAsyncIO_();

    // Force OS flush on shutdown by resetting the timer
    last_os_flush_time_ = std::chrono::steady_clock::time_point{};
    flush_();

    // Finalize transposed counter CSV (flush pending row or handle single-batch case)
    if (config_.enable_counter_csv && config_.counter_csv_format == CounterCsvFormat::Pivoted) {
        finalizeCounterCsv_();
    }

    // Append submitted timeline streams (scheduler execution timeline) and close
    // the Perfetto file. Must run after the final drain so simulation trace
    // events and counter samples are already written.
    finalizeTimeline_();

    // Close files
    if (counter_file_.is_open()) counter_file_.close();
    if (default_sink_ && default_sink_->file.is_open()) default_sink_->file.close();
    for (auto& [name, sink] : custom_sinks_) {
        if (sink->file.is_open()) sink->file.close();
    }
}

void ObservationBackend::submitTimeline(TimelineStreamData&& data) {
    std::lock_guard<std::mutex> lock(timeline_submit_mutex_);
    submitted_timelines_.push_back(std::move(data));
}

size_t ObservationBackend::drainQueue_() {
    size_t events_read = 0;

    while (auto* ptr = queue_.prepareRead()) {
        auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
        const std::byte* data = ptr + sizeof(ObservationQueue::RecordHeader);

        processEvent_(header, data);

        auto event_type = header->type;
        queue_.finishRead(header->total_size);
        events_read++;

        // Check for shutdown signal
        if (event_type == ObservationQueue::EventType::SHUTDOWN) {
            should_stop_.store(true, std::memory_order_release);
            break;
        }
    }

    queue_.forceCommitRead();

    if (events_read > 0) {
        events_processed_.fetch_add(events_read, std::memory_order_relaxed);
    }

    return events_read;
}

size_t ObservationBackend::drainPerThreadQueues_() {
    size_t total_events = 0;

    ThreadContextManager::instance().forEachContext([&](ThreadContext* ctx) {
        SPSCQueue& q = ctx->queue();
        size_t batch = 0;
        size_t queue_events = 0;

        while (auto* ptr = q.prepareRead()) {
            auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
            const std::byte* data = ptr + sizeof(ObservationQueue::RecordHeader);

            processEvent_(header, data);

            // Cache event type before finishRead — once reader_pos_ is
            // published via eagerCommitRead, the producer may immediately
            // overwrite this buffer region, making header->type a UAF read.
            auto event_type = header->type;
            q.finishRead(header->total_size);
            queue_events++;

            // Publish freed space incrementally so producers unblock sooner.
            // Without this, the producer is blocked for the entire drain cycle
            // (~2500 events) because eagerCommitRead was only called at the end.
            if (++batch >= 64) {
                q.eagerCommitRead();
                batch = 0;
            }

            // Check for shutdown signal
            if (event_type == ObservationQueue::EventType::SHUTDOWN) {
                should_stop_.store(true, std::memory_order_release);
                break;
            }
        }

        q.eagerCommitRead();
        total_events += queue_events;
    });

    if (total_events > 0) {
        events_processed_.fetch_add(total_events, std::memory_order_relaxed);
    }

    return total_events;
}

size_t ObservationBackend::drainAllQueues_() {
    size_t total = 0;

    total += drainPerThreadQueues_();
    total += drainQueue_();

    return total;
}

size_t ObservationBackend::drainToReorderBuffer_() {
    size_t total_events = 0;

    ThreadContextManager::instance().forEachContext([&](ThreadContext* ctx) {
        SPSCQueue& q = ctx->queue();
        const size_t qid = ctx->id();
        uint64_t queue_max = per_queue_max_cycle_[qid];
        size_t batch = 0;
        size_t queue_events = 0;

        while (auto* ptr = q.prepareRead()) {
            auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
            const std::byte* data = ptr + sizeof(ObservationQueue::RecordHeader);
            size_t data_size = header->total_size - sizeof(ObservationQueue::RecordHeader);

            // Check for shutdown signal before buffering
            if (header->type == ObservationQueue::EventType::SHUTDOWN) {
                should_stop_.store(true, std::memory_order_release);
                q.finishRead(header->total_size);
                break;
            }

            if (data_size >= sizeof(uint64_t)) {
                uint64_t cycle = 0;
                std::memcpy(&cycle, data, sizeof(uint64_t));
                queue_max = std::max(queue_max, cycle);
            }

            reorder_buffer_->bufferEvent(header, data, data_size);

            q.finishRead(header->total_size);
            queue_events++;

            // Publish freed space incrementally
            if (++batch >= 64) {
                q.eagerCommitRead();
                batch = 0;
            }
        }

        per_queue_max_cycle_[qid] = queue_max;
        q.eagerCommitRead();
        total_events += queue_events;
    });

    {
        constexpr size_t legacy_idx = ThreadContextManager::MAX_THREADS;
        uint64_t queue_max = per_queue_max_cycle_[legacy_idx];
        size_t legacy_events = 0;

        while (auto* ptr = queue_.prepareRead()) {
            auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
            const std::byte* data = ptr + sizeof(ObservationQueue::RecordHeader);
            size_t data_size = header->total_size - sizeof(ObservationQueue::RecordHeader);

            // Check for shutdown signal
            if (header->type == ObservationQueue::EventType::SHUTDOWN) {
                should_stop_.store(true, std::memory_order_release);
                queue_.finishRead(header->total_size);
                break;
            }

            if (data_size >= sizeof(uint64_t)) {
                uint64_t cycle = 0;
                std::memcpy(&cycle, data, sizeof(uint64_t));
                queue_max = std::max(queue_max, cycle);
            }

            reorder_buffer_->bufferEvent(header, data, data_size);

            queue_.finishRead(header->total_size);
            legacy_events++;
        }

        per_queue_max_cycle_[legacy_idx] = queue_max;
        total_events += legacy_events;
    }

    queue_.forceCommitRead();

    if (total_events > 0) {
        events_processed_.fetch_add(total_events, std::memory_order_relaxed);
    }

    uint64_t global_min =
        simd::minNonZero(per_queue_max_cycle_.data(), per_queue_max_cycle_.size());

    if (global_min != UINT64_MAX) {
        reorder_buffer_->updateMinCycle(global_min);
    }

    return total_events;
}

void ObservationBackend::processReorderBuffer_(bool flush_all) {
    if (!reorder_buffer_) {
        return;
    }

    // Non-blocking path: if previous async I/O batch is still in-flight,
    // skip flushing and return immediately so the drain thread can keep
    // draining producer queues.  This prevents deadlock when producers
    // use spin_wait backpressure — the drain thread must never block while
    // queues could be filling up.  Events stay in the reorder buffer and
    // will be flushed on a subsequent call once I/O completes.
    // On shutdown (flush_all), we must block to ensure all events are written.
    if (!flush_all && io_in_flight_.load(std::memory_order_acquire)) {
        return;
    }

    // Wait for previous async I/O batch to complete before reusing buffers.
    // This only blocks on shutdown (flush_all) or when I/O just finished
    // (io_in_flight_ is already false, so waitForAsyncIO_ returns immediately).
    waitForAsyncIO_();

    if (flush_all) {
        reorder_buffer_->flushAll(ready_buffer_);
    } else {
        reorder_buffer_->flushReady(ready_buffer_);
    }

    if (ready_buffer_.empty()) {
        return;
    }

    // Drain queues between flush and I/O dispatch.  flushReady() does an
    // expensive sort/merge that may have allowed producer queues to fill.
    // This mid-flush drain keeps queues responsive under heavy load.
    if (!flush_all) {
        drainToReorderBuffer_();
    }

    if (io_worker_thread_.joinable()) {
        io_arena_ = reorder_buffer_->snapshotArena(ready_buffer_);
        std::swap(ready_buffer_, io_buffer_);
        reorder_buffer_->compactArena();
        processEventsAsync_();
    } else {
        // Fallback: synchronous processing (dedicated I/O thread unavailable)
        for (const auto& record : ready_buffer_) {
            if (record.data_size < sizeof(ObservationQueue::RecordHeader)) {
                continue;
            }

            const std::byte* arena_ptr = reorder_buffer_->arenaData(record.data_offset);
            const auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(arena_ptr);
            const std::byte* data = arena_ptr + sizeof(ObservationQueue::RecordHeader);

            processEvent_(header, data);
        }

        // Compact the arena now that all flushed records have been consumed.
        reorder_buffer_->compactArena();
        flush_();
    }
}

void ObservationBackend::waitForAsyncIO_() {
    std::unique_lock<std::mutex> lock(io_wait_mutex_);
    while (io_in_flight_.load(std::memory_order_acquire)) {
        bool done = io_wait_cv_.wait_for(lock, ASYNC_IO_WAIT_TIMEOUT, [this]() {
            return !io_in_flight_.load(std::memory_order_acquire);
        });
        if (done) {
            break;
        }

        ++io_wait_timeout_count_;
        if (io_wait_timeout_count_ == 1 || (io_wait_timeout_count_ % 8) == 0) {
            std::cerr << "[observe] async I/O batch still in-flight after "
                      << (io_wait_timeout_count_ * ASYNC_IO_WAIT_TIMEOUT.count()) << "ms";
            if (should_stop_.load(std::memory_order_acquire)) {
                std::cerr << " (stop requested)";
            }
            std::cerr << "\n";
        }
    }

    if (io_async_error_) {
        try {
            std::rethrow_exception(io_async_error_);
        } catch (const std::exception& e) {
            std::cerr << "[observe] async I/O worker failed: " << e.what()
                      << " (falling back to synchronized pipeline)\n";
        } catch (...) {
            std::cerr << "[observe] async I/O worker failed with unknown exception "
                         "(falling back to synchronized pipeline)\n";
        }
        io_async_error_ = nullptr;
    }

    io_wait_timeout_count_ = 0;
}

void ObservationBackend::processEventsAsync_() {
    {
        std::lock_guard<std::mutex> lock(io_wait_mutex_);
        io_in_flight_.store(true, std::memory_order_release);
    }
    {
        std::lock_guard<std::mutex> lock(io_dispatch_mutex_);
        io_work_ready_ = true;
    }
    io_dispatch_cv_.notify_one();
}

void ObservationBackend::ioWorkerLoop_() {
    for (;;) {
        {
            std::unique_lock<std::mutex> lock(io_dispatch_mutex_);
            io_dispatch_cv_.wait(lock, [this]() { return io_work_ready_ || io_worker_stop_; });
            if (io_worker_stop_ && !io_work_ready_) {
                break;
            }
            io_work_ready_ = false;
        }

        struct CompletionGuard {
            ObservationBackend* self;
            ~CompletionGuard() {
                self->io_in_flight_.store(false, std::memory_order_release);
                self->io_wait_cv_.notify_all();
            }
        } done{this};

        try {
            for (const auto& record : io_buffer_) {
                if (record.data_size < sizeof(ObservationQueue::RecordHeader)) {
                    continue;
                }

                const std::byte* arena_ptr =
                    io_arena_.data.data() + (record.data_offset - io_arena_.base_offset);
                const auto* header =
                    reinterpret_cast<const ObservationQueue::RecordHeader*>(arena_ptr);
                const std::byte* data = arena_ptr + sizeof(ObservationQueue::RecordHeader);

                processEvent_(header, data);
            }
            flush_();
        } catch (...) {
            std::lock_guard<std::mutex> lock(io_wait_mutex_);
            if (!io_async_error_) {
                io_async_error_ = std::current_exception();
            }
        }
    }
}

void ObservationBackend::processEvent_(const ObservationQueue::RecordHeader* header,
                                       const std::byte* data) {
    size_t data_size = header->total_size - sizeof(ObservationQueue::RecordHeader);
    bool is_structured = (header->flags & 1) != 0;

    switch (header->type) {
        case ObservationQueue::EventType::COUNTER_SNAPSHOT: {
            const bool want_timeline =
                config_.timeline_counters && perfetto_writer_ && perfetto_writer_->isOpen();
            if (!config_.enable_counter_csv && !want_timeline) break;

            if ((header->flags & COUNTER_SNAPSHOT_BATCH_FLAG) != 0) {
                if (data_size < sizeof(CounterSnapshotBatchHeader)) break;
                CounterSnapshotBatchHeader batch{};
                std::memcpy(&batch, data, sizeof(batch));
                if (batch.plan_id >= counter_snapshot_plans_.size() ||
                    batch.plan_id >= counter_snapshot_column_indices_.size()) {
                    break;
                }
                const auto& metadata = counter_snapshot_plans_[batch.plan_id].entries;
                if (batch.count != metadata.size() ||
                    data_size < sizeof(batch) + metadata.size() * sizeof(uint64_t)) {
                    break;
                }
                const auto& columns = counter_snapshot_column_indices_[batch.plan_id];
                const std::byte* values = data + sizeof(batch);
                for (size_t i = 0; i < metadata.size(); ++i) {
                    uint64_t value = 0;
                    std::memcpy(&value, values + i * sizeof(value), sizeof(value));
                    processCounterSample_(batch.cycle, metadata[i].unit_name,
                                          metadata[i].counter_name, value, want_timeline,
                                          columns[i]);
                }
            } else if (data_size >= 20) {
                size_t offset = 0;
                uint64_t cycle = 0;
                std::memcpy(&cycle, data + offset, 8);
                offset += 8;

                uint16_t unit_name_length = 0;
                std::memcpy(&unit_name_length, data + offset, 2);
                offset += 2;

                if (data_size >= offset + unit_name_length + 10) {
                    std::string_view unit_name(reinterpret_cast<const char*>(data + offset),
                                               unit_name_length);
                    offset += unit_name_length;

                    uint16_t counter_name_length = 0;
                    std::memcpy(&counter_name_length, data + offset, 2);
                    offset += 2;

                    if (data_size >= offset + counter_name_length + 8) {
                        std::string_view counter_name(reinterpret_cast<const char*>(data + offset),
                                                      counter_name_length);
                        offset += counter_name_length;

                        uint64_t value = 0;
                        std::memcpy(&value, data + offset, 8);
                        processCounterSample_(cycle, unit_name, counter_name, value, want_timeline);
                    }
                }
            }
            break;
        }

        case ObservationQueue::EventType::TIMELINE_EVENT: {
            if (perfetto_writer_ && perfetto_writer_->isOpen()) {
                processTimelineEvent_(data, data_size);
            }
            break;
        }

        case ObservationQueue::EventType::LOG_EVENT: {
            if (log_handler_) {
                log_handler_(header->type, data, data_size);
            } else if (is_structured) {
                processStructuredLog_(data, data_size);
            } else {
                if (data_size > 9) {
                    uint64_t cycle = 0;
                    uint8_t level = 0;
                    std::memcpy(&cycle, data, 8);
                    std::memcpy(&level, data + 8, 1);
                    const char* message = reinterpret_cast<const char*>(data + 9);

                    const char* level_str = "INFO";
                    Channel channel = Channel::Info;
                    switch (static_cast<LogLevel>(level)) {
                        case LogLevel::Debug:
                            level_str = "DEBUG";
                            channel = Channel::Debug;
                            break;
                        case LogLevel::Info:
                            level_str = "INFO";
                            channel = Channel::Info;
                            break;
                        case LogLevel::Warn:
                            level_str = "WARN";
                            channel = Channel::Warn;
                            break;
                        case LogLevel::Error:
                            level_str = "ERROR";
                            channel = Channel::Error;
                            break;
                    }

                    auto* sink = channel_sink_[static_cast<size_t>(channel)];
                    if (sink && sink->file.is_open()) {
                        fmt::format_to(std::back_inserter(sink->buffer), "[{:>10}] [{:>5}] {}\n",
                                       cycle, level_str, message);
                        if (sink->buffer.size() >= TEXT_BUFFER_FLUSH_SIZE) {
                            sink->flush();
                        }
                        local_bytes_written_ += data_size + 30;
                    }
                }
            }
            break;
        }

        case ObservationQueue::EventType::EPOCH_COMMIT:
        case ObservationQueue::EventType::EPOCH_ROLLBACK:
            // These are markers for lookahead support - no output
            break;

        case ObservationQueue::EventType::SHUTDOWN:
            // Handled in drainQueue_
            break;
    }
}

void ObservationBackend::processCounterSample_(uint64_t cycle, std::string_view unit_name,
                                               std::string_view counter_name, uint64_t value,
                                               bool want_timeline, size_t column_index) {
    if (want_timeline) {
        writeCounterToTimeline_(cycle, unit_name, counter_name, value);
    }

    if (!config_.enable_counter_csv) return;

    if (config_.counter_csv_format == CounterCsvFormat::Pivoted) {
        std::string key;
        if (column_index == SIZE_MAX) {
            key.reserve(unit_name.size() + 1 + counter_name.size());
            key.append(unit_name);
            key.push_back('.');
            key.append(counter_name);
        }

        if (!counter_csv_streaming_) {
            if (counter_first_cycle_ == UINT64_MAX) counter_first_cycle_ = cycle;
            if (cycle == counter_first_cycle_) {
                counter_first_batch_.emplace_back(std::move(key), value);
                return;
            }

            finalizeCounterColumns_();
            writeCounterCsvHeader_();
            current_counter_row_.assign(counter_columns_.size(), 0);
            for (const auto& [first_key, first_value] : counter_first_batch_) {
                auto it = counter_col_index_.find(first_key);
                if (it != counter_col_index_.end()) current_counter_row_[it->second] = first_value;
            }
            flushCounterRow_(counter_first_cycle_);
            counter_first_batch_.clear();
            counter_first_batch_.shrink_to_fit();
            counter_csv_streaming_ = true;
            current_counter_cycle_ = cycle;
            current_counter_row_.assign(counter_columns_.size(), 0);
        } else if (current_counter_cycle_ == UINT64_MAX) {
            current_counter_cycle_ = cycle;
        } else if (cycle != current_counter_cycle_) {
            flushCounterRow_(current_counter_cycle_);
            current_counter_cycle_ = cycle;
            current_counter_row_.assign(counter_columns_.size(), 0);
        }

        if (column_index == SIZE_MAX) {
            auto it = counter_col_index_.find(key);
            if (it != counter_col_index_.end()) column_index = it->second;
        }
        if (column_index < current_counter_row_.size()) {
            current_counter_row_[column_index] = value;
        }
        return;
    }

    if (!counter_file_.is_open()) return;
    fmt::format_to(std::back_inserter(counter_buffer_), "{},{},{},{}\n", cycle, unit_name,
                   counter_name, value);
    local_bytes_written_ += unit_name.length() + counter_name.length() + 30;

    if (!derived_counter_defs_.empty()) {
        if (long_current_cycle_ != UINT64_MAX && cycle != long_current_cycle_) {
            emitLongDerivedValues_(long_current_cycle_);
            long_cycle_values_.clear();
        }
        long_current_cycle_ = cycle;
        std::string long_key;
        long_key.reserve(unit_name.size() + 1 + counter_name.size());
        long_key.append(unit_name);
        long_key.push_back('.');
        long_key.append(counter_name);
        long_cycle_values_[std::move(long_key)] = value;
    }

    if (counter_buffer_.size() >= COUNTER_BUFFER_FLUSH_SIZE) flushCounterBuffer_();
}

void ObservationBackend::processStructuredLog_(const std::byte* data, size_t data_size) {
    if (data_size < sizeof(StructuredRecord)) {
        return;
    }

    const auto* rec = reinterpret_cast<const StructuredRecord*>(data);
    const std::byte* args_data = data + sizeof(StructuredRecord);
    size_t args_size = data_size - sizeof(StructuredRecord);

    const char* level_str = "INFO";
    Channel channel = Channel::Info;
    if (rec->category & category::LOG_DEBUG) {
        level_str = "DEBUG";
        channel = Channel::Debug;
    } else if (rec->category & category::LOG_WARN) {
        level_str = "WARN";
        channel = Channel::Warn;
    } else if (rec->category & category::LOG_ERROR) {
        level_str = "ERROR";
        channel = Channel::Error;
    }

    auto* sink = channel_sink_[static_cast<size_t>(channel)];
    if (sink && sink->file.is_open()) {
        writeEventAsText_(rec, args_data, args_size, level_str, *sink);
    }
}

}  // namespace chronon::observe
