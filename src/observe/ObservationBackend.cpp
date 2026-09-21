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

#include "FormatRegistry.hpp"

namespace chronon::observe {

ObservationBackend::ObservationBackend(ObservationQueue& queue) : queue_(queue), config_() {}

ObservationBackend::ObservationBackend(ObservationQueue& queue, const Config& config)
    : queue_(queue), config_(config) {}

ObservationBackend::~ObservationBackend() {
    stop();
    if (service_) service_->detach();
}

void ObservationBackend::attachScheduler(HostServices& scheduler) {
    if (isRunning()) throw std::logic_error("attach observation scheduler before start");
    // Repeated setup of a stopped backend must not accumulate detached slots
    // and dilute its share of scheduler polls. Reuse the completed attachment.
    if (io_job_ && scheduler.hasRegistration(service_)) return;
    if (service_) service_->detach();
    io_job_.reset();
    // start() also attaches the owned scheduler. When switching to an external
    // one, release its old I/O job first, then join the standalone driver.
    if (standalone_scheduler_.get() != &scheduler) standalone_scheduler_.reset();
    service_ = scheduler.add(*this);
    service_->detach();
    try {
        io_job_ = scheduler.addIO(service_, this, [](void* self) noexcept {
            static_cast<ObservationBackend*>(self)->runIO_();
        });
    } catch (...) {
        service_.reset();
        throw;
    }
}

void ObservationBackend::start() {
    if (isRunning()) return;
    if (config_.service_buffer_bytes < 65536 || config_.service_buffer_bytes > (1u << 30))
        throw std::invalid_argument(
            "observation service_buffer_bytes must be in [65536,1073741824]");
    if (!service_) {
        standalone_scheduler_ = std::make_unique<HostServices>(true);
        attachScheduler(*standalone_scheduler_);
    }
    service_batch_.resize(SERVICE_BATCH_BYTES);
    service_batch_size_ = service_queue_cursor_ = 0;
    per_queue_max_cycle_.assign(ThreadContextManager::MAX_THREADS + 1, 0);
    io_in_flight_.store(false, std::memory_order_relaxed);
    {
        std::lock_guard lock(error_mutex_);
        output_error_ = nullptr;
    }
    should_stop_.store(false, std::memory_order_relaxed);
    // Opening, encoding and closing all run on the scheduler's I/O lane.
    io_phase_ = IOPhase::Open;
    io_job_->submit();
    io_job_->wait();
    rethrowIfFailed();
    ThreadContextManager::instance().setBackendWakeup(
        [](void* self) { static_cast<ObservationBackend*>(self)->wakeUp(); }, this);
    ThreadContextManager::instance().setService(service_);
    queue_.setPublicationSignal(&service_->ready);
    running_.store(true, std::memory_order_release);
    service_->activate(*this);
}

void ObservationBackend::stop() noexcept {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }

    should_stop_.store(true, std::memory_order_release);
    if (service_) {
        service_->detach();
        ThreadContextManager::instance().setService(nullptr);
        queue_.setPublicationSignal(nullptr);
    }
    // Unregister wakeup callback before final drain so producers stop spin-waiting
    // and fall back to drop while backend is shutting down.
    ThreadContextManager::instance().setBackendWakeup(nullptr, nullptr);
    try {
        waitForAsyncIO_();
        ThreadContextManager::instance().flushAll();
        while (drainServiceBatch_(256)) {
            processEventsAsync_();
            waitForAsyncIO_();
        }
    } catch (...) {
        recordFailure_(std::current_exception());
    }
    // Finalize on the same I/O lane even after failure (to close every sink).
    io_job_->wait();
    io_phase_ = IOPhase::Close;
    io_job_->submit();
    io_job_->wait();
    if (output_error_) {
        // The caller has quiesced producers, as for a normal final drain.
        // Discard unread records so a later run cannot replay stale source IDs.
        ThreadContextManager::instance().flushAll();
        ThreadContextManager::instance().forEachContext([](ThreadContext* ctx) {
            auto& q = ctx->queue();
            while (auto* ptr = q.prepareRead())
                q.finishRead(reinterpret_cast<ObservationQueue::RecordHeader*>(ptr)->total_size);
            q.eagerCommitRead();
        });
        while (auto* ptr = queue_.prepareRead())
            queue_.finishRead(reinterpret_cast<ObservationQueue::RecordHeader*>(ptr)->total_size);
        queue_.forceCommitRead();
        reorder_buffer_.reset();
        ready_buffer_.clear();
        std::lock_guard<std::mutex> lock(timeline_submit_mutex_);
        submitted_timelines_.clear();
    }
    running_.store(false, std::memory_order_release);
}

void ObservationBackend::rethrowIfFailed() {
    std::lock_guard<std::mutex> lock(error_mutex_);
    if (output_error_) std::rethrow_exception(output_error_);
}

void ObservationBackend::recordFailure_(std::exception_ptr error) noexcept {
    {
        std::lock_guard<std::mutex> lock(error_mutex_);
        if (!output_error_) {
            output_error_ = error;
            try {
                std::rethrow_exception(error);
            } catch (const std::exception& e) {
                std::cerr << "[observe] output failed: " << e.what() << '\n';
            } catch (...) {
                std::cerr << "[observe] output failed with unknown exception\n";
            }
        }
    }
    // A failed consumer must not leave SpinWait producers waiting on a full queue.
    timeline_sink_open_.store(false, std::memory_order_release);
    ThreadContextManager::instance().setBackendWakeup(nullptr, nullptr);
    should_stop_.store(true, std::memory_order_release);
    wakeUp();
}

void ObservationBackend::wakeUp() noexcept {
    if (service_) service_->ready.store(true, std::memory_order_release);
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

void ObservationBackend::finalizeOutput_() {
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
}

void ObservationBackend::submitTimeline(TimelineStreamData&& data) {
    std::lock_guard<std::mutex> lock(timeline_submit_mutex_);
    submitted_timelines_.push_back(std::move(data));
}

void ObservationBackend::waitForAsyncIO_() {
    io_job_->wait();
    rethrowIfFailed();
}

void ObservationBackend::processEventsAsync_() {
    io_phase_ = IOPhase::Batch;
    io_in_flight_.store(true, std::memory_order_release);
    io_job_->submit();
}

void ObservationBackend::runIO_() noexcept {
    try {
        switch (io_phase_) {
            case IOPhase::Open: {
                initializeOutputDir_();
                prepareCounterSnapshotPlans_();
                reorder_buffer_.reset();
                if (config_.enable_reordering) {
                    ReorderBuffer::Config cfg;
                    cfg.watermark_cycles = config_.reorder_watermark_cycles;
                    cfg.max_buffer_events = config_.reorder_max_events;
                    cfg.initial_arena_size =
                        std::min(cfg.initial_arena_size, config_.service_buffer_bytes);
                    reorder_buffer_ = std::make_unique<ReorderBuffer>(cfg);
                }
                counter_buffer_.reserve(COUNTER_BUFFER_FLUSH_SIZE * 2);
                break;
            }
            case IOPhase::Batch:
                processServiceBatch_();
                break;
            case IOPhase::Close:
                rethrowIfFailed();
                if (reorder_buffer_) flushServiceReorder_(true);
                finalizeOutput_();
                break;
        }
    } catch (...) {
        recordFailure_(std::current_exception());
    }
    if (io_phase_ == IOPhase::Close || (io_phase_ == IOPhase::Open && output_error_)) {
        // A failure in one sink must not prevent the others from closing.
        const auto close = [this](auto&& action) {
            try {
                action();
            } catch (...) {
                recordFailure_(std::current_exception());
            }
        };
        close([&] {
            if (perfetto_writer_) perfetto_writer_->close();
        });
        timeline_sink_open_.store(false, std::memory_order_release);
        close([&] {
            if (counter_file_.is_open()) counter_file_.close();
        });
        close([&] {
            if (default_sink_ && default_sink_->file.is_open()) default_sink_->file.close();
        });
        for (auto& [name, sink] : custom_sinks_)
            close([&] {
                if (sink->file.is_open()) sink->file.close();
            });
    }
    io_in_flight_.store(false, std::memory_order_release);
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
