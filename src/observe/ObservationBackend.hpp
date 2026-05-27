// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <fmt/format.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// stdexec for unified stop propagation.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wextra-semi"
#include <stdexec/stop_token.hpp>
#pragma GCC diagnostic pop

#include "BinaryTraceWriter.hpp"
#include "DerivedCounter.hpp"
#include "FormatRegistry.hpp"
#include "ObservationQueue.hpp"
#include "ObservationYAMLConfig.hpp"
#include "ReorderBuffer.hpp"
#include "ThreadContextManager.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Background worker that drains observability queues and writes output files.
 *
 * Drains per-thread SPSC queues (traces/logs) and the shared queue (counter
 * snapshots, lookahead commits), then routes each event to text and/or binary
 * sinks based on per-channel format configuration.
 *
 * Output files:
 * - events.log    — text output (channels with format=Text or Both).
 * - events.ctrace — binary output (channels with format=Binary or Both).
 * - counters.csv  — performance counter snapshots.
 */
class ObservationBackend {
public:
    using EventHandler =
        std::function<void(ObservationQueue::EventType type, const std::byte* data, size_t size)>;

    /**
     * @brief Per-category, per-channel format overrides.
     *
     * When set, the channel-specific override takes priority over the per-channel default.
     */
    struct CategoryFormatOverride {
        std::optional<OutputFormat> trace_format;
        std::optional<OutputFormat> debug_format;
        std::optional<OutputFormat> info_format;
        std::optional<OutputFormat> warn_format;
        std::optional<OutputFormat> error_format;

        /// Check if any channel override matches the target format (or Both).
        [[nodiscard]] bool hasFormat(OutputFormat target) const noexcept {
            for (const auto* fmt :
                 {&trace_format, &debug_format, &info_format, &warn_format, &error_format}) {
                if (*fmt == target || *fmt == OutputFormat::Both) {
                    return true;
                }
            }
            return false;
        }
    };

    struct Config {
        std::string output_dir = "out";
        std::chrono::microseconds poll_interval{100};
        bool enable_counter_csv = true;
        CounterCsvFormat counter_csv_format = CounterCsvFormat::Pivoted;

        OutputFormat trace_format = OutputFormat::Binary;
        OutputFormat debug_format = OutputFormat::Text;
        OutputFormat info_format = OutputFormat::Text;
        OutputFormat warn_format = OutputFormat::Text;
        OutputFormat error_format = OutputFormat::Text;

        std::string trace_file;
        std::string debug_file;
        std::string info_file;
        std::string warn_file;
        std::string error_file;

        std::unordered_map<CategoryMask, CategoryFormatOverride> category_format_overrides;

        TraceChannelConfig trace_config;

        bool enable_reordering = true;
        uint64_t reorder_watermark_cycles = 1000;
        size_t reorder_max_events = 100000;

        std::string simulation_name;

        [[nodiscard]] bool needsTextOutput() const noexcept {
            return hasFormat_(OutputFormat::Text);
        }

        [[nodiscard]] bool needsBinaryOutput() const noexcept {
            return hasFormat_(OutputFormat::Binary);
        }

    private:
        /// Check if any channel (direct or override) uses the target format (or Both).
        [[nodiscard]] bool hasFormat_(OutputFormat target) const noexcept {
            for (OutputFormat fmt :
                 {trace_format, debug_format, info_format, warn_format, error_format}) {
                if (fmt == target || fmt == OutputFormat::Both) {
                    return true;
                }
            }
            for (const auto& [mask, ovr] : category_format_overrides) {
                if (ovr.hasFormat(target)) {
                    return true;
                }
            }
            return false;
        }
    };

    explicit ObservationBackend(ObservationQueue& queue);
    explicit ObservationBackend(ObservationQueue& queue, const Config& config);
    ~ObservationBackend();

    ObservationBackend(const ObservationBackend&) = delete;
    ObservationBackend& operator=(const ObservationBackend&) = delete;

    void start();

    /// Drains remaining events before stopping.
    void stop() noexcept;

    /**
     * @brief Hook a stop token for unified stop propagation.
     *
     * The backend installs a stop_callback that sets should_stop_ and wakes
     * the worker when stop is requested externally (simulation termination,
     * exception). PRECONDITION: must be called before start().
     */
    void setStopToken(stdexec::inplace_stop_token token) noexcept { stop_token_ = token; }

    /// Set the wake flag; the spin-wait loop picks it up. Safe from any producer.
    void wakeUp() noexcept;

    bool isRunning() const noexcept { return running_.load(std::memory_order_relaxed); }

    const std::filesystem::path& outputDir() const noexcept { return output_dir_; }

    void setTraceHandler(EventHandler handler) { trace_handler_ = std::move(handler); }
    void setLogHandler(EventHandler handler) { log_handler_ = std::move(handler); }

    /// Called by ObservationManager before start(). Definitions are resolved to
    /// column indices once raw counter columns are finalized.
    void setDerivedCounterDefs(std::vector<DerivedCounterDef> defs);

    void setSourceNameLookup(std::function<std::string_view(uint16_t)> lookup) noexcept {
        source_name_lookup_ = std::move(lookup);
        // Pre-populate a flat cache so the hot path avoids std::function dispatch.
        // Source IDs are 1-based and sequential; probe until an empty name is returned.
        source_name_cache_.clear();
        source_name_cache_.emplace_back();
        for (uint16_t id = 1; id < UINT16_MAX; ++id) {
            auto name = source_name_lookup_(id);
            if (name.empty()) {
                break;
            }
            source_name_cache_.emplace_back(name);
        }
        if (binary_trace_writer_) {
            binary_trace_writer_->setSourceNameLookup(source_name_lookup_);
        }
    }

    void setUnitTypeLookup(std::function<std::string_view(uint16_t)> lookup) noexcept {
        unit_type_lookup_ = std::move(lookup);
        if (binary_trace_writer_) {
            binary_trace_writer_->setUnitTypeLookup(lookup);
        }
    }

    uint64_t eventsProcessed() const noexcept {
        return events_processed_.load(std::memory_order_relaxed);
    }

    uint64_t bytesWritten() const noexcept {
        return bytes_written_.load(std::memory_order_relaxed);
    }

private:
    enum class Channel : uint8_t { Trace = 0, Debug = 1, Info = 2, Warn = 3, Error = 4, Count = 5 };

    /// @brief Per-channel file sink: owns an ofstream and a write buffer.
    struct LogFileSink {
        std::ofstream file;
        fmt::memory_buffer buffer;

        void flush() {
            if (buffer.size() > 0 && file.is_open()) {
                file.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
                buffer.clear();
            }
        }

        void osFlush() {
            if (file.is_open()) {
                file.flush();
            }
        }
    };

    void run_();
    size_t drainQueue_();
    size_t drainPerThreadQueues_();
    size_t drainAllQueues_();
    size_t drainToReorderBuffer_();
    void processReorderBuffer_(bool flush_all);
    void processEventsAsync_();
    void ioWorkerLoop_();
    void waitForAsyncIO_();
    void processEvent_(const ObservationQueue::RecordHeader* header, const std::byte* data);
    void processStructuredTrace_(const std::byte* data, size_t data_size);
    void processStructuredLog_(const std::byte* data, size_t data_size);
    std::string reconstructMessage_(const FormatInfo& fmt_info, const StructuredRecord* rec,
                                    const std::byte* args_data, size_t args_size);
    void reconstructMessageTo_(fmt::memory_buffer& out, const FormatInfo& fmt_info,
                               const StructuredRecord* rec, const std::byte* args_data,
                               size_t args_size);
    std::string formatArg_(const std::byte* data, ArgType type, std::string_view spec);
    void formatArgTo_(fmt::memory_buffer& out, const std::byte* data, ArgType type,
                      std::string_view spec);
    void formatArgToFast_(fmt::memory_buffer& out, const std::byte* data, ArgType type, bool hex);
    size_t argSize_(ArgType type, const std::byte* data, const std::byte* end);
    void flush_();
    void flushAllTextBuffers_();
    void flushCounterBuffer_();
    void finalizeCounterColumns_();
    void writeCounterCsvHeader_();
    void flushCounterRow_(uint64_t cycle);
    void finalizeCounterCsv_();
    void resolveDerivedCounters_();
    void computeDerived_();
    void emitLongDerivedValues_(uint64_t cycle);
    void initializeOutputDir_();

    void writeEventAsText_(const StructuredRecord* rec, const std::byte* args_data,
                           size_t args_size, const char* level_str, LogFileSink& sink);
    OutputFormat resolveTraceFormat_(CategoryMask event_category) const;
    OutputFormat resolveLogFormat_(CategoryMask log_category) const;

    ObservationQueue& queue_;
    Config config_;

    std::filesystem::path output_dir_;

    std::ofstream counter_file_;

    std::unique_ptr<LogFileSink> default_sink_;
    std::array<LogFileSink*, static_cast<size_t>(Channel::Count)> channel_sink_{};
    std::unordered_map<std::string, std::unique_ptr<LogFileSink>> custom_sinks_;

    std::thread worker_thread_;
    std::thread io_worker_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> should_stop_{false};

    stdexec::inplace_stop_token stop_token_{};

    // Drain thread wakeup: atomic flag for lock-free spin checks plus mutex+condvar for
    // blocking. Can't use atomic::wait because GCC 12's implementation uses a shared
    // 16-bucket proxy pool for non-trivially-waitable types (sizeof(bool) != sizeof(int)),
    // which causes missed wakeups when other atomics hash to the same bucket.
    std::atomic<bool> wake_flag_{false};
    std::mutex wake_mutex_;
    std::condition_variable wake_cv_;

    static constexpr int SPIN_HOT_ITERS = 256;
    static constexpr int SPIN_YIELD_ITERS = 64;
    static constexpr std::chrono::microseconds SPIN_SLEEP_US{50};

    // Buffer writes always happen; file.flush() only runs every OS_FLUSH_INTERVAL
    // to reduce syscall overhead.
    static constexpr std::chrono::milliseconds OS_FLUSH_INTERVAL{50};
    std::chrono::steady_clock::time_point last_os_flush_time_{};

    EventHandler trace_handler_;
    EventHandler log_handler_;

    std::atomic<uint64_t> events_processed_{0};
    std::atomic<uint64_t> bytes_written_{0};
    uint64_t local_bytes_written_ = 0;  ///< Accumulated locally; flushed to atomic in flush_().

    std::unique_ptr<ReorderBuffer> reorder_buffer_;

    std::vector<uint64_t> per_queue_max_cycle_;

    std::function<std::string_view(uint16_t)> source_name_lookup_;
    /// Flat cache populated eagerly in setSourceNameLookup() so the hot path
    /// avoids std::function dispatch per event.
    std::vector<std::string> source_name_cache_;

    std::function<std::string_view(uint16_t)> unit_type_lookup_;

    std::unique_ptr<BinaryTraceWriter> binary_trace_writer_;

    std::vector<BufferedRecord> ready_buffer_;

    std::vector<BufferedRecord> io_buffer_;
    ArenaSnapshot io_arena_;
    std::atomic<bool> io_in_flight_{false};
    std::mutex io_dispatch_mutex_;
    std::condition_variable io_dispatch_cv_;
    bool io_work_ready_ = false;
    bool io_worker_stop_ = false;
    std::mutex io_wait_mutex_;
    std::condition_variable io_wait_cv_;
    std::exception_ptr io_async_error_{};
    uint32_t io_wait_timeout_count_ = 0;
    static constexpr std::chrono::milliseconds ASYNC_IO_WAIT_TIMEOUT{250};

    static constexpr size_t TEXT_BUFFER_FLUSH_SIZE = 1024 * 1024;

    fmt::memory_buffer counter_buffer_;
    static constexpr size_t COUNTER_BUFFER_FLUSH_SIZE = 64 * 1024;

    // Pivoted CSV state (rows=cycles, cols=counters). The first batch is buffered
    // so we can discover all column names before writing the header; subsequent
    // batches stream as single rows.
    bool counter_csv_streaming_ = false;
    std::vector<std::string> counter_columns_;
    std::unordered_map<std::string, size_t> counter_col_index_;
    uint64_t current_counter_cycle_ = UINT64_MAX;
    std::vector<uint64_t> current_counter_row_;
    std::vector<std::pair<std::string, uint64_t>> counter_first_batch_;
    uint64_t counter_first_cycle_ = UINT64_MAX;

    std::vector<DerivedCounterDef> derived_counter_defs_;

    /// @brief Derived counter resolved against finalized counter columns.
    struct ResolvedDerived {
        std::string column_name;
        std::vector<size_t> source_cols;
        ComputeFn compute;
    };
    std::vector<ResolvedDerived> resolved_derived_;
    std::vector<double> current_derived_row_;

    uint64_t long_current_cycle_ = UINT64_MAX;
    std::unordered_map<std::string, uint64_t> long_cycle_values_;
};

}  // namespace chronon::observe
