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
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

// stdexec for unified stop propagation.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wextra-semi"
#include <stdexec/stop_token.hpp>
#pragma GCC diagnostic pop

#include "../chronon/HostServices.hpp"
#include "CounterSnapshot.hpp"
#include "DerivedCounter.hpp"
#include "FormatRegistry.hpp"
#include "ObservationQueue.hpp"
#include "ObservationYAMLConfig.hpp"
#include "PerfettoTraceWriter.hpp"
#include "ReorderBuffer.hpp"
#include "ThreadContextManager.hpp"
#include "TimelineApi.hpp"
#include "TimelineData.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Background worker that drains observability queues and writes output files.
 *
 * Drains per-thread SPSC queues (traces/logs) and the shared queue (counter
 * snapshots, lookahead commits), then routes each event to the text and/or
 * Perfetto timeline sinks.
 *
 * Output files:
 * - events.log       — text output (debug/info/warn/error, optional trace mirror).
 * - timeline.pftrace — Perfetto timeline (unit events, counter tracks, and the
 *                      scheduler execution timeline submitted at shutdown).
 * - counters.csv     — performance counter snapshots.
 */
class ObservationBackend : public HostService {
public:
    using EventHandler =
        std::function<void(ObservationQueue::EventType type, const std::byte* data, size_t size)>;

    struct Config {
        std::string output_dir = "out";
        std::chrono::microseconds poll_interval{100};
        bool enable_counter_csv = true;
        CounterCsvFormat counter_csv_format = CounterCsvFormat::Pivoted;

        std::string debug_file;
        std::string info_file;
        std::string warn_file;
        std::string error_file;

        bool timeline_enabled = true;
        std::string timeline_file = "timeline.pftrace";
        bool timeline_counters = true;
        bool timeline_compress = true;

        bool enable_reordering = true;
        uint64_t reorder_watermark_cycles = 1000;
        size_t reorder_max_events = 100000;

        std::string simulation_name;

        size_t service_buffer_bytes = 16 * 1024 * 1024;
    };

    explicit ObservationBackend(ObservationQueue& queue);
    explicit ObservationBackend(ObservationQueue& queue, const Config& config);
    ~ObservationBackend();

    ObservationBackend(const ObservationBackend&) = delete;
    ObservationBackend& operator=(const ObservationBackend&) = delete;

    void start();

    /// Register before start(), with simulation workers quiescent.
    void attachScheduler(HostServices& scheduler);
    size_t poll(size_t record_budget) noexcept override;
    HostServiceRegistration::Stats serviceStats() const {
        return service_ ? service_->stats() : HostServiceRegistration::Stats{};
    }

    /// Drains remaining events before stopping; discards them if output failed.
    /// PRECONDITION: producers have stopped submitting records.
    void stop() noexcept;

    /// Rethrow the first worker/output failure, retained until the next start().
    /// Thread-safe. Call after stop() to include final flush/close failures.
    void rethrowIfFailed();

    /**
     * @brief Set an optional observation-lifetime stop token.
     *
     * Do not pass the simulation worker token: final records are submitted
     * after workers terminate. PRECONDITION: must be called before start().
     */
    void setStopToken(stdexec::inplace_stop_token token) noexcept { stop_token_ = token; }

    /// Publish ingress readiness. Safe from any producer.
    void wakeUp() noexcept;

    bool isRunning() const noexcept { return running_.load(std::memory_order_relaxed); }

    const std::filesystem::path& outputDir() const noexcept { return output_dir_; }

    /// True when this backend has an open Perfetto timeline sink. Reports false
    /// when the timeline is disabled by config or opening/writing failed, so
    /// callers (scheduler timeline handoff) can fall back to standalone output.
    bool timelineEnabled() const noexcept {
        return timeline_sink_open_.load(std::memory_order_acquire);
    }

    /**
     * @brief Submit recorded timeline streams for inclusion in timeline.pftrace.
     *
     * Thread-safe; intended for end-of-run handoff (e.g. the scheduler execution
     * timeline). The data is written by the worker thread during stop(), after
     * the final event drain, so callers must submit before stopping the backend.
     */
    void submitTimeline(TimelineStreamData&& data);

    void setLogHandler(EventHandler handler) { log_handler_ = std::move(handler); }

    /// Called by ObservationManager before start(). Definitions are resolved to
    /// column indices once raw counter columns are finalized.
    void setDerivedCounterDefs(std::vector<DerivedCounterDef> defs);

    /// Register immutable counter names once; periodic records then carry only values.
    /// PRECONDITION: called before start().
    void setCounterSnapshotPlans(std::vector<CounterSnapshotPlanMetadata> plans);

    /// Register the complete raw counter schema, including contexts that only
    /// contribute legacy/final snapshots and therefore have no periodic plan.
    /// PRECONDITION: called before start().
    void setCounterColumns(std::vector<CounterSnapshotEntryMetadata> columns);

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
    }

    uint64_t eventsProcessed() const noexcept {
        return events_processed_.load(std::memory_order_relaxed);
    }

    uint64_t bytesWritten() const noexcept {
        return bytes_written_.load(std::memory_order_relaxed);
    }

private:
    friend struct ObservationBackendTestAccess;

    enum class Channel : uint8_t { Debug = 0, Info = 1, Warn = 2, Error = 3, Count = 4 };

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

    void recordFailure_(std::exception_ptr error) noexcept;
    void runIO_() noexcept;
    void finalizeOutput_();
    size_t drainServiceBatch_(size_t record_budget);
    void processServiceBatch_();
    void flushServiceReorder_(bool all);
    void processEventsAsync_();
    void waitForAsyncIO_();
    void processEvent_(const ObservationQueue::RecordHeader* header, const std::byte* data);
    void processCounterSample_(uint64_t cycle, std::string_view unit_name,
                               std::string_view counter_name, uint64_t value, bool want_timeline,
                               size_t column_index = SIZE_MAX);
    void prepareCounterSnapshotPlans_();
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
    void finalizeTimeline_();

    void writeEventAsText_(const StructuredRecord* rec, const std::byte* args_data,
                           size_t args_size, const char* level_str, LogFileSink& sink);

    /// Emit a counter snapshot sample on its (lazily created) counter track.
    void writeCounterToTimeline_(uint64_t cycle, std::string_view unit_name,
                                 std::string_view counter_name, uint64_t value);

    /// Track for @p source_id under the simulation process group (lazily created).
    uint64_t timelineTrackForSource_(uint16_t source_id);

    /// Track for a hierarchical unit path ("cpu0.lsu" → group "cpu0" → track
    /// "lsu"), creating parent group tracks as needed. The unit tree is
    /// mirrored into Perfetto's parent_uuid chain so the UI sidebar follows
    /// the design hierarchy.
    uint64_t timelineTrackForPath_(std::string_view path);
    uint64_t timelineTrackForPath_(std::string_view path, int32_t sibling_order_rank);

    /// Create source tracks in registry order once the timeline writer opens so
    /// Perfetto's explicit sibling ranks match YAML unit order, not event arrival
    /// or lexicographic UI sorting.
    void predeclareTimelineSourceTracks_();

    /// Route a TimelineRecord (span begin/end or lane instant)
    /// to the Perfetto timeline, maintaining the open-span table.
    void processTimelineEvent_(const std::byte* data, size_t data_size);

    /// Perfetto track for (timeline track id, slot), lazily created under the
    /// owning unit's track: single track, lane group + per-slot child, or
    /// lane track selected by the declaration.
    uint64_t timelineSlotTrack_(uint32_t track_id, uint16_t slot);

    /// Lazily cached registry lookups (string_views point at stable deque storage).
    std::string_view timelineEventName_(uint16_t name_id);
    std::string_view timelineAnnotationKey_(uint16_t key_id);
    /// Perfetto category string for a record's category bit (0..63), or
    /// "timeline" for TIMELINE_NO_CATEGORY / unregistered bits.
    std::string_view timelineCategoryName_(uint8_t category_bit);

    struct PipelineSliceNames {
        std::string category;
        std::string event_name;
    };

    const PipelineSliceNames& pipelineSliceNames_(uint64_t payload, bool hex_name);

    static constexpr size_t PIPELINE_SLICE_NAME_CACHE_MAX_ENTRIES = 65536;

    ObservationQueue& queue_;
    Config config_;

    std::filesystem::path output_dir_;

    std::ofstream counter_file_;

    std::unique_ptr<LogFileSink> default_sink_;
    std::array<LogFileSink*, static_cast<size_t>(Channel::Count)> channel_sink_{};
    std::unordered_map<std::string, std::unique_ptr<LogFileSink>> custom_sinks_;

    std::array<std::unordered_map<uint64_t, PipelineSliceNames>, 2> pipeline_slice_name_cache_;
    PipelineSliceNames pipeline_slice_name_scratch_;

    std::unique_ptr<HostServices> standalone_scheduler_;
    std::shared_ptr<HostServiceRegistration> service_;
    static constexpr size_t SERVICE_BATCH_BYTES = 256 * 1024;
    std::vector<std::byte> service_batch_;
    size_t service_batch_size_ = 0;
    size_t service_queue_cursor_ = 0;
    uint64_t service_min_cycle_ = 0;
    std::unique_ptr<HostIOJob> io_job_;
    enum class IOPhase { Open, Batch, Close };
    IOPhase io_phase_ = IOPhase::Open;
    std::atomic<bool> running_{false};
    std::atomic<bool> should_stop_{false};

    stdexec::inplace_stop_token stop_token_{};

    // Buffer writes always happen; file.flush() only runs every OS_FLUSH_INTERVAL
    // to reduce syscall overhead.
    static constexpr std::chrono::milliseconds OS_FLUSH_INTERVAL{50};
    std::chrono::steady_clock::time_point last_os_flush_time_{};

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

    std::unique_ptr<PerfettoTraceWriter> perfetto_writer_;
    /// Set once the timeline file is open (in start(), before threads spawn);
    /// cleared when the writer closes at shutdown.
    std::atomic<bool> timeline_sink_open_{false};
    uint64_t sim_process_uuid_ = 0;
    /// source_id → timeline track uuid (0 = not yet created).
    std::vector<uint64_t> source_track_uuids_;
    /// Hierarchical path → track uuid (units and their parent group tracks).
    std::unordered_map<std::string, uint64_t> timeline_path_uuids_;
    /// Pull-model counter unit path → synthetic counter group uuid.
    std::unordered_map<std::string, uint64_t> counter_group_uuids_;
    /// "unit.counter" → counter track uuid.
    std::unordered_map<std::string, uint64_t> counter_track_uuids_;
    fmt::memory_buffer timeline_msg_buffer_;

    /// @brief Lazily created Perfetto tracks for one declared timeline track.
    struct TimelineTrackUuids {
        uint64_t single = 0;          ///< Lane (lanes <= 1) or counter track.
        uint64_t group = 0;           ///< Lane group (lanes > 1).
        std::vector<uint64_t> slots;  ///< Per-slot child tracks (lanes > 1).
    };
    /// timeline track id → created uuids (index 0 unused; ids are 1-based).
    std::vector<TimelineTrackUuids> timeline_track_uuids_;
    /// Open spans keyed by (track_id << 16 | slot) → slot track uuid.
    std::unordered_map<uint64_t, uint64_t> open_spans_;
    /// Highest timeline-event cycle seen; used to close dangling spans at shutdown.
    uint64_t timeline_max_cycle_ = 0;
    /// Lazy id → name caches over the global registries.
    std::vector<std::string_view> timeline_event_name_cache_;
    std::vector<std::string_view> timeline_annotation_key_cache_;
    /// Category bit (0..63) → registered category name (empty = unresolved).
    std::array<std::string_view, 64> timeline_category_names_{};

    /// Timeline streams handed over via submitTimeline(); written at shutdown.
    std::mutex timeline_submit_mutex_;
    std::vector<TimelineStreamData> submitted_timelines_;

    std::vector<BufferedRecord> ready_buffer_;

    std::atomic<bool> io_in_flight_{false};
    std::mutex error_mutex_;
    std::exception_ptr output_error_{};

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

    std::vector<CounterSnapshotPlanMetadata> counter_snapshot_plans_;
    std::vector<std::vector<size_t>> counter_snapshot_column_indices_;
    std::vector<CounterSnapshotEntryMetadata> counter_column_metadata_;

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
