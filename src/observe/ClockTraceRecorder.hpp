// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "../chronon/HostServices.hpp"
#include "../time/ClockDomain.hpp"
#include "PerfettoTraceWriter.hpp"

namespace chronon::observe {

enum class ClockEventKind : uint16_t {
    Write = 1,
    Visible,
    Read,
    Output,
    Consume,
    Full,
    Empty,
    User = 256
};
enum class ClockEventPhase : uint8_t { Evaluate = 0, Commit = 1 };

/// Compact owned record: neither pointers to payloads nor deferred callbacks.
struct ClockRecord {
    uint64_t local_cycle = 0;
    uint64_t transaction_id = 0;
    uint64_t value = 0;
    uint64_t ordinal = 0;
    uint32_t fifo_id = 0;
    ClockEventKind kind = ClockEventKind::User;
    ClockEventPhase phase = ClockEventPhase::Evaluate;
};
static_assert(sizeof(ClockRecord) == 40);

/// One producer at a time per stream, one backend consumer. Different streams
/// may be written concurrently, including several streams in the same domain.
class ClockTraceRecorder;

class ClockTraceStream {
public:
    void record(uint64_t cycle, ClockEventKind kind, uint64_t transaction = 0, uint64_t value = 0,
                uint32_t fifo = 0, ClockEventPhase phase = ClockEventPhase::Evaluate);
    /// Promise no more records below next_cycle, then wait for the global safe
    /// bucket frontier. Call in bounded batches from independent producer threads.
    /// Quiet streams must also advance, or finish when permanently done.
    void advance(uint64_t next_cycle);
    void finish();
    /// Scheduler actor boundary: publish non-lossy gap metadata before progress.
    void endEdge();

private:
    friend class ClockTraceRecorder;
    friend struct ClockTraceStreamTestAccess;
    ClockTraceRecorder* coordinator_ = nullptr;
    HostServiceRegistration* service_ = nullptr;
    size_t record_base_bytes_ = 0;
    ClockTraceStream(size_t capacity, bool lossless, bool perfetto, ClockDomain clock,
                     std::atomic<bool>* failed);
    std::vector<ClockRecord> ring_;
    const bool lossless_;
    std::atomic<bool>* failed_;
    const bool perfetto_;
    const ClockDomain clock_;
    std::atomic<uint64_t> watermark_{0}, acknowledged_{0};
    std::atomic<bool> finished_{false};
    uint64_t minimum_cycle_ = 0;
    alignas(64) std::atomic<uint64_t> head_{0};
    alignas(64) std::atomic<uint64_t> tail_{0};
    uint64_t ordinal_ = 0;
    uint64_t dropped_ = 0;
    uint64_t peak_ = 0;
    uint64_t stalls_ = 0, stall_ns_ = 0;
    bool parallel_ = false;
    ClockRecord dropped_run_{};  // value = count; phase high bit marks gap metadata.
};

class ClockTraceRecorder {
public:
    struct Config {
        std::filesystem::path output_dir = "out/clocks";
        std::string run_id = "chronon";
        bool text = true;
        bool perfetto = true;
        bool lossless = true;
        size_t stream_capacity = 4096;
        size_t drain_batch = 256;
        bool reverse_drain = false;
        PerfettoTraceWriter::Options perfetto_options;
    };
    struct Stats {
        uint64_t events = 0;
        uint64_t dropped = 0;
        uint64_t producer_stalls = 0, producer_stall_ns = 0;
        uint64_t admission_retries = 0;
        uint64_t progress_stalls = 0, progress_stall_ns = 0;
        uint64_t allocated_buffer_bytes = 0;
        uint64_t peak_buffer_bytes = 0;  // Sum of per-stream high water marks (upper bound).
        uint64_t file_bytes = 0;
        uint64_t native_buffer_peak_bytes = 0;
        uint64_t native_buffer_peak_records = 0;
        uint64_t first_output_ns = 0;          // Host time since native writer open().
        uint64_t allocated_staging_bytes = 0;  // Record + bucket descriptor array allocation bytes.
        uint64_t peak_staging_records = 0;
        uint64_t service_calls = 0, service_records = 0, service_ns = 0, service_max_poll_ns = 0;
    };

    explicit ClockTraceRecorder(Config config);
    ~ClockTraceRecorder();
    ClockTraceRecorder(const ClockTraceRecorder&) = delete;
    ClockTraceRecorder& operator=(const ClockTraceRecorder&) = delete;
    void defineEvent(ClockEventKind kind, std::string name);
    void attachScheduler(HostServices& scheduler);
    ClockTraceStream* addStream(const ClockDomain& domain, uint32_t unit_id, std::string unit_name);
    /// Independent actor, same logical unit/track. Stable nonzero producer_order
    /// breaks commit ties (CDC uses FIFO ID + 1), never a host worker identity.
    ClockTraceStream* addProducerStream(ClockTraceStream* unit, uint64_t producer_order);
    /// Serial coordinator mode requires begin/endClockBatch around all producers.
    /// All records must belong to that exact batch time; do not advance/finish
    /// individual streams. Leave disabled for independent worker streams.
    void start(bool serial_coordinator = false);
    /// Scheduler-owned, nonblocking observation credits. Compact staging for
    /// admitted ns buckets lets the backend drain EVERY ingress stream without
    /// waiting for simulation progress, including several actors on one worker.
    void startParallel(size_t lookahead_batches);
    bool parallelActive() const noexcept;
    /// Single scheduler coordinator only; false means retry after backend drain.
    bool tryAdmitClockBatch(const SimTime& time);
    /// All records below this bound have been published (units AND CDC commits).
    /// Does not wait for the backend; quiet actors need no per-stream advance.
    void publishClockProgress(uint64_t exclusive_ns);
    void beginClockBatch(const SimTime& time);
    void endClockBatch();
    /// Coordinator-only alternative to per-stream advance: ALL producers must
    /// have published every event with floor(time/ns) < exclusive_ns before this
    /// call, and promise never to publish another. Waits for backend acknowledgement.
    /// Use at scheduler safe points, not sequential blocking per-stream advances.
    void advance(uint64_t exclusive_ns);
    bool needsProgress() const noexcept;
    /// Join/drain and finish the open tail buckets. Encoded prefixes are already
    /// available during recording; no whole-trace sorting or temporary disk IO.
    /// I/O errors are surfaced here and to blocked producers.
    void close();
    /// Valid after close(); includes metadata files and both enabled sinks.
    Stats stats() const;
    bool enabled() const noexcept;

private:
    friend class ClockTraceStream;
    void reserveClockRecord(size_t base_bytes, ClockEventKind kind);
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace chronon::observe
