// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

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
class ClockTraceStream {
public:
    void record(uint64_t cycle, ClockEventKind kind, uint64_t transaction = 0, uint64_t value = 0,
                uint32_t fifo = 0, ClockEventPhase phase = ClockEventPhase::Evaluate);

private:
    friend class ClockTraceRecorder;
    ClockTraceStream(size_t capacity, bool lossless, std::atomic<bool>* failed);
    std::vector<ClockRecord> ring_;
    const bool lossless_;
    std::atomic<bool>* failed_;
    alignas(64) std::atomic<uint64_t> head_{0};
    alignas(64) std::atomic<uint64_t> tail_{0};
    uint64_t ordinal_ = 0;
    uint64_t dropped_ = 0;
    uint64_t peak_ = 0;
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
        uint64_t allocated_buffer_bytes = 0;
        uint64_t peak_buffer_bytes = 0;  // Sum of per-stream high water marks (upper bound).
        uint64_t file_bytes = 0;
    };

    explicit ClockTraceRecorder(Config config);
    ~ClockTraceRecorder();
    ClockTraceRecorder(const ClockTraceRecorder&) = delete;
    ClockTraceRecorder& operator=(const ClockTraceRecorder&) = delete;
    void defineEvent(ClockEventKind kind, std::string name);
    ClockTraceStream* addStream(const ClockDomain& domain, uint32_t unit_id, std::string unit_name);
    void start();
    /// Join/drain, then finalize Perfetto with a bounded-memory offline sort.
    /// Native Perfetto output is ready only after successful close().
    /// I/O errors are surfaced here and to blocked producers.
    void close();
    /// Valid after close(); includes metadata files and both enabled sinks.
    Stats stats() const;
    bool enabled() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace chronon::observe
