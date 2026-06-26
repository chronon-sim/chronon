// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string_view>

namespace chronon::observe {

/**
 * @brief Perfetto trace writer for the small TracePacket subset chronon needs.
 *
 * Thin wrapper over the Perfetto SDK's protozero message writers: TrackDescriptor
 * (process / plain / counter tracks) and TrackEvent (complete slices, instants,
 * counter samples). Output opens directly in ui.perfetto.dev and `trace_processor`.
 * The SDK is an implementation detail (pimpl) and does not leak into chronon
 * headers; no tracing session or category registration is involved — packets are
 * written straight to the file.
 *
 * Events are split across two packet sequences by time domain:
 * - Simulation domain (instant(), counterValue()): timestamps are cycles on a
 *   custom incremental clock (sequence-scoped clock id 64) declared via
 *   ClockSnapshot and TracePacketDefaults. The clock is paired 1:1 with the
 *   builtin boot clock, so the rendered axis is unchanged (1 cycle = 1 ns) but
 *   packets carry varint deltas instead of absolute timestamps. Out-of-order
 *   cycles fall back to an absolute boot-clock timestamp on that packet so the
 *   incremental state never corrupts.
 * - Wall-clock domain (sliceComplete(), wallInstant()): real nanoseconds since
 *   the recorder's base time on the default trace clock (used by the scheduler
 *   execution timeline). The `cycle` debug annotation carries the simulation
 *   cycle for slices where the two domains differ.
 *
 * Both sequences intern event names, categories, and debug-annotation names
 * (InternedData + SEQ_NEEDS_INCREMENTAL_STATE). Incremental state is
 * checkpointed (SEQ_INCREMENTAL_STATE_CLEARED + fresh clock snapshot) every
 * Options::checkpoint_interval_packets packets and whenever an intern table
 * reaches Options::max_interned_strings, which bounds reader memory and keeps
 * crash-truncated traces decodable.
 *
 * Packets buffer in memory and flush to disk past a threshold, so the file is
 * written incrementally and remains parseable after a crash (Perfetto traces
 * need no footer). With Options::compress, each flushed batch is wrapped in a
 * TracePacket.compressed_packets (zlib deflate) — handled natively by
 * ui.perfetto.dev and trace_processor. NOT thread-safe; intended for a single
 * backend thread.
 */
class PerfettoTraceWriter {
public:
    struct Options {
        /// Deflate each flushed packet batch into TracePacket.compressed_packets.
        bool compress = true;
        /// Re-emit incremental state (clock snapshot + intern tables) every N
        /// packets per sequence; bounds reader state and keeps traces seekable.
        size_t checkpoint_interval_packets = 65536;
        /// Per-table intern cap; reaching it forces a checkpoint on that sequence.
        size_t max_interned_strings = 65536;
    };

    /**
     * @brief Typed debug annotation (SDK-free view; names are interned).
     *
     * @c bits carries the value: zero-extended for Uint/Bool/Pointer,
     * bit-cast int64_t for Int, bit-cast double for Double.
     */
    struct Annotation {
        enum class Kind : uint8_t { Uint, Int, Double, Bool, Pointer, String };

        std::string_view name;
        Kind kind;
        uint64_t bits;
        std::string_view string = {};
    };

    PerfettoTraceWriter();
    ~PerfettoTraceWriter();

    PerfettoTraceWriter(const PerfettoTraceWriter&) = delete;
    PerfettoTraceWriter& operator=(const PerfettoTraceWriter&) = delete;

    /// Opens @p path for writing; sequence-start packets are emitted lazily.
    bool open(const std::filesystem::path& path, const Options& options);
    bool open(const std::filesystem::path& path) { return open(path, Options{}); }

    [[nodiscard]] bool isOpen() const noexcept;

    /// Writes buffered packets to the OS; does not close the file.
    void flush();

    void close();

    /// @return Track UUID for a process-scoped group track (ProcessDescriptor).
    uint64_t addProcessTrack(std::string_view process_name, int32_t pid);

    /// @return Track UUID for a named track, optionally nested under @p parent_uuid.
    uint64_t addTrack(std::string_view name, uint64_t parent_uuid = 0,
                      int32_t sibling_order_rank = -1);

    /// @return Track UUID for a counter track (rendered as a value graph).
    uint64_t addCounterTrack(std::string_view name, std::string_view unit_name,
                             uint64_t parent_uuid = 0, int32_t sibling_order_rank = -1);

    /**
     * @brief Emit a complete wall-clock slice (paired SLICE_BEGIN / SLICE_END packets).
     *
     * @p cycle is attached as a debug annotation; @p detail likewise when non-empty.
     */
    void sliceComplete(uint64_t track_uuid, std::string_view category, std::string_view name,
                       uint64_t ts_ns, uint64_t dur_ns, uint64_t cycle,
                       std::string_view detail = {});

    /// Emit an instant event at @p ts_ns on the wall-clock sequence.
    void wallInstant(uint64_t track_uuid, std::string_view category, std::string_view name,
                     uint64_t ts_ns);
    void wallInstant(uint64_t track_uuid, std::string_view category, std::string_view name,
                     uint64_t ts_ns, uint64_t cycle, std::string_view detail = {});

    /// Emit an instant event at @p cycle on the simulation cycle clock.
    void instant(uint64_t track_uuid, std::string_view category, std::string_view name,
                 uint64_t cycle);

    /// Instant with flow id (0 = none) and typed annotations (cycle clock).
    void instant(uint64_t track_uuid, std::string_view category, std::string_view name,
                 uint64_t cycle, uint64_t flow_id, std::span<const Annotation> annotations);

    /**
     * @brief Open a span at @p cycle on the simulation cycle clock.
     *
     * The matching sliceEnd() may come many cycles later; pairing is the
     * caller's responsibility (the backend's open-span table).
     */
    void sliceBegin(uint64_t track_uuid, std::string_view category, std::string_view name,
                    uint64_t cycle, uint64_t flow_id, std::span<const Annotation> annotations);

    /// Open a span with an explicit flow id; unlike sliceBegin(), flow id 0 is emitted.
    void sliceBeginWithFlow(uint64_t track_uuid, std::string_view category, std::string_view name,
                            uint64_t cycle, uint64_t flow_id,
                            std::span<const Annotation> annotations);

    /// Close the innermost open span on @p track_uuid at @p cycle.
    void sliceEnd(uint64_t track_uuid, uint64_t cycle);

    /// Emit a counter sample at @p cycle on a counter track.
    void counterValue(uint64_t track_uuid, uint64_t cycle, int64_t value);

    [[nodiscard]] uint64_t eventsWritten() const noexcept { return events_written_; }
    [[nodiscard]] uint64_t bytesWritten() const noexcept { return bytes_written_; }

private:
    struct Impl;

    /// Compresses one packet-aligned chunk into a compressed_packets wrapper
    /// (raw fallback on deflate failure).
    void writeChunk_(const uint8_t* data, size_t size);

    void sliceBeginImpl_(uint64_t track_uuid, std::string_view category, std::string_view name,
                         uint64_t cycle, uint64_t flow_id, bool has_flow_id,
                         std::span<const Annotation> annotations);

    std::unique_ptr<Impl> impl_;

    uint64_t next_uuid_ = 1;
    uint64_t events_written_ = 0;
    uint64_t bytes_written_ = 0;
};

}  // namespace chronon::observe
