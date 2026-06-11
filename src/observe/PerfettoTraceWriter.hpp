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
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

namespace chronon::observe {

/**
 * @brief Minimal Perfetto protobuf trace writer (no Perfetto SDK dependency).
 *
 * Hand-encodes the protobuf wire format for the small TracePacket subset chronon
 * needs: TrackDescriptor (process / plain / counter tracks) and TrackEvent
 * (complete slices, instants, counter samples). Output opens directly in
 * ui.perfetto.dev and `trace_processor`.
 *
 * Timestamps are nanoseconds on the default trace clock. Simulation-domain
 * events use timestamp = cycle (1 cycle rendered as 1 ns); wall-clock-domain
 * events use real nanoseconds since their recorder's base time. The `cycle`
 * debug annotation carries the simulation cycle for slices where the two differ.
 *
 * Packets buffer in memory and flush to disk past a threshold, so the file is
 * written incrementally and remains parseable after a crash (Perfetto traces
 * need no footer). NOT thread-safe; intended for a single backend thread.
 */
class PerfettoTraceWriter {
public:
    PerfettoTraceWriter() = default;
    ~PerfettoTraceWriter();

    PerfettoTraceWriter(const PerfettoTraceWriter&) = delete;
    PerfettoTraceWriter& operator=(const PerfettoTraceWriter&) = delete;

    /// Opens @p path for writing; emits the sequence-start packet.
    bool open(const std::filesystem::path& path);

    [[nodiscard]] bool isOpen() const noexcept { return file_.is_open(); }

    /// Writes buffered packets to the OS; does not close the file.
    void flush();

    void close();

    /// @return Track UUID for a process-scoped group track (ProcessDescriptor).
    uint64_t addProcessTrack(std::string_view process_name, int32_t pid);

    /// @return Track UUID for a named track, optionally nested under @p parent_uuid.
    uint64_t addTrack(std::string_view name, uint64_t parent_uuid = 0);

    /// @return Track UUID for a counter track (rendered as a value graph).
    uint64_t addCounterTrack(std::string_view name, std::string_view unit_name,
                             uint64_t parent_uuid = 0);

    /**
     * @brief Emit a complete slice (paired SLICE_BEGIN / SLICE_END packets).
     *
     * @p cycle is attached as a debug annotation; @p detail likewise when non-empty.
     */
    void sliceComplete(uint64_t track_uuid, std::string_view category, std::string_view name,
                       uint64_t ts_ns, uint64_t dur_ns, uint64_t cycle,
                       std::string_view detail = {});

    /// Emit an instant event at @p ts_ns.
    void instant(uint64_t track_uuid, std::string_view category, std::string_view name,
                 uint64_t ts_ns);

    /// Emit a counter sample on a counter track.
    void counterValue(uint64_t track_uuid, uint64_t ts_ns, int64_t value);

    [[nodiscard]] uint64_t eventsWritten() const noexcept { return events_written_; }
    [[nodiscard]] uint64_t bytesWritten() const noexcept { return bytes_written_; }

private:
    // --- protobuf wire-format helpers (wire type 0 = varint, 2 = length-delimited) ---
    static void appendVarint(std::string& buf, uint64_t value);
    static void appendTag(std::string& buf, uint32_t field, uint32_t wire_type);
    static void appendVarintField(std::string& buf, uint32_t field, uint64_t value);
    static void appendStringField(std::string& buf, uint32_t field, std::string_view value);
    /// Appends @p sub as a length-delimited sub-message field.
    static void appendMessageField(std::string& buf, uint32_t field, const std::string& sub);

    /// Wraps @p packet as `Trace.packet` (field 1) into the output buffer.
    void commitPacket_();

    /// Starts a fresh packet in pkt_ with sequence id (+ sequence-start flags once).
    void beginPacket_();

    void emitTrackDescriptor_(const std::string& descriptor);

    void maybeFlush_();

    std::ofstream file_;
    std::string out_buf_;  ///< Encoded packets pending fwrite.
    std::string pkt_;      ///< Scratch: current TracePacket body.
    std::string msg_;      ///< Scratch: nested message (TrackEvent / TrackDescriptor).
    std::string sub_;      ///< Scratch: doubly-nested message (descriptors, annotations).

    uint64_t next_uuid_ = 1;
    bool first_packet_ = true;
    uint64_t events_written_ = 0;
    uint64_t bytes_written_ = 0;

    static constexpr uint32_t SEQUENCE_ID = 1;
    static constexpr size_t FLUSH_THRESHOLD = size_t{256} * 1024;
};

}  // namespace chronon::observe
