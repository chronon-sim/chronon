// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file PftraceTestDecoder.hpp
/// @brief Independent Perfetto wire-format decoder for tests: inflates
///        compressed_packets batches, resolves interned strings per sequence,
///        replays incremental clock state, and surfaces track events with
///        flows and typed debug annotations. Deliberately shares no code with
///        PerfettoTraceWriter.

#pragma once

#include <zlib.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <vector>

// Always-evaluated check: survives NDEBUG builds.
#ifndef CHECK
#define CHECK(cond)                                                                         \
    do {                                                                                    \
        if (!(cond)) {                                                                      \
            std::cerr << "CHECK failed at " << __FILE__ << ":" << __LINE__ << ": " << #cond \
                      << "\n";                                                              \
            std::abort();                                                                   \
        }                                                                                   \
    } while (0)
#endif

namespace pftrace_test {

// ---------------------------------------------------------------------------
// Minimal independent protobuf wire-format decoder
// ---------------------------------------------------------------------------

struct Decoder {
    const uint8_t* p;
    const uint8_t* end;

    bool done() const { return p >= end; }

    uint64_t varint() {
        uint64_t value = 0;
        int shift = 0;
        while (p < end) {
            uint8_t byte = *p++;
            value |= static_cast<uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) return value;
            shift += 7;
            CHECK(shift < 64 && "varint too long");
        }
        CHECK(false && "truncated varint");
        return value;
    }

    uint64_t fixed64() {
        CHECK(p + 8 <= end && "truncated fixed64");
        uint64_t value = 0;
        std::memcpy(&value, p, 8);  // Wire format is little-endian, as is the host.
        p += 8;
        return value;
    }

    // Returns {field_number, wire_type}.
    std::pair<uint32_t, uint32_t> tag() {
        uint64_t t = varint();
        return {static_cast<uint32_t>(t >> 3), static_cast<uint32_t>(t & 0x7)};
    }

    Decoder lenDelimited() {
        uint64_t len = varint();
        CHECK(p + len <= end && "length-delimited field overruns buffer");
        Decoder sub{p, p + len};
        p += len;
        return sub;
    }

    std::string str() {
        Decoder sub = lenDelimited();
        return std::string(reinterpret_cast<const char*>(sub.p),
                           static_cast<size_t>(sub.end - sub.p));
    }

    void skip(uint32_t wire_type) {
        switch (wire_type) {
            case 0:
                varint();
                break;
            case 1:
                fixed64();
                break;
            case 2:
                lenDelimited();
                break;
            default:
                CHECK(false && "unexpected wire type");
        }
    }
};

/// zlib-inflates one compressed_packets payload (a serialized Trace fragment).
inline std::vector<uint8_t> inflateBytes(const uint8_t* data, size_t size) {
    std::vector<uint8_t> out(size * 8 + 4096);
    z_stream zs{};
    CHECK(inflateInit(&zs) == Z_OK);
    zs.next_in = const_cast<Bytef*>(data);
    zs.avail_in = static_cast<uInt>(size);
    size_t written = 0;
    for (;;) {
        zs.next_out = out.data() + written;
        zs.avail_out = static_cast<uInt>(out.size() - written);
        int rc = inflate(&zs, Z_NO_FLUSH);
        written = out.size() - zs.avail_out;
        if (rc == Z_STREAM_END) break;
        CHECK((rc == Z_OK || rc == Z_BUF_ERROR) && "inflate failed");
        if (zs.avail_out == 0) {
            out.resize(out.size() * 2);
        } else {
            CHECK(rc != Z_BUF_ERROR && "truncated deflate stream");
        }
    }
    inflateEnd(&zs);
    out.resize(written);
    return out;
}

// Decoded views of the packet subset the writer emits.
struct DecodedTrackEvent {
    uint32_t sequence_id = 0;
    uint64_t timestamp = 0;      ///< Resolved (absolute) timestamp / cycle.
    uint64_t raw_timestamp = 0;  ///< On-the-wire value (delta on incremental clocks).
    /// Set when the packet carried an explicit timestamp_clock_id override.
    std::optional<uint32_t> explicit_clock_id;
    uint64_t type = 0;
    uint64_t track_uuid = 0;
    std::string name;
    std::string category;
    std::optional<int64_t> counter_value;
    std::vector<uint64_t> flow_ids;
    std::map<std::string, uint64_t> uint_annotations;
    std::map<std::string, int64_t> int_annotations;
    std::map<std::string, double> double_annotations;
    std::map<std::string, bool> bool_annotations;
    std::map<std::string, uint64_t> pointer_annotations;
    std::map<std::string, std::string> string_annotations;
    bool name_was_interned = false;
};

struct DecodedTrack {
    uint64_t uuid = 0;
    uint64_t parent_uuid = 0;
    std::string name;
    bool is_process = false;
    bool is_counter = false;
    std::string process_name;
};

struct DecodedClock {
    uint32_t clock_id = 0;
    uint64_t timestamp = 0;
    bool is_incremental = false;
};

struct DecodedTrace {
    std::vector<DecodedTrack> tracks;
    std::vector<DecodedTrackEvent> events;
    bool saw_sequence_start = false;
    size_t compressed_wrappers = 0;
    /// Per-wrapper {compressed payload bytes, inflated bytes}, in file order.
    std::vector<std::pair<size_t, size_t>> compressed_wrapper_sizes;
    /// sequence id → count of SEQ_INCREMENTAL_STATE_CLEARED packets.
    std::map<uint32_t, size_t> state_clears;
    /// sequence id → total InternedData entries emitted (all three tables).
    std::map<uint32_t, size_t> interned_entries;
    std::vector<DecodedClock> cycle_clock_snapshots;
};

/// Per-sequence decoder state mirroring Perfetto's incremental-state rules.
struct SequenceDecodeState {
    std::map<uint64_t, std::string> event_names;
    std::map<uint64_t, std::string> categories;
    std::map<uint64_t, std::string> annotation_names;
    std::optional<uint32_t> default_clock_id;
    bool default_clock_incremental = false;
    uint64_t last_timestamp = 0;

    void clear() {
        event_names.clear();
        categories.clear();
        annotation_names.clear();
        default_clock_id.reset();
        default_clock_incremental = false;
        last_timestamp = 0;
    }
};

constexpr uint32_t kCycleClockId = 64;
constexpr uint32_t kBootClockId = 6;

inline void decodePacket(Decoder pkt, DecodedTrace& trace,
                         std::map<uint32_t, SequenceDecodeState>& sequences);

inline void decodeTraceBytes(const uint8_t* data, size_t size, DecodedTrace& trace,
                             std::map<uint32_t, SequenceDecodeState>& sequences) {
    Decoder d{data, data + size};
    while (!d.done()) {
        auto [field, wire] = d.tag();
        CHECK(field == 1 && wire == 2 && "top level must be Trace.packet");
        decodePacket(d.lenDelimited(), trace, sequences);
    }
}

inline void decodePacket(Decoder pkt, DecodedTrace& trace,
                         std::map<uint32_t, SequenceDecodeState>& sequences) {
    // A packet's fields can arrive in any order, and interned data must be
    // applied before the track event that references it — so collect raw
    // sub-ranges first, then process in dependency order.
    uint64_t timestamp = 0;
    std::optional<uint32_t> ts_clock_id;
    uint32_t seq_id = 0;
    uint64_t seq_flags = 0;
    std::optional<Decoder> clock_snapshot;
    std::optional<Decoder> defaults;
    std::optional<Decoder> interned;
    std::optional<Decoder> track_descriptor;
    std::optional<Decoder> track_event;
    std::optional<Decoder> compressed;

    while (!pkt.done()) {
        auto [pf, pw] = pkt.tag();
        switch (pf) {
            case 8:  // timestamp
                timestamp = pkt.varint();
                break;
            case 58:  // timestamp_clock_id
                ts_clock_id = static_cast<uint32_t>(pkt.varint());
                break;
            case 10:  // trusted_packet_sequence_id
                seq_id = static_cast<uint32_t>(pkt.varint());
                break;
            case 13:  // sequence_flags
                seq_flags = pkt.varint();
                break;
            case 6:  // clock_snapshot
                clock_snapshot = pkt.lenDelimited();
                break;
            case 59:  // trace_packet_defaults
                defaults = pkt.lenDelimited();
                break;
            case 12:  // interned_data
                interned = pkt.lenDelimited();
                break;
            case 60:  // track_descriptor
                track_descriptor = pkt.lenDelimited();
                break;
            case 11:  // track_event
                track_event = pkt.lenDelimited();
                break;
            case 50:  // compressed_packets
                compressed = pkt.lenDelimited();
                break;
            default:
                pkt.skip(pw);
        }
    }

    CHECK(seq_id != 0 && "every packet must carry trusted_packet_sequence_id");

    if (compressed) {
        ++trace.compressed_wrappers;
        const size_t payload_size = static_cast<size_t>(compressed->end - compressed->p);
        std::vector<uint8_t> inner = inflateBytes(compressed->p, payload_size);
        trace.compressed_wrapper_sizes.emplace_back(payload_size, inner.size());
        decodeTraceBytes(inner.data(), inner.size(), trace, sequences);
        return;
    }

    SequenceDecodeState& seq = sequences[seq_id];

    if (seq_flags & 1) {  // SEQ_INCREMENTAL_STATE_CLEARED
        trace.saw_sequence_start = true;
        ++trace.state_clears[seq_id];
        seq.clear();
    }

    if (defaults) {
        Decoder d = *defaults;
        while (!d.done()) {
            auto [df, dw] = d.tag();
            if (df == 58) {  // TracePacketDefaults.timestamp_clock_id
                seq.default_clock_id = static_cast<uint32_t>(d.varint());
            } else {
                d.skip(dw);
            }
        }
    }

    if (clock_snapshot) {
        Decoder cs = *clock_snapshot;
        while (!cs.done()) {
            auto [cf, cw] = cs.tag();
            if (cf == 1) {  // clocks
                Decoder c = cs.lenDelimited();
                DecodedClock clk;
                while (!c.done()) {
                    auto [ff, fw] = c.tag();
                    if (ff == 1) {
                        clk.clock_id = static_cast<uint32_t>(c.varint());
                    } else if (ff == 2) {
                        clk.timestamp = c.varint();
                    } else if (ff == 3) {
                        clk.is_incremental = c.varint() != 0;
                    } else {
                        c.skip(fw);
                    }
                }
                if (clk.clock_id == kCycleClockId) {
                    trace.cycle_clock_snapshots.push_back(clk);
                    if (clk.is_incremental) {
                        seq.default_clock_incremental = true;
                        seq.last_timestamp = clk.timestamp;
                    }
                }
            } else {
                cs.skip(cw);
            }
        }
    }

    if (interned) {
        Decoder in = *interned;
        while (!in.done()) {
            auto [inf, inw] = in.tag();
            // 1 = event_categories, 2 = event_names, 3 = debug_annotation_names
            if (inf == 1 || inf == 2 || inf == 3) {
                Decoder entry = in.lenDelimited();
                uint64_t iid = 0;
                std::string name;
                while (!entry.done()) {
                    auto [ef, ew] = entry.tag();
                    if (ef == 1) {
                        iid = entry.varint();
                    } else if (ef == 2) {
                        name = entry.str();
                    } else {
                        entry.skip(ew);
                    }
                }
                CHECK(iid != 0 && "interned entries must have a non-zero iid");
                auto& table = (inf == 1)   ? seq.categories
                              : (inf == 2) ? seq.event_names
                                           : seq.annotation_names;
                CHECK(table.find(iid) == table.end() && "iid re-interned without state clear");
                table[iid] = std::move(name);
                ++trace.interned_entries[seq_id];
            } else {
                in.skip(inw);
            }
        }
    }

    if (track_descriptor) {
        DecodedTrack track;
        Decoder td = *track_descriptor;
        while (!td.done()) {
            auto [tf, tw] = td.tag();
            switch (tf) {
                case 1:
                    track.uuid = td.varint();
                    break;
                case 2:
                    track.name = td.str();
                    break;
                case 3: {  // process descriptor
                    track.is_process = true;
                    Decoder pd = td.lenDelimited();
                    while (!pd.done()) {
                        auto [ff, fw] = pd.tag();
                        if (ff == 6) {
                            track.process_name = pd.str();
                        } else {
                            pd.skip(fw);
                        }
                    }
                    break;
                }
                case 5:
                    track.parent_uuid = td.varint();
                    break;
                case 8:  // counter descriptor
                    track.is_counter = true;
                    td.skip(tw);
                    break;
                default:
                    td.skip(tw);
            }
        }
        trace.tracks.push_back(std::move(track));
    }

    if (track_event) {
        DecodedTrackEvent ev;
        ev.sequence_id = seq_id;
        ev.raw_timestamp = timestamp;
        ev.explicit_clock_id = ts_clock_id;

        // Resolve the timestamp against the sequence's clock state.
        uint32_t effective_clock =
            ts_clock_id.value_or(seq.default_clock_id.value_or(kBootClockId));
        if (effective_clock == kCycleClockId && seq.default_clock_incremental) {
            seq.last_timestamp += timestamp;
            ev.timestamp = seq.last_timestamp;
        } else {
            ev.timestamp = timestamp;
        }

        Decoder te = *track_event;
        while (!te.done()) {
            auto [tf, tw] = te.tag();
            switch (tf) {
                case 9:
                    ev.type = te.varint();
                    break;
                case 11:
                    ev.track_uuid = te.varint();
                    break;
                case 3: {  // category_iids
                    uint64_t iid = te.varint();
                    auto it = seq.categories.find(iid);
                    CHECK(it != seq.categories.end() && "unresolved category iid");
                    ev.category = it->second;
                    break;
                }
                case 22:
                    ev.category = te.str();
                    break;
                case 10: {  // name_iid
                    uint64_t iid = te.varint();
                    auto it = seq.event_names.find(iid);
                    CHECK(it != seq.event_names.end() && "unresolved event name iid");
                    ev.name = it->second;
                    ev.name_was_interned = true;
                    break;
                }
                case 23:
                    ev.name = te.str();
                    break;
                case 30:
                    ev.counter_value = static_cast<int64_t>(te.varint());
                    break;
                case 47:  // flow_ids (fixed64)
                    ev.flow_ids.push_back(te.fixed64());
                    break;
                case 4: {  // debug annotation
                    Decoder da = te.lenDelimited();
                    std::string name;
                    while (!da.done()) {
                        auto [df, dw] = da.tag();
                        if (df == 1) {  // name_iid
                            uint64_t iid = da.varint();
                            auto it = seq.annotation_names.find(iid);
                            CHECK(it != seq.annotation_names.end() &&
                                  "unresolved annotation name iid");
                            name = it->second;
                        } else if (df == 10) {
                            name = da.str();
                        } else if (df == 2) {  // bool_value
                            ev.bool_annotations[name] = da.varint() != 0;
                        } else if (df == 3) {  // uint_value
                            ev.uint_annotations[name] = da.varint();
                        } else if (df == 4) {  // int_value
                            ev.int_annotations[name] = static_cast<int64_t>(da.varint());
                        } else if (df == 5) {  // double_value (fixed64)
                            uint64_t bits = da.fixed64();
                            double value = 0.0;
                            std::memcpy(&value, &bits, sizeof(value));
                            ev.double_annotations[name] = value;
                        } else if (df == 6) {  // string_value
                            ev.string_annotations[name] = da.str();
                        } else if (df == 7) {  // pointer_value
                            ev.pointer_annotations[name] = da.varint();
                        } else {
                            da.skip(dw);
                        }
                    }
                    break;
                }
                default:
                    te.skip(tw);
            }
        }
        trace.events.push_back(std::move(ev));
    }
}

inline DecodedTrace decodeFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    CHECK(in.is_open());
    std::vector<uint8_t> bytes{std::istreambuf_iterator<char>(in),
                               std::istreambuf_iterator<char>()};

    DecodedTrace trace;
    std::map<uint32_t, SequenceDecodeState> sequences;
    decodeTraceBytes(bytes.data(), bytes.size(), trace, sequences);
    return trace;
}

inline const DecodedTrack* findTrack(const DecodedTrace& trace, uint64_t uuid) {
    for (const auto& t : trace.tracks) {
        if (t.uuid == uuid) return &t;
    }
    return nullptr;
}

inline const DecodedTrack* findTrackByName(const DecodedTrace& trace, std::string_view name) {
    for (const auto& t : trace.tracks) {
        if (t.name == name) return &t;
    }
    return nullptr;
}

}  // namespace pftrace_test
