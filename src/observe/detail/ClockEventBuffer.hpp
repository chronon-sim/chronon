// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <array>
#include <functional>
#include <string>
#include <vector>

#include "../PerfettoTraceWriter.hpp"

namespace chronon::observe::detail {

/// Bounded open timestamp buckets. No scratch files or whole-trace sorting.
class ClockEventBuffer {
public:
    static constexpr size_t BaseRecordBytes = 512;
    static constexpr size_t MaxRecordBytes = 64 * 1024;
    static constexpr size_t MaxPendingBytes = 4 * 1024 * 1024;
    static constexpr size_t MaxRecords = 65536;
    struct OwnedAnnotation {
        std::string name;
        PerfettoTraceWriter::Annotation::Kind kind{};
        uint64_t bits = 0;
        std::string string;
    };
    struct Record {
        SimTime time;
        uint64_t ns = 0, phase = 0, ordinal = 0, stream_order = 0;
        uint64_t track = 0, cycle = 0, flow = 0;
        uint32_t stream = 0;
        std::string track_name, category, name;
        std::array<OwnedAnnotation, 6> annotations;
        size_t annotation_count = 0, bytes = 0;
    };

    explicit ClockEventBuffer(size_t capacity);
    void append(Record record);
    /// Emit only ns < exclusive_ns. Future appends below that bound are invalid.
    void advance(uint64_t exclusive_ns, const std::function<void(const Record&)>& consume);
    uint64_t watermark() const noexcept { return watermark_; }
    size_t peakBytes() const noexcept { return records_.capacity() * sizeof(Record) + peak_bytes_; }
    size_t peakRecords() const noexcept { return peak_records_; }

private:
    std::vector<Record> records_;
    const size_t capacity_;
    uint64_t watermark_ = 0;
    size_t bytes_ = 0, peak_bytes_ = 0, peak_records_ = 0;
};

}  // namespace chronon::observe::detail
