// SPDX-License-Identifier: MPL-2.0
#include "ClockEventBuffer.hpp"

#include <algorithm>
#include <tuple>

namespace chronon::observe::detail {

ClockEventBuffer::ClockEventBuffer(size_t capacity) : capacity_(capacity) {
    if (capacity < 2 || capacity > MaxRecords)
        throw std::invalid_argument("clock buffer records must be in [2,65536]");
    records_.reserve(capacity);
}

void ClockEventBuffer::append(Record record) {
    if (record.ns < watermark_)
        throw std::invalid_argument("clock event precedes the published watermark");
    if (records_.size() == capacity_ || record.bytes > MaxPendingBytes - bytes_)
        throw std::length_error(
            "native clock bucket buffer exhausted; publish progress in smaller batches "
            "or increase clock_buffer_records (one ns bucket must fit)");
    const auto bytes = record.bytes;
    records_.push_back(std::move(record));
    bytes_ += bytes;
    peak_bytes_ = std::max(peak_bytes_, bytes_);
    peak_records_ = std::max(peak_records_, records_.size());
}

void ClockEventBuffer::advance(uint64_t exclusive_ns,
                               const std::function<void(const Record&)>& consume) {
    if (exclusive_ns < watermark_)
        throw std::invalid_argument("clock watermark cannot move backwards");
    if (exclusive_ns == watermark_) return;
    auto end = std::partition(records_.begin(), records_.end(),
                              [=](const auto& record) { return record.ns < exclusive_ns; });
    // Integer buckets first; exact comparisons are needed only for ns collisions.
    std::sort(records_.begin(), end, [](const auto& a, const auto& b) {
        if (a.ns != b.ns) return a.ns < b.ns;
        if (a.time != b.time) return a.time < b.time;
        return std::tie(a.phase, a.track_name, a.ordinal, a.stream, a.stream_order) <
               std::tie(b.phase, b.track_name, b.ordinal, b.stream, b.stream_order);
    });
    for (auto it = records_.begin(); it != end; ++it) {
        consume(*it);
        bytes_ -= it->bytes;
    }
    records_.erase(records_.begin(), end);
    watermark_ = exclusive_ns;
}

}  // namespace chronon::observe::detail
