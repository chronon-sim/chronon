// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <array>
#include <functional>
#include <memory>
#include <string>

#include "../PerfettoTraceWriter.hpp"

namespace chronon::observe::detail {

/// Private, disk-backed external sort. No simulated producer calls this class.
/// Runs are bounded by BOTH record count and encoded bytes; merge fan-in is fixed.
class ClockEventSpool {
public:
    static constexpr size_t MaxRecordBytes = 64 * 1024;
    static constexpr size_t MaxRunBytes = 4 * 1024 * 1024;
    static constexpr size_t MaxRunRecords = 8192;
    static constexpr size_t MergeFanIn = 16;

    struct OwnedAnnotation {
        std::string name;
        PerfettoTraceWriter::Annotation::Kind kind{};
        uint64_t bits = 0;
        std::string string;
    };
    struct Record {
        SimTime time;
        uint64_t phase = 0, ordinal = 0, stream_order = 0;
        uint64_t track = 0, cycle = 0, flow = 0;
        uint32_t stream = 0;
        std::string track_name, category, name;
        std::array<OwnedAnnotation, 6> annotations;
        size_t annotation_count = 0;
    };

    ClockEventSpool(const std::filesystem::path& destination, size_t run_records);
    ~ClockEventSpool();
    void append(const Record& record);
    void flush();
    /// Offline, after all appends. Callback borrows the record only for this call.
    void replay(const std::function<void(const Record&)>& consume);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace chronon::observe::detail
