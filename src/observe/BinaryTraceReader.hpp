// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "BinaryTraceWriter.hpp"
#include "FormatRegistry.hpp"
#include "Types.hpp"

namespace chronon::observe {

class CompressionBuffer;

/** @brief Decoded event from a binary trace. */
struct TraceEvent {
    uint64_t cycle;
    uint32_t format_id;
    uint32_t category;
    uint16_t source_id;
    uint8_t arg_count;
    uint8_t event_type;  ///< 0=trace, 1=log, 2=counter.

    std::vector<std::byte> args;

    std::string format_string;
    std::string source_name;
};

/** @brief Summary information about a trace file. */
struct TraceInfo {
    uint16_t version_major;
    uint16_t version_minor;
    uint64_t flags;

    uint64_t total_events;
    uint64_t min_cycle;
    uint64_t max_cycle;
    uint64_t created_timestamp;

    size_t format_count;
    size_t unit_count;
    size_t block_count;
    std::string simulation_name;

    bool compressed;
    double avg_compression_ratio;
};

/** @brief Format entry from the embedded schema. */
struct SchemaEntry {
    uint32_t id;
    std::string format_string;
    std::string file;
    uint32_t line;
    std::vector<ArgType> arg_types;
    bool is_log;
    LogLevel log_level;
};

/** @brief Unit metadata from the embedded schema. */
struct UnitSchemaEntry {
    uint16_t id;
    std::string name;
    std::string type_name;
};

/**
 * @brief Offline reader for binary trace files.
 *
 * Lazy block loading with optional decompression, forward iteration,
 * footer-index cycle seek, and message reconstruction from the embedded schema.
 */
class BinaryTraceReader {
public:
    BinaryTraceReader();
    ~BinaryTraceReader();

    BinaryTraceReader(const BinaryTraceReader&) = delete;
    BinaryTraceReader& operator=(const BinaryTraceReader&) = delete;

    bool open(const std::filesystem::path& path);

    [[nodiscard]] bool isOpen() const noexcept { return file_.is_open(); }

    void close();

    [[nodiscard]] const TraceInfo& info() const noexcept { return info_; }
    [[nodiscard]] const std::vector<SchemaEntry>& formats() const noexcept { return formats_; }
    [[nodiscard]] const std::vector<UnitSchemaEntry>& units() const noexcept { return units_; }

    /// @return Next event, or nullopt at end of file.
    std::optional<TraceEvent> readEvent();

    /// After seek, readEvent() returns the first event with cycle >= @p cycle.
    bool seekToCycle(uint64_t cycle);

    void reset();

    [[nodiscard]] bool atEnd() const noexcept {
        return current_block_ >= blocks_.size() && event_offset_ >= current_events_.size();
    }

    std::string reconstructMessage(const TraceEvent& event) const;

    [[nodiscard]] std::string_view lookupUnitName(uint16_t source_id) const;

    [[nodiscard]] uint64_t eventsRead() const noexcept { return events_read_; }

private:
    bool readHeader_();
    bool readSchema_();
    bool readFooter_();
    bool loadBlock_(size_t block_index);
    std::optional<TraceEvent> parseEvent_(const std::byte* data, size_t& offset, size_t max_size);

    std::string formatArg_(const std::byte* data, ArgType type, std::string_view spec) const;
    size_t argSize_(ArgType type, const std::byte* data, const std::byte* end) const;

    std::ifstream file_;
    std::filesystem::path path_;

    BinaryTraceWriter::FileHeader header_;
    std::vector<BlockInfo> blocks_;
    TraceInfo info_;

    std::vector<SchemaEntry> formats_;
    std::vector<UnitSchemaEntry> units_;
    std::unordered_map<uint16_t, size_t> unit_id_to_index_;

    size_t current_block_ = 0;
    std::vector<std::byte> current_block_data_;
    std::vector<TraceEvent> current_events_;
    size_t event_offset_ = 0;

    std::unique_ptr<CompressionBuffer> decompressor_;

    uint64_t events_read_ = 0;
};

}  // namespace chronon::observe
