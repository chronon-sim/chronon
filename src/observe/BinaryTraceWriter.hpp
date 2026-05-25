// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "FormatRegistry.hpp"
#include "Types.hpp"

namespace chronon::observe {

class CompressionBuffer;

/** @brief Configuration for binary trace output. */
struct BinaryTraceConfig {
    bool compression_enabled = true;
    int compression_level = 3;      ///< zstd level: 1-22; 3 is the fast default.
    size_t block_size = 64 * 1024;  ///< Pre-compression block size.

    bool embed_schema = true;
    bool generate_index = true;
};

/** @brief Per-block info used to build the footer index. */
struct BlockInfo {
    uint32_t block_index;
    uint32_t event_count;
    uint64_t min_cycle;
    uint64_t max_cycle;
    uint32_t uncompressed_size;
    uint32_t compressed_size;
    uint64_t data_offset;
};

/**
 * @brief High-performance binary trace writer using FlatBuffers + optional zstd.
 *
 * File layout: `[FileHeader 64B][Schema FlatBuffer][EventBlock 0]...[EventBlock N][Footer
 * FlatBuffer]`. Events buffer in memory and flush as compressed blocks once the buffer exceeds
 * `block_size`. NOT thread-safe; intended for a single backend thread.
 */
class BinaryTraceWriter {
public:
    static constexpr uint32_t MAGIC = 0x43545243;  ///< "CTRC" little-endian.
    static constexpr uint16_t VERSION_MAJOR = 1;
    static constexpr uint16_t VERSION_MINOR = 1;  ///< v1.1 adds per-event args_size prefix.
    static constexpr size_t FILE_HEADER_SIZE = 64;

    /** @brief Fixed 64-byte file header. */
    struct FileHeader {
        uint32_t magic;
        uint16_t version_major;
        uint16_t version_minor;
        uint64_t schema_offset;
        uint64_t schema_size;
        uint64_t first_block_offset;
        uint64_t footer_offset;  ///< 0 until close() writes the footer.
        uint64_t footer_size;
        uint64_t flags;
        uint8_t reserved[8];
    };
    static_assert(sizeof(FileHeader) == FILE_HEADER_SIZE, "FileHeader size mismatch");

    static constexpr uint64_t FLAG_COMPRESSED = 1ULL << 0;
    static constexpr uint64_t FLAG_HAS_INDEX = 1ULL << 1;
    static constexpr uint64_t FLAG_HAS_SCHEMA = 1ULL << 2;

    explicit BinaryTraceWriter(const BinaryTraceConfig& config = {});

    ~BinaryTraceWriter();

    BinaryTraceWriter(const BinaryTraceWriter&) = delete;
    BinaryTraceWriter& operator=(const BinaryTraceWriter&) = delete;

    /// Opens @p path, writes a placeholder header. Schema is written lazily.
    bool open(const std::filesystem::path& path);

    [[nodiscard]] bool isOpen() const noexcept { return file_.is_open(); }

    /// Buffers an event; blocks are flushed when @c block_size is exceeded.
    void writeEvent(const StructuredRecord* record, const std::byte* args_data, size_t args_size);

    void setSourceNameLookup(std::function<std::string_view(uint16_t)> lookup) noexcept {
        source_name_lookup_ = std::move(lookup);
    }

    void setUnitTypeLookup(std::function<std::string_view(uint16_t)> lookup) noexcept {
        unit_type_lookup_ = std::move(lookup);
    }

    void setSimulationName(std::string name) noexcept { simulation_name_ = std::move(name); }

    void flush();

    /// Writes footer and updates header before closing.
    void close();

    [[nodiscard]] uint64_t eventsWritten() const noexcept { return events_written_; }
    [[nodiscard]] uint64_t bytesWritten() const noexcept { return bytes_written_; }
    [[nodiscard]] uint64_t blocksWritten() const noexcept { return blocks_.size(); }

private:
    void writeHeader_();
    void writeSchema_();
    void flushBlock_();
    void writeFooter_();
    void updateHeader_();

    BinaryTraceConfig config_;

    std::ofstream file_;
    std::filesystem::path path_;

    FileHeader header_;
    std::vector<BlockInfo> blocks_;

    std::vector<std::byte> event_buffer_;
    size_t event_buffer_used_ = 0;  ///< Manual used count avoids zero-init on resize.
    uint32_t events_in_buffer_ = 0;
    uint64_t buffer_min_cycle_ = UINT64_MAX;
    uint64_t buffer_max_cycle_ = 0;

    std::unique_ptr<CompressionBuffer> compression_;

    bool schema_written_ = false;

    uint64_t events_written_ = 0;
    uint64_t bytes_written_ = 0;
    uint64_t min_cycle_ = UINT64_MAX;
    uint64_t max_cycle_ = 0;

    std::function<std::string_view(uint16_t)> source_name_lookup_;
    std::function<std::string_view(uint16_t)> unit_type_lookup_;
    std::string simulation_name_;
};

}  // namespace chronon::observe
