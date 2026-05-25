// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "BinaryTraceWriter.hpp"

#include <flatbuffers/flatbuffers.h>

#include <chrono>
#include <cstring>

#include "CompressionBuffer.hpp"
#include "FormatRegistry.hpp"
#include "TraceSchema_generated.h"

namespace chronon::observe {

BinaryTraceWriter::BinaryTraceWriter(const BinaryTraceConfig& config) : config_(config) {
    std::memset(&header_, 0, sizeof(header_));
    event_buffer_.reserve(config_.block_size * 2);  // Reserve extra for overflow
}

BinaryTraceWriter::~BinaryTraceWriter() {
    if (isOpen()) {
        close();
    }
}

bool BinaryTraceWriter::open(const std::filesystem::path& path) {
    if (isOpen()) {
        close();
    }

    path_ = path;
    file_.open(path, std::ios::binary | std::ios::trunc);
    if (!file_.is_open()) {
        return false;
    }

    // Initialize compression if enabled
    if (config_.compression_enabled) {
        compression_ =
            std::make_unique<CompressionBuffer>(config_.compression_level, config_.block_size);
    }

    // Write placeholder header
    writeHeader_();

    // Reset state
    events_written_ = 0;
    bytes_written_ = FILE_HEADER_SIZE;
    min_cycle_ = UINT64_MAX;
    max_cycle_ = 0;
    schema_written_ = false;
    blocks_.clear();
    event_buffer_used_ = 0;  // Opt 4: reset used counter instead of clear()
    events_in_buffer_ = 0;
    buffer_min_cycle_ = UINT64_MAX;
    buffer_max_cycle_ = 0;

    return true;
}

void BinaryTraceWriter::writeHeader_() {
    header_.magic = MAGIC;
    header_.version_major = VERSION_MAJOR;
    header_.version_minor = VERSION_MINOR;
    header_.schema_offset = 0;
    header_.schema_size = 0;
    header_.first_block_offset = 0;
    header_.footer_offset = 0;
    header_.footer_size = 0;
    header_.flags = 0;

    if (config_.compression_enabled) {
        header_.flags |= FLAG_COMPRESSED;
    }
    if (config_.generate_index) {
        header_.flags |= FLAG_HAS_INDEX;
    }
    if (config_.embed_schema) {
        header_.flags |= FLAG_HAS_SCHEMA;
    }

    file_.write(reinterpret_cast<const char*>(&header_), sizeof(header_));
}

void BinaryTraceWriter::writeSchema_() {
    if (schema_written_) {
        return;
    }
    // Always mark as written to prevent repeated calls, even when not
    // embedding the schema. Without this, writeEvent() re-enters
    // writeSchema_() on every event when embed_schema is false.
    schema_written_ = true;

    if (!config_.embed_schema) {
        return;
    }

    flatbuffers::FlatBufferBuilder builder(4096);

    // Build format entries
    std::vector<flatbuffers::Offset<binary::FormatEntry>> format_offsets;
    FormatRegistry::instance().forEach([&](FormatId id, const FormatInfo& info) {
        // Build arg_types vector (stored as int8_t in FlatBuffer)
        std::vector<int8_t> arg_types;
        for (size_t i = 0; i < info.arg_count; ++i) {
            arg_types.push_back(static_cast<int8_t>(info.arg_types[i]));
        }

        auto format_str = builder.CreateString(info.format_string);
        auto file_str = builder.CreateString(info.file);
        auto arg_types_vec = builder.CreateVector(arg_types);

        binary::FormatEntryBuilder entry_builder(builder);
        entry_builder.add_id(id);
        entry_builder.add_format_string(format_str);
        entry_builder.add_file(file_str);
        entry_builder.add_line(info.line);
        entry_builder.add_arg_types(arg_types_vec);
        entry_builder.add_is_log(info.is_log);
        entry_builder.add_log_level(static_cast<binary::LogLevel>(info.log_level));

        format_offsets.push_back(entry_builder.Finish());
    });

    // Build unit entries from lookup function
    std::vector<flatbuffers::Offset<binary::UnitEntry>> unit_offsets;
    if (source_name_lookup_) {
        // We need to probe for valid unit IDs
        // Typically unit IDs are sequential from 1
        for (uint16_t id = 1; id < 1024; ++id) {
            auto name = source_name_lookup_(id);
            if (name.empty()) {
                continue;
            }
            auto name_str = builder.CreateString(name);

            std::string type_name;
            if (unit_type_lookup_) {
                type_name = std::string(unit_type_lookup_(id));
            }
            auto type_str = builder.CreateString(type_name);

            binary::UnitEntryBuilder entry_builder(builder);
            entry_builder.add_id(id);
            entry_builder.add_name(name_str);
            entry_builder.add_type_name(type_str);
            unit_offsets.push_back(entry_builder.Finish());
        }
    }

    // Build root schema
    auto formats_vec = builder.CreateVector(format_offsets);
    auto units_vec = builder.CreateVector(unit_offsets);
    auto sim_name = builder.CreateString(simulation_name_);

    binary::TraceSchemaBuilder schema_builder(builder);
    schema_builder.add_version(1);
    schema_builder.add_formats(formats_vec);
    schema_builder.add_units(units_vec);
    schema_builder.add_simulation_name(sim_name);

    auto schema = schema_builder.Finish();
    binary::FinishTraceSchemaBuffer(builder, schema);

    // Write schema to file
    header_.schema_offset = static_cast<uint64_t>(file_.tellp());
    file_.write(reinterpret_cast<const char*>(builder.GetBufferPointer()), builder.GetSize());
    header_.schema_size = builder.GetSize();
    bytes_written_ += builder.GetSize();

    // Record first block offset
    header_.first_block_offset = static_cast<uint64_t>(file_.tellp());

    // schema_written_ already set at top of function
}

void BinaryTraceWriter::writeEvent(const StructuredRecord* record, const std::byte* args_data,
                                   size_t args_size) {
    if (!isOpen()) {
        return;
    }

    // Write schema on first event (after all formats are registered)
    if (!schema_written_) {
        writeSchema_();
    }

    // Calculate total event size: [args_size:2][StructuredRecord:24][args:N]
    // The 2-byte args_size prefix allows the reader to skip events with unknown formats
    size_t event_size = 2 + sizeof(StructuredRecord) + args_size;

    // Opt 4: Grow capacity only when needed (avoid zero-init from resize())
    size_t required = event_buffer_used_ + event_size;
    if (required > event_buffer_.size()) {
        size_t new_size = std::max(event_buffer_.size() * 2, required);
        event_buffer_.resize(new_size);
    }

    // Write at current used offset (no zero-init overhead)
    size_t write_pos = event_buffer_used_;

    // Write args_size prefix (2 bytes, little-endian)
    uint16_t args_size_u16 = static_cast<uint16_t>(args_size);
    std::memcpy(event_buffer_.data() + write_pos, &args_size_u16, 2);

    // Write StructuredRecord
    std::memcpy(event_buffer_.data() + write_pos + 2, record, sizeof(StructuredRecord));

    // Write args data
    if (args_size > 0) {
        std::memcpy(event_buffer_.data() + write_pos + 2 + sizeof(StructuredRecord), args_data,
                    args_size);
    }

    event_buffer_used_ += event_size;

    // Update buffer stats
    events_in_buffer_++;
    if (record->cycle < buffer_min_cycle_) {
        buffer_min_cycle_ = record->cycle;
    }
    if (record->cycle > buffer_max_cycle_) {
        buffer_max_cycle_ = record->cycle;
    }

    // Flush if used bytes exceed threshold
    if (event_buffer_used_ >= config_.block_size) {
        flushBlock_();
    }
}

void BinaryTraceWriter::flushBlock_() {
    if (event_buffer_used_ == 0 || events_in_buffer_ == 0) {
        return;
    }

    // Opt 4: Use event_buffer_used_ for actual data size
    uint32_t uncompressed_size = static_cast<uint32_t>(event_buffer_used_);
    uint32_t compressed_size;
    const std::byte* write_data;
    size_t write_size;
    std::vector<std::byte> compressed_data;

    if (compression_ && compression_->isAvailable()) {
        compressed_data = compression_->compress(event_buffer_.data(), event_buffer_used_);
        compressed_size = static_cast<uint32_t>(compressed_data.size());
        write_data = compressed_data.data();
        write_size = compressed_data.size();
    } else {
        compressed_size = uncompressed_size;
        write_data = event_buffer_.data();
        write_size = event_buffer_used_;
    }

    // Record block info
    BlockInfo block;
    block.block_index = static_cast<uint32_t>(blocks_.size());
    block.event_count = events_in_buffer_;
    block.min_cycle = buffer_min_cycle_;
    block.max_cycle = buffer_max_cycle_;
    block.uncompressed_size = uncompressed_size;
    block.compressed_size = compressed_size;
    block.data_offset = static_cast<uint64_t>(file_.tellp());

    // Write block header (simple binary format for streaming)
    // Format: [event_count:4][min_cycle:8][max_cycle:8][uncompressed:4][compressed:4][data...]
    file_.write(reinterpret_cast<const char*>(&block.event_count), 4);
    file_.write(reinterpret_cast<const char*>(&block.min_cycle), 8);
    file_.write(reinterpret_cast<const char*>(&block.max_cycle), 8);
    file_.write(reinterpret_cast<const char*>(&block.uncompressed_size), 4);
    file_.write(reinterpret_cast<const char*>(&block.compressed_size), 4);

    // Write event data
    file_.write(reinterpret_cast<const char*>(write_data),
                static_cast<std::streamsize>(write_size));

    // Update stats
    blocks_.push_back(block);
    events_written_ += events_in_buffer_;
    bytes_written_ += 28 + write_size;  // header + data

    if (buffer_min_cycle_ < min_cycle_) {
        min_cycle_ = buffer_min_cycle_;
    }
    if (buffer_max_cycle_ > max_cycle_) {
        max_cycle_ = buffer_max_cycle_;
    }

    // Opt 4: Reset used counter (preserves buffer capacity, no zero-init)
    event_buffer_used_ = 0;
    events_in_buffer_ = 0;
    buffer_min_cycle_ = UINT64_MAX;
    buffer_max_cycle_ = 0;
}

void BinaryTraceWriter::flush() {
    if (!isOpen()) {
        return;
    }

    // Flush current block if it has events
    if (events_in_buffer_ > 0) {
        flushBlock_();
    }

    file_.flush();
}

void BinaryTraceWriter::writeFooter_() {
    if (!config_.generate_index || blocks_.empty()) {
        return;
    }

    flatbuffers::FlatBufferBuilder builder(1024 + blocks_.size() * 48);

    // Build block headers
    std::vector<flatbuffers::Offset<binary::BlockHeader>> block_offsets;
    for (const auto& block : blocks_) {
        binary::BlockHeaderBuilder header_builder(builder);
        header_builder.add_block_index(block.block_index);
        header_builder.add_event_count(block.event_count);
        header_builder.add_min_cycle(block.min_cycle);
        header_builder.add_max_cycle(block.max_cycle);
        header_builder.add_uncompressed_size(block.uncompressed_size);
        header_builder.add_compressed_size(block.compressed_size);
        header_builder.add_data_offset(block.data_offset);
        block_offsets.push_back(header_builder.Finish());
    }

    auto blocks_vec = builder.CreateVector(block_offsets);

    // Get current timestamp
    auto now = std::chrono::system_clock::now();
    auto timestamp = static_cast<uint64_t>(std::chrono::system_clock::to_time_t(now));

    binary::FileFooterBuilder footer_builder(builder);
    footer_builder.add_blocks(blocks_vec);
    footer_builder.add_total_events(events_written_);
    footer_builder.add_min_cycle(min_cycle_);
    footer_builder.add_max_cycle(max_cycle_);
    footer_builder.add_created_timestamp(timestamp);

    auto footer = footer_builder.Finish();
    builder.Finish(footer);

    // Write footer
    header_.footer_offset = static_cast<uint64_t>(file_.tellp());
    file_.write(reinterpret_cast<const char*>(builder.GetBufferPointer()), builder.GetSize());
    header_.footer_size = builder.GetSize();
    bytes_written_ += builder.GetSize();
}

void BinaryTraceWriter::updateHeader_() {
    // Seek to beginning and rewrite header with final offsets
    file_.seekp(0);
    file_.write(reinterpret_cast<const char*>(&header_), sizeof(header_));
}

void BinaryTraceWriter::close() {
    if (!isOpen()) {
        return;
    }

    // Flush any remaining events
    flush();

    // Write footer with index
    writeFooter_();

    // Update header with final offsets
    updateHeader_();

    file_.close();
}

}  // namespace chronon::observe
