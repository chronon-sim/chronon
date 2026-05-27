// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "BinaryTraceReader.hpp"

#include <flatbuffers/flatbuffers.h>

#include <cstring>

#include "ArgFormat.hpp"
#include "CompressionBuffer.hpp"
#include "TraceSchema_generated.h"

namespace chronon::observe {

BinaryTraceReader::BinaryTraceReader() = default;

BinaryTraceReader::~BinaryTraceReader() { close(); }

bool BinaryTraceReader::open(const std::filesystem::path& path) {
    close();

    path_ = path;
    file_.open(path, std::ios::binary);
    if (!file_.is_open()) {
        return false;
    }

    // Initialize decompressor
    decompressor_ = std::make_unique<CompressionBuffer>(3, 256 * 1024);

    // Read and validate header
    if (!readHeader_()) {
        close();
        return false;
    }

    // Read schema
    if (header_.flags & BinaryTraceWriter::FLAG_HAS_SCHEMA) {
        if (!readSchema_()) {
            close();
            return false;
        }
    }

    // Read footer/index
    if (header_.flags & BinaryTraceWriter::FLAG_HAS_INDEX) {
        if (!readFooter_()) {
            close();
            return false;
        }
    }

    // Reset to first block
    current_block_ = 0;
    event_offset_ = 0;
    events_read_ = 0;

    return true;
}

void BinaryTraceReader::close() {
    if (file_.is_open()) {
        file_.close();
    }

    formats_.clear();
    units_.clear();
    unit_id_to_index_.clear();
    blocks_.clear();
    current_block_data_.clear();
    current_events_.clear();
    current_block_ = 0;
    event_offset_ = 0;
    events_read_ = 0;
}

bool BinaryTraceReader::readHeader_() {
    file_.read(reinterpret_cast<char*>(&header_), sizeof(header_));
    if (!file_.good()) {
        return false;
    }

    // Validate magic
    if (header_.magic != BinaryTraceWriter::MAGIC) {
        return false;
    }

    // Populate info
    info_.version_major = header_.version_major;
    info_.version_minor = header_.version_minor;
    info_.flags = header_.flags;
    info_.compressed = (header_.flags & BinaryTraceWriter::FLAG_COMPRESSED) != 0;

    return true;
}

bool BinaryTraceReader::readSchema_() {
    if (header_.schema_size == 0) {
        return true;  // No schema embedded
    }

    // Read schema data
    std::vector<uint8_t> schema_data(header_.schema_size);
    file_.seekg(header_.schema_offset);
    file_.read(reinterpret_cast<char*>(schema_data.data()), header_.schema_size);
    if (!file_.good()) {
        return false;
    }

    // Parse FlatBuffer schema
    auto* schema = binary::GetTraceSchema(schema_data.data());
    if (!schema) {
        return false;
    }

    info_.simulation_name = schema->simulation_name() ? schema->simulation_name()->str() : "";

    // Parse format entries
    if (schema->formats()) {
        for (const auto* entry : *schema->formats()) {
            SchemaEntry fmt;
            fmt.id = entry->id();
            fmt.format_string = entry->format_string() ? entry->format_string()->str() : "";
            fmt.file = entry->file() ? entry->file()->str() : "";
            fmt.line = entry->line();
            fmt.is_log = entry->is_log();
            fmt.log_level = static_cast<LogLevel>(entry->log_level());

            if (entry->arg_types()) {
                for (auto at : *entry->arg_types()) {
                    fmt.arg_types.push_back(static_cast<ArgType>(at));
                }
            }

            formats_.push_back(std::move(fmt));
        }
    }
    info_.format_count = formats_.size();

    // Parse unit entries
    if (schema->units()) {
        for (const auto* entry : *schema->units()) {
            UnitSchemaEntry unit;
            unit.id = entry->id();
            unit.name = entry->name() ? entry->name()->str() : "";
            unit.type_name = entry->type_name() ? entry->type_name()->str() : "";

            unit_id_to_index_[unit.id] = units_.size();
            units_.push_back(std::move(unit));
        }
    }
    info_.unit_count = units_.size();

    return true;
}

bool BinaryTraceReader::readFooter_() {
    if (header_.footer_size == 0) {
        return true;  // No footer
    }

    // Read footer data
    std::vector<uint8_t> footer_data(header_.footer_size);
    file_.seekg(header_.footer_offset);
    file_.read(reinterpret_cast<char*>(footer_data.data()), header_.footer_size);
    if (!file_.good()) {
        return false;
    }

    // Parse FlatBuffer footer (not root type, so use GetRoot)
    auto* footer = flatbuffers::GetRoot<binary::FileFooter>(footer_data.data());
    if (!footer) {
        return false;
    }

    info_.total_events = footer->total_events();
    info_.min_cycle = footer->min_cycle();
    info_.max_cycle = footer->max_cycle();
    info_.created_timestamp = footer->created_timestamp();

    // Parse block headers
    if (footer->blocks()) {
        uint64_t total_uncompressed = 0;
        uint64_t total_compressed = 0;

        for (const auto* bh : *footer->blocks()) {
            BlockInfo block;
            block.block_index = bh->block_index();
            block.event_count = bh->event_count();
            block.min_cycle = bh->min_cycle();
            block.max_cycle = bh->max_cycle();
            block.uncompressed_size = bh->uncompressed_size();
            block.compressed_size = bh->compressed_size();
            block.data_offset = bh->data_offset();

            total_uncompressed += block.uncompressed_size;
            total_compressed += block.compressed_size;

            blocks_.push_back(block);
        }

        info_.block_count = blocks_.size();
        info_.avg_compression_ratio =
            total_uncompressed > 0 ? static_cast<double>(total_compressed) / total_uncompressed
                                   : 1.0;
    }

    return true;
}

bool BinaryTraceReader::loadBlock_(size_t block_index) {
    if (block_index >= blocks_.size()) {
        return false;
    }

    const auto& block = blocks_[block_index];

    // Seek to block data (skip the 28-byte inline header)
    file_.seekg(block.data_offset + 28);  // event_count(4)+min(8)+max(8)+uncomp(4)+comp(4)

    // Read compressed data
    std::vector<std::byte> compressed(block.compressed_size);
    file_.read(reinterpret_cast<char*>(compressed.data()), block.compressed_size);
    if (!file_.good()) {
        return false;
    }

    // Decompress if needed
    if (block.compressed_size != block.uncompressed_size && decompressor_) {
        current_block_data_ = decompressor_->decompress(compressed.data(), compressed.size(),
                                                        block.uncompressed_size);
    } else {
        current_block_data_ = std::move(compressed);
    }

    // Parse events from decompressed data
    current_events_.clear();
    current_events_.reserve(block.event_count);

    size_t offset = 0;
    while (offset < current_block_data_.size()) {
        auto event = parseEvent_(current_block_data_.data(), offset, current_block_data_.size());
        if (event) {
            current_events_.push_back(std::move(*event));
        } else {
            break;
        }
    }

    event_offset_ = 0;
    return true;
}

std::optional<TraceEvent> BinaryTraceReader::parseEvent_(const std::byte* data, size_t& offset,
                                                         size_t max_size) {
    // Event format (v1.1): [args_size:2][StructuredRecord:24][args:N]
    // The 2-byte args_size prefix allows skipping events with unknown formats

    // Check if we have enough data for the args_size prefix
    if (offset + 2 > max_size) {
        return std::nullopt;
    }

    // Read args_size prefix
    uint16_t args_size = 0;
    std::memcpy(&args_size, data + offset, 2);
    offset += 2;

    // Check if we have enough data for the record header
    if (offset + sizeof(StructuredRecord) > max_size) {
        return std::nullopt;
    }

    TraceEvent event;

    // Parse header
    const auto* rec = reinterpret_cast<const StructuredRecord*>(data + offset);
    event.cycle = rec->cycle;
    event.format_id = rec->format_id;
    event.category = rec->category;
    event.source_id = rec->source_id;
    event.arg_count = rec->arg_count;
    event.event_type = 0;  // Default to trace

    offset += sizeof(StructuredRecord);

    // Look up format to get argument types
    const SchemaEntry* fmt = nullptr;
    for (const auto& f : formats_) {
        if (f.id == event.format_id) {
            fmt = &f;
            event.format_string = f.format_string;
            break;
        }
    }

    // Parse arguments using the stored args_size for bounds checking
    if (args_size > 0) {
        // Verify we have enough data
        if (offset + args_size > max_size) {
            return std::nullopt;
        }

        const std::byte* arg_ptr = data + offset;
        const std::byte* arg_end = data + offset + args_size;

        // Try to parse arguments if format is known
        if (fmt) {
            for (uint8_t i = 0; i < event.arg_count && arg_ptr < arg_end; ++i) {
                // Get type from schema if available
                ArgType type = ArgType::None;
                if (i < fmt->arg_types.size()) {
                    type = fmt->arg_types[i];
                } else {
                    // Unknown type - copy remaining bytes as raw data
                    size_t remaining = arg_end - arg_ptr;
                    if (remaining > 0) {
                        size_t old_size = event.args.size();
                        event.args.resize(old_size + remaining);
                        std::memcpy(event.args.data() + old_size, arg_ptr, remaining);
                    }
                    break;
                }

                size_t arg_sz = argSize_(type, arg_ptr, arg_end);
                if (arg_sz == 0 || arg_ptr + arg_sz > arg_end) {
                    break;
                }

                // Copy argument bytes
                size_t old_size = event.args.size();
                event.args.resize(old_size + arg_sz);
                std::memcpy(event.args.data() + old_size, arg_ptr, arg_sz);

                arg_ptr += arg_sz;
            }
        }

        // Always advance offset by the stored args_size (even if we couldn't parse all args)
        // This ensures we correctly find the next event
        offset += args_size;
    }

    // Look up unit name
    auto it = unit_id_to_index_.find(event.source_id);
    if (it != unit_id_to_index_.end() && it->second < units_.size()) {
        event.source_name = units_[it->second].name;
    }

    return event;
}

std::optional<TraceEvent> BinaryTraceReader::readEvent() {
    // Load next block if needed
    while (event_offset_ >= current_events_.size()) {
        if (current_block_ >= blocks_.size()) {
            return std::nullopt;  // End of file
        }

        if (!loadBlock_(current_block_)) {
            return std::nullopt;
        }
        current_block_++;
    }

    TraceEvent event = std::move(current_events_[event_offset_]);
    event_offset_++;
    events_read_++;

    return event;
}

bool BinaryTraceReader::seekToCycle(uint64_t cycle) {
    // Find block containing the cycle
    for (size_t i = 0; i < blocks_.size(); ++i) {
        if (blocks_[i].min_cycle <= cycle && blocks_[i].max_cycle >= cycle) {
            if (!loadBlock_(i)) {
                return false;
            }
            current_block_ = i + 1;

            // Find first event at or after cycle
            for (size_t j = 0; j < current_events_.size(); ++j) {
                if (current_events_[j].cycle >= cycle) {
                    event_offset_ = j;
                    return true;
                }
            }

            event_offset_ = current_events_.size();
            return true;
        }
    }

    // Cycle not in any block - check if it's before or after all blocks
    if (!blocks_.empty()) {
        if (cycle < blocks_.front().min_cycle) {
            reset();
            return true;
        }
        if (cycle > blocks_.back().max_cycle) {
            current_block_ = blocks_.size();
            current_events_.clear();
            event_offset_ = 0;
            return true;
        }
    }

    return false;
}

void BinaryTraceReader::reset() {
    current_block_ = 0;
    current_events_.clear();
    event_offset_ = 0;
    events_read_ = 0;

    if (file_.is_open()) {
        file_.seekg(header_.first_block_offset);
    }
}

std::string BinaryTraceReader::reconstructMessage(const TraceEvent& event) const {
    // Find format in schema
    const SchemaEntry* fmt = nullptr;
    for (const auto& f : formats_) {
        if (f.id == event.format_id) {
            fmt = &f;
            break;
        }
    }

    if (!fmt) {
        return "[unknown format]";
    }

    // Reconstruct message (same logic as ObservationBackend)
    std::string result;
    result.reserve(fmt->format_string.size() + event.args.size() * 2);

    const std::byte* arg_ptr = event.args.data();
    const std::byte* arg_end = event.args.data() + event.args.size();
    size_t arg_index = 0;

    const std::string& fmtstr = fmt->format_string;
    size_t i = 0;

    while (i < fmtstr.size()) {
        if (fmtstr[i] == '{' && i + 1 < fmtstr.size()) {
            size_t j = i + 1;
            while (j < fmtstr.size() && fmtstr[j] != '}') {
                ++j;
            }

            if (j < fmtstr.size() && arg_ptr < arg_end && arg_index < fmt->arg_types.size()) {
                std::string_view spec(fmtstr.data() + i + 1, j - i - 1);
                ArgType arg_type = fmt->arg_types[arg_index];

                std::string arg_str = formatArg_(arg_ptr, arg_type, spec);
                result += arg_str;

                arg_ptr += argSize_(arg_type, arg_ptr, arg_end);
                arg_index++;
                i = j + 1;
            } else {
                result += fmtstr[i];
                ++i;
            }
        } else {
            result += fmtstr[i];
            ++i;
        }
    }

    return result;
}

std::string_view BinaryTraceReader::lookupUnitName(uint16_t source_id) const {
    auto it = unit_id_to_index_.find(source_id);
    if (it != unit_id_to_index_.end() && it->second < units_.size()) {
        return units_[it->second].name;
    }
    return "";
}

std::string BinaryTraceReader::formatArg_(const std::byte* data, ArgType type,
                                          std::string_view spec) const {
    bool hex = spec.find('x') != std::string_view::npos || spec.find('X') != std::string_view::npos;
    return formatArgToString(data, type, hex);
}

size_t BinaryTraceReader::argSize_(ArgType type, const std::byte* data,
                                   const std::byte* end) const {
    return argSize(type, data, end);
}

}  // namespace chronon::observe
