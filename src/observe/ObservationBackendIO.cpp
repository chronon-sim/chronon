// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file ObservationBackendIO.cpp
/// @brief File I/O, text/binary output formatting, counter CSV writing,
///        and output directory initialization for ObservationBackend.

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <chrono>
#include <iostream>

#include "BinaryTraceWriter.hpp"
#include "FormatRegistry.hpp"
#include "ObservationBackend.hpp"

namespace chronon::observe {

// ---------------------------------------------------------------------------
// Text event formatting
// ---------------------------------------------------------------------------

void ObservationBackend::writeEventAsText_(const StructuredRecord* rec, const std::byte* args_data,
                                           size_t args_size, const char* level_str,
                                           LogFileSink& sink) {
    const FormatInfo& fmt_info = FormatRegistry::instance().getFormat(rec->format_id);
    if (fmt_info.format_string.empty()) {
        return;  // Invalid format ID
    }

    std::string_view source_name;
    if (rec->source_id > 0 && rec->source_id < source_name_cache_.size()) {
        source_name = source_name_cache_[rec->source_id];
    }

    if (!source_name.empty()) {
        fmt::format_to(std::back_inserter(sink.buffer), "[{:>10}] [{:>5}] {}: ", rec->cycle,
                       level_str, source_name);
    } else {
        fmt::format_to(std::back_inserter(sink.buffer), "[{:>10}] [{:>5}] ", rec->cycle, level_str);
    }

    reconstructMessageTo_(sink.buffer, fmt_info, rec, args_data, args_size);
    sink.buffer.push_back('\n');

    size_t approx_bytes = 25 + source_name.size() + fmt_info.format_string.size() + args_size * 2;
    local_bytes_written_ += approx_bytes;

    if (sink.buffer.size() >= TEXT_BUFFER_FLUSH_SIZE) {
        sink.flush();
    }
}

// ---------------------------------------------------------------------------
// Per-channel format resolution
// ---------------------------------------------------------------------------

OutputFormat ObservationBackend::resolveTraceFormat_(CategoryMask event_category) const {
    CategoryMask user_cat = event_category & category::USER_CATEGORY_MASK;
    if (user_cat != 0) {
        auto it = config_.category_format_overrides.find(user_cat);
        if (it != config_.category_format_overrides.end() && it->second.trace_format.has_value()) {
            return *it->second.trace_format;
        }
    }

    return config_.trace_format;
}

OutputFormat ObservationBackend::resolveLogFormat_(CategoryMask log_category) const {
    if (log_category & static_cast<uint64_t>(category::LOG_DEBUG)) {
        return config_.debug_format;
    }
    if (log_category & static_cast<uint64_t>(category::LOG_WARN)) {
        return config_.warn_format;
    }
    if (log_category & static_cast<uint64_t>(category::LOG_ERROR)) {
        return config_.error_format;
    }
    return config_.info_format;
}

// ---------------------------------------------------------------------------
// Format-string reconstruction
// ---------------------------------------------------------------------------

std::string ObservationBackend::reconstructMessage_(const FormatInfo& fmt_info,
                                                    const StructuredRecord* rec,
                                                    const std::byte* args_data, size_t args_size) {
    fmt::memory_buffer buf;
    buf.reserve(fmt_info.format_string.size() + args_size * 2);
    reconstructMessageTo_(buf, fmt_info, rec, args_data, args_size);
    return std::string(buf.data(), buf.size());
}

/// Lazy-compiles the format string into segments on first use, then iterates
/// segments instead of scanning char-by-char on every event.
void ObservationBackend::reconstructMessageTo_(fmt::memory_buffer& out, const FormatInfo& fmt_info,
                                               const StructuredRecord* rec,
                                               const std::byte* args_data, size_t args_size) {
    if (!fmt_info.compiled.compiled) {
        fmt_info.compiled.compile(fmt_info.format_string);
    }

    const auto& segments = fmt_info.compiled.segments;
    const std::string& fmt = fmt_info.format_string;
    const std::byte* arg_ptr = args_data;
    const std::byte* arg_end = args_data + args_size;
    size_t arg_index = 0;

    for (const auto& seg : segments) {
        if (seg.type == FormatSegment::Type::Literal) {
            out.append(fmt.data() + seg.start, fmt.data() + seg.start + seg.length);
        } else {
            if (arg_ptr < arg_end && arg_index < rec->arg_count) {
                ArgType arg_type = ArgType::None;
                if (arg_index < fmt_info.arg_count && arg_index < MAX_FORMAT_ARGS) {
                    arg_type = fmt_info.arg_types[arg_index];
                }
                formatArgToFast_(out, arg_ptr, arg_type, seg.hex);
                arg_ptr += argSize_(arg_type, arg_ptr, arg_end);
                arg_index++;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Argument formatting
// ---------------------------------------------------------------------------

std::string ObservationBackend::formatArg_(const std::byte* data, ArgType type,
                                           std::string_view spec) {
    fmt::memory_buffer buf;
    formatArgTo_(buf, data, type, spec);
    return std::string(buf.data(), buf.size());
}

void ObservationBackend::formatArgTo_(fmt::memory_buffer& out, const std::byte* data, ArgType type,
                                      std::string_view spec) {
    bool hex = spec.find('x') != std::string_view::npos || spec.find('X') != std::string_view::npos;
    formatArgToFast_(out, data, type, hex);
}

/// Fast argument formatting with pre-computed hex flag -- avoids re-parsing
/// the format spec string on every call.
void ObservationBackend::formatArgToFast_(fmt::memory_buffer& out, const std::byte* data,
                                          ArgType type, bool hex) {
    switch (type) {
        case ArgType::Int8: {
            int8_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", static_cast<int>(val));
            } else {
                fmt::format_to(std::back_inserter(out), "{}", static_cast<int>(val));
            }
            return;
        }
        case ArgType::Int16: {
            int16_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::Int32: {
            int32_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::Int64: {
            int64_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::UInt8: {
            uint8_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", static_cast<unsigned>(val));
            } else {
                fmt::format_to(std::back_inserter(out), "{}", static_cast<unsigned>(val));
            }
            return;
        }
        case ArgType::UInt16: {
            uint16_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::UInt32: {
            uint32_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::UInt64: {
            uint64_t val;
            std::memcpy(&val, data, sizeof(val));
            if (hex) {
                fmt::format_to(std::back_inserter(out), "{:x}", val);
            } else {
                fmt::format_to(std::back_inserter(out), "{}", val);
            }
            return;
        }
        case ArgType::Float: {
            float val;
            std::memcpy(&val, data, sizeof(val));
            fmt::format_to(std::back_inserter(out), "{}", val);
            return;
        }
        case ArgType::Double: {
            double val;
            std::memcpy(&val, data, sizeof(val));
            fmt::format_to(std::back_inserter(out), "{}", val);
            return;
        }
        case ArgType::Pointer: {
            const void* val;
            std::memcpy(&val, data, sizeof(val));
            fmt::format_to(std::back_inserter(out), "0x{:x}", reinterpret_cast<uintptr_t>(val));
            return;
        }
        case ArgType::String: {
            const char* str = reinterpret_cast<const char*>(data);
            out.append(str, str + std::char_traits<char>::length(str));
            return;
        }
        case ArgType::Bool: {
            bool val;
            std::memcpy(&val, data, sizeof(val));
            const char* s = val ? "true" : "false";
            out.append(s, s + (val ? 4 : 5));
            return;
        }
        case ArgType::None:
        default:
            out.push_back('?');
            return;
    }
}

size_t ObservationBackend::argSize_(ArgType type, const std::byte* data, const std::byte* end) {
    switch (type) {
        case ArgType::Int8:
        case ArgType::UInt8:
        case ArgType::Bool:
            return 1;
        case ArgType::Int16:
        case ArgType::UInt16:
            return 2;
        case ArgType::Int32:
        case ArgType::UInt32:
        case ArgType::Float:
            return 4;
        case ArgType::Int64:
        case ArgType::UInt64:
        case ArgType::Double:
        case ArgType::Pointer:
            return 8;
        case ArgType::String: {
            const char* str = reinterpret_cast<const char*>(data);
            size_t len = 0;
            while (data + len < end && str[len] != '\0') {
                ++len;
            }
            return len + 1;  // Include null terminator
        }
        case ArgType::None:
        default:
            return 0;
    }
}

// ---------------------------------------------------------------------------
// Buffer and file flushing
// ---------------------------------------------------------------------------

void ObservationBackend::flushAllTextBuffers_() {
    if (default_sink_) {
        default_sink_->flush();
    }
    for (auto& [name, sink] : custom_sinks_) {
        sink->flush();
    }
}

void ObservationBackend::flushCounterBuffer_() {
    if (counter_buffer_.size() > 0 && counter_file_.is_open()) {
        counter_file_.write(counter_buffer_.data(),
                            static_cast<std::streamsize>(counter_buffer_.size()));
        counter_buffer_.clear();
    }
}

void ObservationBackend::flush_() {
    flushAllTextBuffers_();

    flushCounterBuffer_();

    // Time-based OS flush: only call file.flush() periodically to reduce
    // syscall overhead.
    auto now = std::chrono::steady_clock::now();
    if (now - last_os_flush_time_ >= OS_FLUSH_INTERVAL) {
        if (counter_file_.is_open()) counter_file_.flush();
        if (default_sink_) default_sink_->osFlush();
        for (auto& [name, sink] : custom_sinks_) {
            sink->osFlush();
        }
        if (binary_trace_writer_) binary_trace_writer_->flush();
        last_os_flush_time_ = now;
    }

    if (local_bytes_written_ > 0) {
        bytes_written_.fetch_add(local_bytes_written_, std::memory_order_relaxed);
        local_bytes_written_ = 0;
    }
}

// ---------------------------------------------------------------------------
// Counter CSV output
// ---------------------------------------------------------------------------

void ObservationBackend::finalizeCounterColumns_() {
    for (const auto& [key, value] : counter_first_batch_) {
        auto [it, inserted] = counter_col_index_.try_emplace(key, counter_columns_.size());
        if (inserted) {
            counter_columns_.push_back(key);
        }
    }

    resolveDerivedCounters_();
}

void ObservationBackend::writeCounterCsvHeader_() {
    counter_file_.open(output_dir_ / "counters.csv");
    if (!counter_file_.is_open()) {
        return;
    }

    fmt::memory_buffer buf;
    fmt::format_to(std::back_inserter(buf), "cycle");
    for (const auto& col : counter_columns_) {
        fmt::format_to(std::back_inserter(buf), ",{}", col);
    }
    for (const auto& rd : resolved_derived_) {
        fmt::format_to(std::back_inserter(buf), ",{}", rd.column_name);
    }
    buf.push_back('\n');
    counter_file_.write(buf.data(), static_cast<std::streamsize>(buf.size()));
}

void ObservationBackend::flushCounterRow_(uint64_t cycle) {
    if (!counter_file_.is_open() || counter_columns_.empty()) {
        return;
    }

    if (!resolved_derived_.empty()) {
        computeDerived_();
    }

    fmt::memory_buffer buf;
    fmt::format_to(std::back_inserter(buf), "{}", cycle);
    for (size_t i = 0; i < counter_columns_.size(); ++i) {
        fmt::format_to(std::back_inserter(buf), ",{}", current_counter_row_[i]);
    }
    for (double v : current_derived_row_) {
        fmt::format_to(std::back_inserter(buf), ",{:.6f}", v);
    }
    buf.push_back('\n');
    counter_file_.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    counter_file_.flush();

    local_bytes_written_ += buf.size();
}

/// Handles the single-batch edge case (only one dump cycle occurred) and
/// flushes the last pending row on shutdown.
void ObservationBackend::finalizeCounterCsv_() {
    if (!counter_csv_streaming_ && !counter_first_batch_.empty()) {
        finalizeCounterColumns_();
        writeCounterCsvHeader_();
        current_counter_row_.assign(counter_columns_.size(), 0);
        for (const auto& [k, v] : counter_first_batch_) {
            auto it = counter_col_index_.find(k);
            if (it != counter_col_index_.end()) {
                current_counter_row_[it->second] = v;
            }
        }
        flushCounterRow_(counter_first_cycle_);
        counter_first_batch_.clear();
    } else if (counter_csv_streaming_ && current_counter_cycle_ != UINT64_MAX) {
        flushCounterRow_(current_counter_cycle_);
    }

    if (config_.counter_csv_format == CounterCsvFormat::Long && !derived_counter_defs_.empty() &&
        long_current_cycle_ != UINT64_MAX && !long_cycle_values_.empty()) {
        emitLongDerivedValues_(long_current_cycle_);
        long_cycle_values_.clear();
        flushCounterBuffer_();
    }
}

// ---------------------------------------------------------------------------
// Derived counters
// ---------------------------------------------------------------------------

void ObservationBackend::setDerivedCounterDefs(std::vector<DerivedCounterDef> defs) {
    derived_counter_defs_ = std::move(defs);
}

void ObservationBackend::resolveDerivedCounters_() {
    resolved_derived_.clear();

    for (const auto& def : derived_counter_defs_) {
        std::vector<size_t> cols;
        cols.reserve(def.source_names.size());
        bool all_found = true;

        for (const auto& src_name : def.source_names) {
            std::string qualified;
            qualified.reserve(def.unit_name.size() + 1 + src_name.size());
            qualified.append(def.unit_name);
            qualified.push_back('.');
            qualified.append(src_name);

            auto it = counter_col_index_.find(qualified);
            if (it != counter_col_index_.end()) {
                cols.push_back(it->second);
            } else {
                std::cerr << "[observe] derived counter '" << def.unit_name << '.'
                          << def.derived_name << "': source '" << qualified << "' not found\n";
                all_found = false;
            }
        }

        if (all_found) {
            ResolvedDerived rd;
            rd.column_name.reserve(def.unit_name.size() + 1 + def.derived_name.size());
            rd.column_name.append(def.unit_name);
            rd.column_name.push_back('.');
            rd.column_name.append(def.derived_name);
            rd.source_cols = std::move(cols);
            rd.compute = def.compute;
            resolved_derived_.push_back(std::move(rd));
        }
    }

    current_derived_row_.resize(resolved_derived_.size(), 0.0);
}

void ObservationBackend::computeDerived_() {
    thread_local std::vector<uint64_t> vals;
    for (size_t i = 0; i < resolved_derived_.size(); ++i) {
        const auto& rd = resolved_derived_[i];
        vals.clear();
        for (size_t col : rd.source_cols) {
            vals.push_back(current_counter_row_[col]);
        }
        current_derived_row_[i] = rd.compute(std::span<const uint64_t>(vals));
    }
}

void ObservationBackend::emitLongDerivedValues_(uint64_t cycle) {
    std::vector<uint64_t> vals;
    for (const auto& def : derived_counter_defs_) {
        vals.clear();
        vals.reserve(def.source_names.size());
        for (const auto& src_name : def.source_names) {
            std::string qualified = def.unit_name + "." + src_name;
            auto it = long_cycle_values_.find(qualified);
            vals.push_back(it != long_cycle_values_.end() ? it->second : 0);
        }
        double result = def.compute(std::span<const uint64_t>(vals));
        fmt::format_to(std::back_inserter(counter_buffer_), "{},{},{},{:.6f}\n", cycle,
                       def.unit_name, def.derived_name, result);
    }
}

// ---------------------------------------------------------------------------
// Output directory initialization
// ---------------------------------------------------------------------------

/// Sets up the timestamped output directory, opens counter CSV, binary trace
/// writer, and text log sinks based on per-channel format configuration.
void ObservationBackend::initializeOutputDir_() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    std::string timestamp = fmt::format("{:%Y%m%d_%H%M%S}", *std::localtime(&time));

    output_dir_ = std::filesystem::path(config_.output_dir) / timestamp;
    std::filesystem::create_directories(output_dir_);

    // Create/update "latest" symlink pointing to this run's output directory
    {
        auto latest_link = std::filesystem::path(config_.output_dir) / "latest";
        std::error_code ec;
        std::filesystem::remove(latest_link, ec);
        std::filesystem::create_directory_symlink(output_dir_.filename(), latest_link, ec);
    }

    if (config_.enable_counter_csv && config_.counter_csv_format == CounterCsvFormat::Long) {
        counter_file_.open(output_dir_ / "counters.csv");
        counter_file_ << "cycle,unit,counter_name,value\n";
    }

    if (config_.needsBinaryOutput()) {
        const auto& trace_cfg = config_.trace_config;
        BinaryTraceConfig binary_cfg;
        binary_cfg.compression_enabled = trace_cfg.compression.enabled;
        binary_cfg.compression_level = trace_cfg.compression.level;
        binary_cfg.block_size = trace_cfg.compression.block_size;
        binary_cfg.embed_schema = trace_cfg.embed_schema;
        binary_cfg.generate_index = trace_cfg.generate_index;

        binary_trace_writer_ = std::make_unique<BinaryTraceWriter>(binary_cfg);
        binary_trace_writer_->setSimulationName(config_.simulation_name);
        if (source_name_lookup_) {
            binary_trace_writer_->setSourceNameLookup(source_name_lookup_);
        }
        if (unit_type_lookup_) {
            binary_trace_writer_->setUnitTypeLookup(unit_type_lookup_);
        }
        binary_trace_writer_->open(output_dir_ / "events.ctrace");
    }

    if (config_.needsTextOutput()) {
        default_sink_ = std::make_unique<LogFileSink>();
        default_sink_->file.open(output_dir_ / "events.log");
        default_sink_->buffer.reserve(TEXT_BUFFER_FLUSH_SIZE * 2);

        for (auto& sink : channel_sink_) {
            sink = default_sink_.get();
        }

        auto getOrCreateSink = [&](const std::string& filename) -> LogFileSink* {
            auto it = custom_sinks_.find(filename);
            if (it != custom_sinks_.end()) {
                return it->second.get();
            }
            auto sink = std::make_unique<LogFileSink>();
            sink->file.open(output_dir_ / filename);
            sink->buffer.reserve(TEXT_BUFFER_FLUSH_SIZE * 2);
            auto* ptr = sink.get();
            custom_sinks_.emplace(filename, std::move(sink));
            return ptr;
        };

        if (!config_.trace_file.empty()) {
            channel_sink_[static_cast<size_t>(Channel::Trace)] =
                getOrCreateSink(config_.trace_file);
        }
        if (!config_.debug_file.empty()) {
            channel_sink_[static_cast<size_t>(Channel::Debug)] =
                getOrCreateSink(config_.debug_file);
        }
        if (!config_.info_file.empty()) {
            channel_sink_[static_cast<size_t>(Channel::Info)] = getOrCreateSink(config_.info_file);
        }
        if (!config_.warn_file.empty()) {
            channel_sink_[static_cast<size_t>(Channel::Warn)] = getOrCreateSink(config_.warn_file);
        }
        if (!config_.error_file.empty()) {
            channel_sink_[static_cast<size_t>(Channel::Error)] =
                getOrCreateSink(config_.error_file);
        }
    }
}

}  // namespace chronon::observe
