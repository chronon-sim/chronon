// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Compile-time format-string registration for zero-copy trace/log.
 *
 * Format strings are registered at static-initialization time and assigned a
 * unique 32-bit ID; the runtime transmits only ID + arg values, and the backend
 * thread reconstructs formatted messages.
 */

using FormatId = uint32_t;

constexpr FormatId INVALID_FORMAT_ID = 0;

enum class ArgType : uint8_t {
    None = 0,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float,
    Double,
    Pointer,
    String,
    Bool,
};

constexpr size_t MAX_FORMAT_ARGS = 8;

/**
 * @brief Pre-parsed segment of a format string.
 *
 * Format strings are parsed once into segments so reconstruction is O(segments)
 * instead of re-scanning the format char-by-char per event.
 */
struct FormatSegment {
    enum class Type : uint8_t { Literal, Placeholder };
    Type type;
    uint16_t start;     ///< Offset into format_string.
    uint16_t length;    ///< Literal length.
    uint8_t arg_index;  ///< Placeholder argument index.
    bool hex;           ///< Spec contains 'x'/'X' (pre-computed).
};

/** @brief Pre-parsed format string for fast message reconstruction; lazy-compiled. */
struct CompiledFormat {
    std::vector<FormatSegment> segments;
    bool compiled = false;

    void compile(const std::string& fmt_string) {
        segments.clear();
        segments.reserve(8);
        size_t i = 0;
        uint8_t arg_idx = 0;
        size_t literal_start = 0;

        while (i < fmt_string.size()) {
            if (fmt_string[i] == '{' && i + 1 < fmt_string.size()) {
                if (i > literal_start) {
                    segments.push_back({FormatSegment::Type::Literal,
                                        static_cast<uint16_t>(literal_start),
                                        static_cast<uint16_t>(i - literal_start), 0, false});
                }

                size_t j = i + 1;
                while (j < fmt_string.size() && fmt_string[j] != '}') {
                    ++j;
                }

                if (j < fmt_string.size()) {
                    bool hex = false;
                    for (size_t k = i + 1; k < j; ++k) {
                        if (fmt_string[k] == 'x' || fmt_string[k] == 'X') {
                            hex = true;
                            break;
                        }
                    }

                    segments.push_back({FormatSegment::Type::Placeholder, static_cast<uint16_t>(i),
                                        static_cast<uint16_t>(j - i + 1), arg_idx, hex});
                    arg_idx++;
                    i = j + 1;
                } else {
                    // No closing brace; treat '{' as a literal character.
                    i++;
                    continue;
                }
                literal_start = i;
            } else {
                i++;
            }
        }

        if (literal_start < fmt_string.size()) {
            segments.push_back({FormatSegment::Type::Literal, static_cast<uint16_t>(literal_start),
                                static_cast<uint16_t>(fmt_string.size() - literal_start), 0,
                                false});
        }

        compiled = true;
    }
};

/** @brief Metadata for a registered format string. */
struct FormatInfo {
    std::string format_string;
    std::string_view file;
    uint32_t line;
    uint8_t arg_count;
    std::array<ArgType, MAX_FORMAT_ARGS> arg_types;
    bool is_log;                      ///< true for LOG, false for TRACE.
    LogLevel log_level;               ///< Only valid if is_log == true.
    mutable CompiledFormat compiled;  ///< Lazy-compiled on first reconstruction.

    FormatInfo() = default;

    FormatInfo(std::string_view fmt, std::string_view f, uint32_t ln,
               std::initializer_list<ArgType> types, bool log = false,
               LogLevel level = LogLevel::Info)
        : format_string(fmt),
          file(f),
          line(ln),
          arg_count(static_cast<uint8_t>(std::min(types.size(), MAX_FORMAT_ARGS))),
          is_log(log),
          log_level(level) {
        arg_types.fill(ArgType::None);
        size_t i = 0;
        for (auto t : types) {
            if (i < MAX_FORMAT_ARGS) {
                arg_types[i++] = t;
            }
        }
    }
};

/** @brief Thread-safe global registry for format strings. */
class FormatRegistry {
public:
    static constexpr size_t MAX_FORMATS = 4096;

    static FormatRegistry& instance() {
        static FormatRegistry registry;
        return registry;
    }

    /**
     * @brief Register a format string and return its unique ID.
     * @return Format ID, or INVALID_FORMAT_ID on overflow.
     */
    FormatId registerFormat(std::string_view fmt, std::string_view file, uint32_t line,
                            std::initializer_list<ArgType> arg_types, bool is_log = false,
                            LogLevel level = LogLevel::Info) {
        std::lock_guard<std::mutex> lock(mutex_);

        FormatId id = next_id_++;
        if (id >= MAX_FORMATS) {
            return INVALID_FORMAT_ID;
        }

        formats_[id] = FormatInfo(fmt, file, line, arg_types, is_log, level);
        return id;
    }

    const FormatInfo& getFormat(FormatId id) const {
        if (id < next_id_.load(std::memory_order_relaxed)) {
            return formats_[id];
        }
        static FormatInfo empty;
        return empty;
    }

    size_t size() const { return next_id_.load(std::memory_order_relaxed); }

    template <typename Fn>
    void forEach(Fn&& fn) const {
        size_t count = next_id_.load(std::memory_order_relaxed);
        for (size_t i = 1; i < count; ++i) {
            fn(static_cast<FormatId>(i), formats_[i]);
        }
    }

private:
    /// next_id_ starts at 1 because 0 is reserved as INVALID_FORMAT_ID.
    FormatRegistry() : next_id_(1) { formats_.resize(MAX_FORMATS); }

    std::mutex mutex_;
    std::atomic<FormatId> next_id_;
    std::vector<FormatInfo> formats_;
};

/** @brief Helper for static-storage format registration. */
struct FormatRegistrar {
    FormatId id;

    FormatRegistrar(std::string_view fmt, std::string_view file, uint32_t line,
                    std::initializer_list<ArgType> arg_types, bool is_log = false,
                    LogLevel level = LogLevel::Info)
        : id(FormatRegistry::instance().registerFormat(fmt, file, line, arg_types, is_log, level)) {
    }
};

/** @brief Type-erased argument value (8 bytes max inline). Strings are stored separately. */
struct ArgValue {
    union {
        int32_t i32;
        int64_t i64;
        uint32_t u32;
        uint64_t u64;
        float f32;
        double f64;
        const void* ptr;
        bool b;
    };
    ArgType type;

    ArgValue() : u64(0), type(ArgType::None) {}

    explicit ArgValue(int32_t v) : i32(v), type(ArgType::Int32) {}
    explicit ArgValue(int64_t v) : i64(v), type(ArgType::Int64) {}
    explicit ArgValue(uint32_t v) : u32(v), type(ArgType::UInt32) {}
    explicit ArgValue(uint64_t v) : u64(v), type(ArgType::UInt64) {}
    explicit ArgValue(float v) : f32(v), type(ArgType::Float) {}
    explicit ArgValue(double v) : f64(v), type(ArgType::Double) {}
    explicit ArgValue(const void* v) : ptr(v), type(ArgType::Pointer) {}
    explicit ArgValue(bool v) : b(v), type(ArgType::Bool) {}
};

/// Maps a C++ type to its ArgType.
template <typename T>
struct ArgTypeOf;
template <>
struct ArgTypeOf<int32_t> {
    static constexpr ArgType value = ArgType::Int32;
};
template <>
struct ArgTypeOf<int64_t> {
    static constexpr ArgType value = ArgType::Int64;
};
template <>
struct ArgTypeOf<uint32_t> {
    static constexpr ArgType value = ArgType::UInt32;
};
template <>
struct ArgTypeOf<uint64_t> {
    static constexpr ArgType value = ArgType::UInt64;
};
template <>
struct ArgTypeOf<float> {
    static constexpr ArgType value = ArgType::Float;
};
template <>
struct ArgTypeOf<double> {
    static constexpr ArgType value = ArgType::Double;
};
template <>
struct ArgTypeOf<bool> {
    static constexpr ArgType value = ArgType::Bool;
};
template <>
struct ArgTypeOf<const char*> {
    static constexpr ArgType value = ArgType::String;
};
template <>
struct ArgTypeOf<char*> {
    static constexpr ArgType value = ArgType::String;
};
template <typename T>
struct ArgTypeOf<T*> {
    static constexpr ArgType value = ArgType::Pointer;
};

// Handle signed/unsigned char, short variations with proper sizes
template <>
struct ArgTypeOf<int8_t> {
    static constexpr ArgType value = ArgType::Int8;
};
template <>
struct ArgTypeOf<int16_t> {
    static constexpr ArgType value = ArgType::Int16;
};
template <>
struct ArgTypeOf<uint8_t> {
    static constexpr ArgType value = ArgType::UInt8;
};
template <>
struct ArgTypeOf<uint16_t> {
    static constexpr ArgType value = ArgType::UInt16;
};

// On Linux x86_64 (LP64), int64_t/uint64_t are typedefs for long/unsigned long,
// so the specializations above already cover them. On macOS (LP64 but int64_t =
// long long) and Windows (LLP64, long = 32-bit), long is a distinct type and
// needs its own specialization; sizeof-based selection picks the right ArgType
// where sizeof(long) != 8.
#if defined(__APPLE__) || defined(_WIN32)
template <>
struct ArgTypeOf<long> {
    static constexpr ArgType value = sizeof(long) == 8 ? ArgType::Int64 : ArgType::Int32;
};
template <>
struct ArgTypeOf<unsigned long> {
    static constexpr ArgType value = sizeof(unsigned long) == 8 ? ArgType::UInt64 : ArgType::UInt32;
};
#endif

/**
 * @brief Compact record header for queue transmission.
 *
 * Layout in queue: [RecordHeader][StructuredRecord][arg1]...[argN][strings...].
 */
struct StructuredRecord {
    uint64_t cycle;
    uint32_t format_id;
    uint32_t category;   ///< CategoryMask truncated to 32 bits.
    uint16_t source_id;  ///< Unit identifier (0 = unknown).
    uint8_t arg_count;
    uint8_t padding[1];
};

static_assert(sizeof(StructuredRecord) == 24, "StructuredRecord size mismatch");

}  // namespace chronon::observe
