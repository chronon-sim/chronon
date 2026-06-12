// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

/// @file ArgFormat.hpp
/// @brief Shared utilities for computing argument sizes and formatting typed
///        arguments from structured trace records, used by ObservationBackend
///        (fmt::format_to path).

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "FormatRegistry.hpp"

namespace chronon::observe {

/**
 * @brief Compute the byte size of a serialized argument of the given type.
 *
 * For fixed-size types, the size is known statically. For strings, scans
 * forward to the null terminator.
 */
inline size_t argSize(ArgType type, const std::byte* data, const std::byte* end) {
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

/**
 * @brief Format a typed argument to a string using snprintf.
 *
 * This is the portable formatting path for consumers that do not depend
 * on fmt.
 */
inline std::string formatArgToString(const std::byte* data, ArgType type, bool hex) {
    char buf[32];
    switch (type) {
        case ArgType::Int8: {
            int8_t val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), hex ? "%x" : "%d", static_cast<int>(val));
            return buf;
        }
        case ArgType::Int16: {
            int16_t val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), hex ? "%x" : "%d", val);
            return buf;
        }
        case ArgType::Int32: {
            int32_t val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), hex ? "%x" : "%d", val);
            return buf;
        }
        case ArgType::Int64: {
            int64_t val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), hex ? "%lx" : "%ld", static_cast<long>(val));
            return buf;
        }
        case ArgType::UInt8: {
            uint8_t val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), hex ? "%x" : "%u", static_cast<unsigned>(val));
            return buf;
        }
        case ArgType::UInt16: {
            uint16_t val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), hex ? "%x" : "%u", val);
            return buf;
        }
        case ArgType::UInt32: {
            uint32_t val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), hex ? "%x" : "%u", val);
            return buf;
        }
        case ArgType::UInt64: {
            uint64_t val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), hex ? "%lx" : "%lu", static_cast<unsigned long>(val));
            return buf;
        }
        case ArgType::Float: {
            float val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), "%g", val);
            return buf;
        }
        case ArgType::Double: {
            double val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), "%g", val);
            return buf;
        }
        case ArgType::Pointer: {
            const void* val;
            std::memcpy(&val, data, sizeof(val));
            snprintf(buf, sizeof(buf), "0x%lx", reinterpret_cast<uintptr_t>(val));
            return buf;
        }
        case ArgType::String: {
            return std::string(reinterpret_cast<const char*>(data));
        }
        case ArgType::Bool: {
            bool val;
            std::memcpy(&val, data, sizeof(val));
            return val ? "true" : "false";
        }
        case ArgType::None:
        default:
            return "?";
    }
}

}  // namespace chronon::observe
