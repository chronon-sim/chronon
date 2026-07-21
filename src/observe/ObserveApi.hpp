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
#include <source_location>
#include <string_view>
#include <utility>

#include "FixedString.hpp"
#include "FormatRegistry.hpp"
#include "ObservationContext.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Global registry that auto-assigns bit positions for user trace categories.
 *
 * User categories start at bit USER_CATEGORY_START (8); bits 0-7 are reserved.
 */
class CategoryRegistry {
public:
    struct CategoryEntry {
        std::string_view name;
        std::string_view description;
        CategoryMask mask;
        uint32_t bit;
    };

    static CategoryRegistry& instance() {
        static CategoryRegistry registry;
        return registry;
    }

    CategoryMask registerCategory(std::string_view name, std::string_view description) {
        std::lock_guard<std::mutex> lock(mutex_);
        uint32_t bit = next_bit_++;
        CategoryMask mask = 1ULL << bit;
        entries_.push_back({name, description, mask, bit});
        return mask;
    }

    const std::vector<CategoryEntry>& entries() const { return entries_; }

    /// Thread-safe lookup of a category name by its assigned bit position.
    /// Returned views reference the categories' static string literals.
    std::string_view nameForBit(uint32_t bit) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& entry : entries_) {
            if (entry.bit == bit) {
                return entry.name;
            }
        }
        return {};
    }

private:
    CategoryRegistry() : next_bit_(category::USER_CATEGORY_START) {}
    std::mutex mutex_;
    std::atomic<uint32_t> next_bit_;
    std::vector<CategoryEntry> entries_;
};

/**
 * @brief Trace category with automatic bit-position assignment at startup.
 *
 * @code
 *   inline const auto CACHE_HIT = Category<"cache_hit", "Cache hit events">{};
 *   unit.event<"cache_hit">(CACHE_HIT, arg<"addr">(addr));
 * @endcode
 */
template <FixedString Name, FixedString Desc = "">
class Category {
public:
    static constexpr std::string_view name = Name;
    static constexpr std::string_view description = Desc;

    CategoryMask mask() const noexcept { return mask_; }
    operator CategoryMask() const noexcept { return mask_; }

    Category() noexcept : mask_(getOrRegister()) {}

private:
    static CategoryMask getOrRegister() {
        // Function-local static ensures one registration per template instantiation.
        static CategoryMask mask = CategoryRegistry::instance().registerCategory(Name, Desc);
        return mask;
    }

    CategoryMask mask_;
};

/**
 * @brief Compile-time format string wrapper with lazy id registration.
 *
 * Each unique format string is a unique type, giving each call site its own
 * static FormatId cache.
 */
template <FixedString Fmt>
struct Format {
    static constexpr std::string_view value = Fmt;

    static FormatId getId(std::string_view file = "", uint32_t line = 0, bool is_log = false,
                          LogLevel level = LogLevel::Info) {
        static FormatId id =
            FormatRegistry::instance().registerFormat(Fmt, file, line, {}, is_log, level);
        return id;
    }

    /// Version with argument types, used to properly reconstruct binary trace records.
    template <typename... ArgTypes>
    static FormatId getIdWithTypes(std::string_view file = "", uint32_t line = 0,
                                   bool is_log = false, LogLevel level = LogLevel::Info) {
        static FormatId id = FormatRegistry::instance().registerFormat(
            Fmt, file, line, {ArgTypeOf<std::decay_t<ArgTypes>>::value...}, is_log, level);
        return id;
    }
};

template <LogLevel Level, FixedString Fmt, typename... Args>
inline void log(ObservationContext* ctx, Args&&... args) {
    if (!ctx) return;
    if (ctx->template shouldLog<Level>()) {
        static FormatId fmt_id = Format<Fmt>::template getIdWithTypes<Args...>("", 0, true, Level);
        ctx->template log<Level>(fmt_id, std::forward<Args>(args)...);
    }
}

template <FixedString Fmt, typename... Args>
inline void log_debug(ObservationContext* ctx, Args&&... args) {
    log<LogLevel::Debug, Fmt>(ctx, std::forward<Args>(args)...);
}

template <FixedString Fmt, typename... Args>
inline void log_info(ObservationContext* ctx, Args&&... args) {
    log<LogLevel::Info, Fmt>(ctx, std::forward<Args>(args)...);
}

template <FixedString Fmt, typename... Args>
inline void log_warn(ObservationContext* ctx, Args&&... args) {
    log<LogLevel::Warn, Fmt>(ctx, std::forward<Args>(args)...);
}

template <FixedString Fmt, typename... Args>
inline void log_error(ObservationContext* ctx, Args&&... args) {
    log<LogLevel::Error, Fmt>(ctx, std::forward<Args>(args)...);
}

}  // namespace chronon::observe
