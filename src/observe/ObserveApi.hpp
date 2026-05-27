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

#include "FormatRegistry.hpp"
#include "ObservationContext.hpp"
#include "Types.hpp"

namespace chronon::observe {

/** @brief Compile-time string literal usable as a non-type template parameter. */
template <size_t N>
struct FixedString {
    char data[N]{};

    constexpr FixedString() = default;

    constexpr FixedString(const char (&str)[N]) {
        for (size_t i = 0; i < N; ++i) {
            data[i] = str[i];
        }
    }

    constexpr operator std::string_view() const noexcept { return std::string_view(data, N - 1); }

    constexpr const char* c_str() const noexcept { return data; }
    constexpr size_t size() const noexcept { return N - 1; }
    constexpr bool operator==(const FixedString&) const = default;
};

template <size_t N>
FixedString(const char (&)[N]) -> FixedString<N>;

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
 *   trace<"Hit at addr=0x{:x}">(CACHE_HIT, addr);
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

/**
 * @brief Emit a trace event with compile-time format string.
 *
 * @code
 *   trace<"Cache HIT: addr=0x{:x}">(ctx, CACHE_HIT, addr);
 * @endcode
 */
template <FixedString Fmt, typename Cat, typename... Args>
inline void trace(ObservationContext* ctx, Cat category, Args&&... args) {
    if (!ctx) return;

    CategoryMask cat_mask = static_cast<CategoryMask>(category);

    if (OBSERVE_UNLIKELY(ctx->shouldTrace(cat_mask))) {
        static FormatId fmt_id = Format<Fmt>::template getIdWithTypes<Args...>("", 0, false);
        ctx->trace(cat_mask, fmt_id, std::forward<Args>(args)...);
    }
}

template <FixedString Fmt, typename... Args>
inline void log_debug(ObservationContext* ctx, Args&&... args) {
    if (!ctx) return;
    if (ctx->template shouldLog<LogLevel::Debug>()) {
        static FormatId fmt_id =
            Format<Fmt>::template getIdWithTypes<Args...>("", 0, true, LogLevel::Debug);
        ctx->template log<LogLevel::Debug>(fmt_id, std::forward<Args>(args)...);
    }
}

template <FixedString Fmt, typename... Args>
inline void log_info(ObservationContext* ctx, Args&&... args) {
    if (!ctx) return;
    if (ctx->template shouldLog<LogLevel::Info>()) {
        static FormatId fmt_id =
            Format<Fmt>::template getIdWithTypes<Args...>("", 0, true, LogLevel::Info);
        ctx->template log<LogLevel::Info>(fmt_id, std::forward<Args>(args)...);
    }
}

template <FixedString Fmt, typename... Args>
inline void log_warn(ObservationContext* ctx, Args&&... args) {
    if (!ctx) return;
    if (ctx->template shouldLog<LogLevel::Warn>()) {
        static FormatId fmt_id =
            Format<Fmt>::template getIdWithTypes<Args...>("", 0, true, LogLevel::Warn);
        ctx->template log<LogLevel::Warn>(fmt_id, std::forward<Args>(args)...);
    }
}

template <FixedString Fmt, typename... Args>
inline void log_error(ObservationContext* ctx, Args&&... args) {
    if (!ctx) return;
    if (ctx->template shouldLog<LogLevel::Error>()) {
        static FormatId fmt_id =
            Format<Fmt>::template getIdWithTypes<Args...>("", 0, true, LogLevel::Error);
        ctx->template log<LogLevel::Error>(fmt_id, std::forward<Args>(args)...);
    }
}

}  // namespace chronon::observe
