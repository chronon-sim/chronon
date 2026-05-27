// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>

namespace chronon::params {

namespace detail {

/**
 * @brief Split a unit-suffixed numeric string into value and unit (e.g. "3GHz" -> {3.0, "GHz"}).
 * @throws std::invalid_argument if the string is empty or has no numeric prefix.
 */
inline std::pair<double, std::string> parseValueAndUnit(const std::string& s,
                                                        const char* type_name) {
    if (s.empty()) {
        throw std::invalid_argument(std::string("Empty ") + type_name + " string");
    }

    size_t unit_pos = 0;
    for (size_t i = 0; i < s.size(); ++i) {
        if (std::isalpha(s[i])) {
            unit_pos = i;
            break;
        }
    }

    if (unit_pos == 0) {
        throw std::invalid_argument(std::string("No numeric value in ") + type_name + ": " + s);
    }

    double value = std::stod(s.substr(0, unit_pos));
    std::string unit = s.substr(unit_pos);

    return {value, unit};
}

inline std::string toLower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    return s;
}

/**
 * @brief Scale entry mapping a unit suffix to a multiplier.
 */
struct ScaleEntry {
    double multiplier;
    const char* suffix;        ///< Display suffix (canonical casing).
    const char* lower_suffix;  ///< Lowercased suffix for case-insensitive parse.
    bool case_sensitive;       ///< If true, match exact suffix (e.g. "B" vs "b").
};

/**
 * @brief CRTP base for scaled-unit types (Frequency, Latency, Bandwidth).
 *
 * Derived must provide:
 *   - static constexpr const char* type_name();
 *   - static const std::array<ScaleEntry, N>& scaleTable();
 *     (ordered from largest to smallest multiplier for toString())
 *   - static constexpr bool non_negative();
 */
template <typename Derived>
class ScaledUnitBase {
public:
    ScaledUnitBase() : value_(0.0) {}

    explicit ScaledUnitBase(double base_value) : value_(base_value) {
        if constexpr (Derived::non_negative()) {
            if (value_ < 0) {
                throw std::invalid_argument(std::string(Derived::type_name()) +
                                            " cannot be negative");
            }
        }
    }

    explicit ScaledUnitBase(const std::string& s) : value_(0.0) { parse(s); }

    double baseValue() const { return value_; }

    Derived operator*(double scalar) const { return Derived(value_ * scalar); }
    Derived operator/(double scalar) const { return Derived(value_ / scalar); }
    double operator/(const Derived& other) const { return value_ / other.baseValue(); }
    Derived operator+(const Derived& other) const { return Derived(value_ + other.baseValue()); }
    Derived operator-(const Derived& other) const { return Derived(value_ - other.baseValue()); }

    bool operator==(const Derived& other) const {
        return std::abs(value_ - other.baseValue()) < 1e-6;
    }
    bool operator!=(const Derived& other) const { return !(*this == other); }
    bool operator<(const Derived& other) const { return value_ < other.baseValue(); }
    bool operator>(const Derived& other) const { return value_ > other.baseValue(); }
    bool operator<=(const Derived& other) const { return value_ <= other.baseValue(); }
    bool operator>=(const Derived& other) const { return value_ >= other.baseValue(); }

    std::string toString() const {
        const auto& table = Derived::scaleTable();
        for (const auto& entry : table) {
            if (value_ >= entry.multiplier) {
                return std::to_string(value_ / entry.multiplier) + entry.suffix;
            }
        }
        // Fallback: use last entry (smallest unit)
        const auto& last = table.back();
        return std::to_string(value_ / last.multiplier) + last.suffix;
    }

protected:
    double value_;

    void parse(const std::string& s) {
        auto [val, unit] = detail::parseValueAndUnit(s, Derived::type_name());
        std::string lower_unit = detail::toLower(unit);

        const auto& table = Derived::scaleTable();
        for (const auto& entry : table) {
            bool match =
                entry.case_sensitive ? (unit == entry.suffix) : (lower_unit == entry.lower_suffix);
            if (match) {
                value_ = val * entry.multiplier;
                if constexpr (Derived::non_negative()) {
                    if (value_ < 0) {
                        throw std::invalid_argument(std::string(Derived::type_name()) +
                                                    " cannot be negative");
                    }
                }
                return;
            }
        }
        throw std::invalid_argument(std::string("Unknown ") + Derived::type_name() +
                                    " unit: " + unit);
    }
};

}  // namespace detail

/**
 * @brief Frequency value with automatic unit parsing (Hz/kHz/MHz/GHz).
 *
 * @code
 * Frequency f("3GHz");
 * double ghz = f.ghz();  // 3.0
 * @endcode
 */
class Frequency : public detail::ScaledUnitBase<Frequency> {
    using Base = detail::ScaledUnitBase<Frequency>;
    friend Base;

    static constexpr const char* type_name() { return "frequency"; }
    static constexpr bool non_negative() { return true; }

    static const auto& scaleTable() {
        static const std::array<detail::ScaleEntry, 4> table = {{
            {1e9, "GHz", "ghz", false},
            {1e6, "MHz", "mhz", false},
            {1e3, "kHz", "khz", false},
            {1.0, "Hz", "hz", false},
        }};
        return table;
    }

public:
    Frequency() : Base() {}
    explicit Frequency(const std::string& s) : Base(s) {}
    explicit Frequency(double hz) : Base(hz) {}

    double hz() const { return value_; }
    double khz() const { return value_ / 1e3; }
    double mhz() const { return value_ / 1e6; }
    double ghz() const { return value_ / 1e9; }
};

/**
 * @brief Latency/time value with automatic unit parsing (ns/us/ms/s).
 *
 * @code
 * Latency lat("10ns");
 * double us = lat.us();  // 0.01
 * @endcode
 */
class Latency : public detail::ScaledUnitBase<Latency> {
    using Base = detail::ScaledUnitBase<Latency>;
    friend Base;

    static constexpr const char* type_name() { return "latency"; }
    static constexpr bool non_negative() { return true; }

    static const auto& scaleTable() {
        static const std::array<detail::ScaleEntry, 4> table = {{
            {1e9, "s", "s", false},
            {1e6, "ms", "ms", false},
            {1e3, "us", "us", false},
            {1.0, "ns", "ns", false},
        }};
        return table;
    }

public:
    Latency() : Base() {}
    explicit Latency(const std::string& s) : Base(s) {}
    explicit Latency(double ns) : Base(ns) {}

    double ns() const { return value_; }
    double us() const { return value_ / 1e3; }
    double ms() const { return value_ / 1e6; }
    double s() const { return value_ / 1e9; }
};

/**
 * @brief Memory size with automatic unit parsing (B/KiB/MiB/GiB/KB/MB/GB).
 *
 * Both binary (1024-based) and decimal (1000-based) suffixes are accepted.
 *
 * @code
 * Size cache("64KiB");
 * uint64_t bytes = cache.bytes();  // 65536
 * @endcode
 */
class Size {
public:
    explicit Size(const std::string& s) : bytes_(0) { parse(s); }
    explicit Size(uint64_t bytes) : bytes_(bytes) {}
    Size() : bytes_(0) {}

    uint64_t bytes() const { return bytes_; }
    double kib() const { return bytes_ / 1024.0; }
    double mib() const { return bytes_ / (1024.0 * 1024.0); }
    double gib() const { return bytes_ / (1024.0 * 1024.0 * 1024.0); }
    double kb() const { return bytes_ / 1000.0; }
    double mb() const { return bytes_ / 1000000.0; }
    double gb() const { return bytes_ / 1000000000.0; }

    Size operator*(uint64_t scalar) const { return Size(bytes_ * scalar); }
    Size operator/(uint64_t scalar) const { return Size(bytes_ / scalar); }
    uint64_t operator/(const Size& other) const { return bytes_ / other.bytes_; }
    Size operator+(const Size& other) const { return Size(bytes_ + other.bytes_); }
    Size operator-(const Size& other) const { return Size(bytes_ - other.bytes_); }

    bool operator==(const Size& other) const { return bytes_ == other.bytes_; }
    bool operator!=(const Size& other) const { return bytes_ != other.bytes_; }
    bool operator<(const Size& other) const { return bytes_ < other.bytes_; }
    bool operator>(const Size& other) const { return bytes_ > other.bytes_; }

    std::string toString() const {
        if (bytes_ >= (1ULL << 30)) {
            return std::to_string(gib()) + "GiB";
        } else if (bytes_ >= (1ULL << 20)) {
            return std::to_string(mib()) + "MiB";
        } else if (bytes_ >= (1ULL << 10)) {
            return std::to_string(kib()) + "KiB";
        }
        return std::to_string(bytes_) + "B";
    }

private:
    uint64_t bytes_;

    struct SizeScale {
        double multiplier;
        const char* suffix;
        const char* lower_suffix;
        bool case_sensitive;
    };

    static const auto& scaleTable() {
        static const std::array<SizeScale, 7> table = {{
            {1024.0 * 1024.0 * 1024.0, "GiB", "gib", false},
            {1024.0 * 1024.0, "MiB", "mib", false},
            {1024.0, "KiB", "kib", false},
            {1000000000.0, "GB", "gb", false},
            {1000000.0, "MB", "mb", false},
            {1000.0, "KB", "kb", false},
            {1.0, "B", "B", true},
        }};
        return table;
    }

    void parse(const std::string& s) {
        auto [value, unit] = detail::parseValueAndUnit(s, "size");
        std::string lower_unit = detail::toLower(unit);

        const auto& table = scaleTable();
        for (const auto& entry : table) {
            bool match =
                entry.case_sensitive ? (unit == entry.suffix) : (lower_unit == entry.lower_suffix);
            if (match) {
                bytes_ = static_cast<uint64_t>(value * entry.multiplier);
                return;
            }
        }
        throw std::invalid_argument("Unknown size unit: " + unit);
    }
};

/**
 * @brief Bandwidth value with automatic unit parsing ("<size>/s").
 *
 * @code
 * Bandwidth bw("10GB/s");
 * double bps = bw.bytesPerSecond();
 * @endcode
 */
class Bandwidth : public detail::ScaledUnitBase<Bandwidth> {
    using Base = detail::ScaledUnitBase<Bandwidth>;
    friend Base;

    static constexpr const char* type_name() { return "bandwidth"; }
    static constexpr bool non_negative() { return true; }

    static const auto& scaleTable() {
        static const std::array<detail::ScaleEntry, 4> table = {{
            {1024.0 * 1024.0 * 1024.0, "GiB/s", "gib/s", false},
            {1024.0 * 1024.0, "MiB/s", "mib/s", false},
            {1024.0, "KiB/s", "kib/s", false},
            {1.0, "B/s", "b/s", false},
        }};
        return table;
    }

public:
    Bandwidth() : Base() {}

    explicit Bandwidth(double bytes_per_second) : Base(bytes_per_second) {}

    explicit Bandwidth(const std::string& s) : Base() {
        if (s.empty()) {
            throw std::invalid_argument("Empty bandwidth string");
        }

        size_t slash_pos = s.find("/s");
        if (slash_pos == std::string::npos) {
            throw std::invalid_argument("Bandwidth must end with '/s': " + s);
        }

        std::string size_str = s.substr(0, slash_pos);
        Size size(size_str);
        value_ = static_cast<double>(size.bytes());
    }

    double bytesPerSecond() const { return value_; }
    double kibPerSecond() const { return value_ / 1024.0; }
    double mibPerSecond() const { return value_ / (1024.0 * 1024.0); }
    double gibPerSecond() const { return value_ / (1024.0 * 1024.0 * 1024.0); }
};

inline std::string toString(const Frequency& f) { return f.toString(); }
inline std::string toString(const Latency& l) { return l.toString(); }
inline std::string toString(const Size& s) { return s.toString(); }
inline std::string toString(const Bandwidth& b) { return b.toString(); }

// Non-template overloads -- chosen by ADL over the generic templates in Parameter.hpp.
inline Frequency fromString(const std::string& s, std::type_identity<Frequency>) {
    return Frequency(s);
}
inline Latency fromString(const std::string& s, std::type_identity<Latency>) { return Latency(s); }
inline Size fromString(const std::string& s, std::type_identity<Size>) { return Size(s); }
inline Bandwidth fromString(const std::string& s, std::type_identity<Bandwidth>) {
    return Bandwidth(s);
}

}  // namespace chronon::params
