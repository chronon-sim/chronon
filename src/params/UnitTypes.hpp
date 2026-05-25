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
 * @brief Split a unit-suffixed numeric string into value and unit (e.g. "3GHz" → {3.0, "GHz"}).
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

}  // namespace detail

/**
 * @brief Frequency value with automatic unit parsing (Hz/kHz/MHz/GHz).
 *
 * @code
 * Frequency f("3GHz");
 * double ghz = f.ghz();  // 3.0
 * @endcode
 */
class Frequency {
public:
    explicit Frequency(const std::string& s) : hz_(0.0) { parse(s); }

    explicit Frequency(double hz) : hz_(hz) {
        if (hz_ < 0) {
            throw std::invalid_argument("Frequency cannot be negative");
        }
    }

    Frequency() : hz_(0.0) {}

    double hz() const { return hz_; }
    double khz() const { return hz_ / 1e3; }
    double mhz() const { return hz_ / 1e6; }
    double ghz() const { return hz_ / 1e9; }

    Frequency operator*(double scalar) const { return Frequency(hz_ * scalar); }
    Frequency operator/(double scalar) const { return Frequency(hz_ / scalar); }
    double operator/(const Frequency& other) const { return hz_ / other.hz_; }
    Frequency operator+(const Frequency& other) const { return Frequency(hz_ + other.hz_); }
    Frequency operator-(const Frequency& other) const { return Frequency(hz_ - other.hz_); }

    bool operator==(const Frequency& other) const { return std::abs(hz_ - other.hz_) < 1e-6; }
    bool operator!=(const Frequency& other) const { return !(*this == other); }
    bool operator<(const Frequency& other) const { return hz_ < other.hz_; }
    bool operator>(const Frequency& other) const { return hz_ > other.hz_; }

    std::string toString() const {
        if (hz_ >= 1e9) {
            return std::to_string(ghz()) + "GHz";
        } else if (hz_ >= 1e6) {
            return std::to_string(mhz()) + "MHz";
        } else if (hz_ >= 1e3) {
            return std::to_string(khz()) + "kHz";
        }
        return std::to_string(hz_) + "Hz";
    }

private:
    double hz_;

    void parse(const std::string& s) {
        auto [value, unit] = detail::parseValueAndUnit(s, "frequency");
        std::string lower_unit = detail::toLower(unit);

        if (lower_unit == "hz") {
            hz_ = value;
        } else if (lower_unit == "khz") {
            hz_ = value * 1e3;
        } else if (lower_unit == "mhz") {
            hz_ = value * 1e6;
        } else if (lower_unit == "ghz") {
            hz_ = value * 1e9;
        } else {
            throw std::invalid_argument("Unknown frequency unit: " + unit);
        }

        if (hz_ < 0) {
            throw std::invalid_argument("Frequency cannot be negative");
        }
    }
};

/**
 * @brief Latency/time value with automatic unit parsing (ns/us/ms/s).
 *
 * @code
 * Latency lat("10ns");
 * double us = lat.us();  // 0.01
 * @endcode
 */
class Latency {
public:
    explicit Latency(const std::string& s) : ns_(0.0) { parse(s); }

    explicit Latency(double ns) : ns_(ns) {
        if (ns_ < 0) {
            throw std::invalid_argument("Latency cannot be negative");
        }
    }

    Latency() : ns_(0.0) {}

    double ns() const { return ns_; }
    double us() const { return ns_ / 1e3; }
    double ms() const { return ns_ / 1e6; }
    double s() const { return ns_ / 1e9; }

    Latency operator*(double scalar) const { return Latency(ns_ * scalar); }
    Latency operator/(double scalar) const { return Latency(ns_ / scalar); }
    double operator/(const Latency& other) const { return ns_ / other.ns_; }
    Latency operator+(const Latency& other) const { return Latency(ns_ + other.ns_); }
    Latency operator-(const Latency& other) const { return Latency(ns_ - other.ns_); }

    bool operator==(const Latency& other) const { return std::abs(ns_ - other.ns_) < 1e-6; }
    bool operator!=(const Latency& other) const { return !(*this == other); }
    bool operator<(const Latency& other) const { return ns_ < other.ns_; }
    bool operator>(const Latency& other) const { return ns_ > other.ns_; }

    std::string toString() const {
        if (ns_ >= 1e9) {
            return std::to_string(s()) + "s";
        } else if (ns_ >= 1e6) {
            return std::to_string(ms()) + "ms";
        } else if (ns_ >= 1e3) {
            return std::to_string(us()) + "us";
        }
        return std::to_string(ns_) + "ns";
    }

private:
    double ns_;

    void parse(const std::string& s) {
        auto [value, unit] = detail::parseValueAndUnit(s, "latency");
        std::string lower_unit = detail::toLower(unit);

        if (lower_unit == "ns") {
            ns_ = value;
        } else if (lower_unit == "us") {
            ns_ = value * 1e3;
        } else if (lower_unit == "ms") {
            ns_ = value * 1e6;
        } else if (lower_unit == "s") {
            ns_ = value * 1e9;
        } else {
            throw std::invalid_argument("Unknown latency unit: " + unit);
        }

        if (ns_ < 0) {
            throw std::invalid_argument("Latency cannot be negative");
        }
    }
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

    void parse(const std::string& s) {
        auto [value, unit] = detail::parseValueAndUnit(s, "size");
        std::string lower_unit = detail::toLower(unit);

        if (unit == "B") {
            bytes_ = static_cast<uint64_t>(value);
        } else if (lower_unit == "kib") {
            bytes_ = static_cast<uint64_t>(value * 1024);
        } else if (lower_unit == "mib") {
            bytes_ = static_cast<uint64_t>(value * 1024 * 1024);
        } else if (lower_unit == "gib") {
            bytes_ = static_cast<uint64_t>(value * 1024 * 1024 * 1024);
        } else if (lower_unit == "kb") {
            bytes_ = static_cast<uint64_t>(value * 1000);
        } else if (lower_unit == "mb") {
            bytes_ = static_cast<uint64_t>(value * 1000000);
        } else if (lower_unit == "gb") {
            bytes_ = static_cast<uint64_t>(value * 1000000000);
        } else {
            throw std::invalid_argument("Unknown size unit: " + unit);
        }
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
class Bandwidth {
public:
    explicit Bandwidth(const std::string& s) : bytes_per_second_(0.0) { parse(s); }

    explicit Bandwidth(double bytes_per_second) : bytes_per_second_(bytes_per_second) {
        if (bytes_per_second_ < 0) {
            throw std::invalid_argument("Bandwidth cannot be negative");
        }
    }

    Bandwidth() : bytes_per_second_(0.0) {}

    double bytesPerSecond() const { return bytes_per_second_; }
    double kibPerSecond() const { return bytes_per_second_ / 1024.0; }
    double mibPerSecond() const { return bytes_per_second_ / (1024.0 * 1024.0); }
    double gibPerSecond() const { return bytes_per_second_ / (1024.0 * 1024.0 * 1024.0); }

    Bandwidth operator*(double scalar) const { return Bandwidth(bytes_per_second_ * scalar); }
    Bandwidth operator/(double scalar) const { return Bandwidth(bytes_per_second_ / scalar); }

    bool operator==(const Bandwidth& other) const {
        return std::abs(bytes_per_second_ - other.bytes_per_second_) < 1e-6;
    }
    bool operator!=(const Bandwidth& other) const { return !(*this == other); }
    bool operator<(const Bandwidth& other) const {
        return bytes_per_second_ < other.bytes_per_second_;
    }
    bool operator>(const Bandwidth& other) const {
        return bytes_per_second_ > other.bytes_per_second_;
    }

    std::string toString() const {
        if (bytes_per_second_ >= 1e9) {
            return std::to_string(gibPerSecond()) + "GiB/s";
        } else if (bytes_per_second_ >= 1e6) {
            return std::to_string(mibPerSecond()) + "MiB/s";
        } else if (bytes_per_second_ >= 1e3) {
            return std::to_string(kibPerSecond()) + "KiB/s";
        }
        return std::to_string(bytes_per_second_) + "B/s";
    }

private:
    double bytes_per_second_;

    void parse(const std::string& s) {
        if (s.empty()) {
            throw std::invalid_argument("Empty bandwidth string");
        }

        size_t slash_pos = s.find("/s");
        if (slash_pos == std::string::npos) {
            throw std::invalid_argument("Bandwidth must end with '/s': " + s);
        }

        std::string size_str = s.substr(0, slash_pos);
        Size size(size_str);
        bytes_per_second_ = static_cast<double>(size.bytes());
    }
};

inline std::string toString(const Frequency& f) { return f.toString(); }
inline std::string toString(const Latency& l) { return l.toString(); }
inline std::string toString(const Size& s) { return s.toString(); }
inline std::string toString(const Bandwidth& b) { return b.toString(); }

// Non-template overloads — chosen by ADL over the generic templates in Parameter.hpp.
inline Frequency fromString(const std::string& s, std::type_identity<Frequency>) {
    return Frequency(s);
}
inline Latency fromString(const std::string& s, std::type_identity<Latency>) { return Latency(s); }
inline Size fromString(const std::string& s, std::type_identity<Size>) { return Size(s); }
inline Bandwidth fromString(const std::string& s, std::type_identity<Bandwidth>) {
    return Bandwidth(s);
}

}  // namespace chronon::params
