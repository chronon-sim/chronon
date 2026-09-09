// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <compare>
#include <cstdint>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>

namespace chronon {

using ClockDomainId = uint32_t;

namespace clock_detail {
// GCC and Clang provide exact double-width intermediates on supported targets.
__extension__ using Wide = unsigned __int128;
inline uint64_t narrow(Wide value) {
    if (value > std::numeric_limits<uint64_t>::max()) {
        throw std::overflow_error("clock time exceeds its uint64 rational representation");
    }
    return static_cast<uint64_t>(value);
}
}  // namespace clock_detail

/// Nonnegative exact rational seconds. No floating point or accumulated rounding.
class SimTime {
public:
    SimTime(uint64_t numerator = 0, uint64_t denominator = 1) {
        if (!denominator) throw std::invalid_argument("SimTime denominator must be positive");
        const auto gcd = std::gcd(numerator, denominator);
        numerator_ = numerator / gcd;
        denominator_ = denominator / gcd;
    }
    static SimTime nanoseconds(uint64_t n) { return {n, 1'000'000'000}; }
    static SimTime picoseconds(uint64_t n) { return {n, 1'000'000'000'000}; }
    uint64_t numerator() const noexcept { return numerator_; }
    uint64_t denominator() const noexcept { return denominator_; }
    std::string str() const {
        return std::to_string(numerator_) + "/" + std::to_string(denominator_) + " s";
    }
    /// Perfetto display only: floor(seconds * 1e9), error in [0, 1) ns.
    uint64_t floorNanoseconds() const {
        const auto ns = clock_detail::Wide(numerator_) * 1'000'000'000 / denominator_;
        if (ns > uint64_t(std::numeric_limits<int64_t>::max())) {
            throw std::overflow_error("Perfetto timestamp exceeds signed 64-bit nanoseconds");
        }
        return static_cast<uint64_t>(ns);
    }
    friend bool operator==(SimTime, SimTime) = default;
    friend std::strong_ordering operator<=>(SimTime a, SimTime b) noexcept {
        const auto left = clock_detail::Wide(a.numerator_) * b.denominator_;
        const auto right = clock_detail::Wide(b.numerator_) * a.denominator_;
        return left < right   ? std::strong_ordering::less
               : left > right ? std::strong_ordering::greater
                              : std::strong_ordering::equal;
    }
    friend SimTime operator+(SimTime a, SimTime b) {
        const uint64_t gcd = std::gcd(a.denominator_, b.denominator_);
        const uint64_t scale = b.denominator_ / gcd;
        const auto denominator = clock_detail::narrow(clock_detail::Wide(a.denominator_) * scale);
        // Each summand is at most 128 bits; check before adding them.
        const auto x = clock_detail::Wide(a.numerator_) * scale;
        const auto y = clock_detail::Wide(b.numerator_) * (a.denominator_ / gcd);
        if (x > UINT64_MAX || y > UINT64_MAX || x + y > UINT64_MAX) {
            throw std::overflow_error("SimTime addition exceeds representation");
        }
        return {static_cast<uint64_t>(x + y), denominator};
    }

private:
    uint64_t numerator_ = 0;
    uint64_t denominator_ = 1;
};

/// Immutable static hardware clock. ID is supplied by the model, not allocation order.
class ClockDomain {
public:
    ClockDomain(ClockDomainId id, std::string name, SimTime period, SimTime phase = {})
        : id_(id), name_(std::move(name)), period_(period), phase_(phase) {
        if (!period.numerator()) throw std::invalid_argument("clock period must be positive");
        if (name_.empty() || name_ == "." || name_ == "..") {
            throw std::invalid_argument("clock name must be a nonempty filename component");
        }
        for (const unsigned char c : name_) {
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                  c == '-' || c == '_' || c == '.')) {
                throw std::invalid_argument("clock name permits ASCII letters, digits, ._- only");
            }
        }
        const auto gcd = std::gcd(period.denominator(), phase.denominator());
        denominator_ = clock_detail::narrow(clock_detail::Wide(period.denominator() / gcd) *
                                            phase.denominator());
        period_ticks_ = clock_detail::narrow(clock_detail::Wide(period.numerator()) *
                                             (denominator_ / period.denominator()));
        phase_ticks_ = clock_detail::narrow(clock_detail::Wide(phase.numerator()) *
                                            (denominator_ / phase.denominator()));
    }
    static ClockDomain fromHz(ClockDomainId id, std::string name, uint64_t hz_numerator,
                              uint64_t hz_denominator = 1, SimTime phase = {}) {
        if (!hz_numerator || !hz_denominator) {
            throw std::invalid_argument("clock frequency must be positive");
        }
        return {id, std::move(name), SimTime(hz_denominator, hz_numerator), phase};
    }
    ClockDomainId id() const noexcept { return id_; }
    const std::string& name() const noexcept { return name_; }
    SimTime period() const noexcept { return period_; }
    SimTime phase() const noexcept { return phase_; }
    uint64_t maxEdgeIndex() const noexcept { return (UINT64_MAX - phase_ticks_) / period_ticks_; }
    SimTime edge(uint64_t n) const {
        return {clock_detail::narrow(clock_detail::Wide(n) * period_ticks_ + phase_ticks_),
                denominator_};
    }
    /// First n >= 0 with E(n) >= t. O(1) double-width integer arithmetic.
    uint64_t edgeAtOrAfter(SimTime t) const { return query_(t, false); }
    /// First n >= 0 with E(n) > t. Same-time updates are excluded from sampling.
    uint64_t edgeAfter(SimTime t) const { return query_(t, true); }

private:
    uint64_t query_(SimTime t, bool strict) const {
        const auto target = clock_detail::Wide(t.numerator()) * denominator_;
        const auto phase = clock_detail::Wide(phase_ticks_) * t.denominator();
        if (target < phase || (!strict && target == phase)) return 0;
        const auto delta = target - phase;
        const auto step = clock_detail::Wide(period_ticks_) * t.denominator();
        auto index = delta / step;
        if (strict || delta % step != 0) ++index;
        if (index > maxEdgeIndex()) throw std::overflow_error("clock edge query out of range");
        return static_cast<uint64_t>(index);
    }
    ClockDomainId id_;
    std::string name_;
    SimTime period_, phase_;
    uint64_t denominator_, period_ticks_, phase_ticks_;
};

}  // namespace chronon
