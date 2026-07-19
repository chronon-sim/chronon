// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

namespace chronon::benchmark {

inline constexpr uint32_t REPRESENTATIVE_WORKLOAD_VERSION = 2;
inline constexpr uint32_t PROBABILITY_SCALE = 1'000'000;
inline constexpr std::array<uint32_t, 6> PAYLOAD_BYTES = {8, 16, 32, 64, 144, 256};

enum class PayloadClass : uint8_t { Bytes8, Bytes16, Bytes32, Bytes64, Bytes144, Bytes256 };

struct ScenarioConfig {
    uint64_t seed = 0x4348524f4e4f4eULL;
    uint32_t num_units = 64;
    uint32_t channels_per_unit = 2;
    uint32_t active_source_count = 0;  // zero means every unit
    uint32_t max_fanout = 4;
    uint32_t forced_delay = std::numeric_limits<uint32_t>::max();
    uint32_t send_probability_ppm = 180'000;
    uint32_t burst_probability_ppm = 5'000;
    uint32_t burst_length = 4;
    uint32_t hotspot_probability_ppm = 150'000;
    uint32_t broadcast_probability_ppm = 100'000;
    uint32_t zero_delay_probability_ppm = 50'000;
    uint32_t queue_capacity = 256;  // zero means unlimited
    uint32_t drain_limit = 8;
    uint32_t median_work = 16;
    uint32_t unit_sigma_milli = 550;
    uint32_t cycle_sigma_milli = 180;
    uint32_t normal_cap_milli = 2'580;
    uint32_t working_set_bytes = 8 * 1024;
    uint32_t work_period = 256;
    uint32_t send_period = 1024;
    bool ensure_ring = true;
    std::array<uint32_t, PAYLOAD_BYTES.size()> payload_weights = {18, 22, 10, 13, 32, 5};

    friend bool operator==(const ScenarioConfig&, const ScenarioConfig&) = default;
};

struct UnitSpec {
    uint32_t id = 0;
    uint32_t base_work = 0;
    uint32_t drain_limit = 0;
    uint32_t working_set_bytes = 0;
    std::vector<uint32_t> work_schedule;

    friend bool operator==(const UnitSpec&, const UnitSpec&) = default;
};

struct ChannelSpec {
    uint32_t id = 0;
    uint32_t source = 0;
    PayloadClass payload = PayloadClass::Bytes8;
    uint32_t delay = 1;
    uint32_t send_probability_ppm = 0;
    std::vector<uint32_t> destinations;
    std::vector<uint64_t> send_schedule;

    [[nodiscard]] bool scheduled(uint64_t cycle, uint32_t period) const noexcept {
        const uint64_t slot = cycle % period;
        return ((send_schedule[slot / 64] >> (slot % 64)) & uint64_t{1}) != 0;
    }

    friend bool operator==(const ChannelSpec&, const ChannelSpec&) = default;
};

struct ScenarioSummary {
    uint64_t scheduled_slots = 0;
    uint64_t scheduled_deliveries = 0;
    uint64_t scheduled_payload_bytes = 0;
    uint32_t zero_delay_channels = 0;
    uint32_t broadcast_channels = 0;
    uint32_t max_indegree = 0;
    uint32_t max_outdegree = 0;

    friend bool operator==(const ScenarioSummary&, const ScenarioSummary&) = default;
};

struct Scenario {
    ScenarioConfig config;
    std::vector<UnitSpec> units;
    std::vector<ChannelSpec> channels;
    ScenarioSummary summary;
    uint64_t fingerprint = 0;

    friend bool operator==(const Scenario&, const Scenario&) = default;
};

[[nodiscard]] inline uint64_t splitMix64(uint64_t value) noexcept {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}

[[nodiscard]] inline uint64_t randomWord(uint64_t seed, uint64_t domain, uint64_t index) noexcept {
    return splitMix64(seed ^ splitMix64(domain) ^ splitMix64(index));
}

[[nodiscard]] inline uint32_t bounded(uint64_t word, uint32_t bound) noexcept {
    return bound == 0 ? 0 : static_cast<uint32_t>(word % bound);
}

[[nodiscard]] inline bool chance(uint64_t word, uint32_t probability_ppm) noexcept {
    return bounded(word, PROBABILITY_SCALE) < probability_ppm;
}

namespace detail {

// exp(x), Q16 input and output. Range reduction and a seven-term Taylor
// polynomial keep scenario generation integer-only and bit-reproducible.
[[nodiscard]] inline uint32_t expQ16(int64_t x_q16) noexcept {
    constexpr int64_t kOneQ30 = int64_t{1} << 30;
    constexpr int64_t kLn2Q30 = 744'261'118;
    constexpr int64_t kMaxXQ16 = int64_t{8} << 16;
    x_q16 = std::clamp(x_q16, -kMaxXQ16, kMaxXQ16);
    const int64_t x_q30 = x_q16 * (int64_t{1} << 14);
    const int64_t n =
        x_q30 >= 0 ? (x_q30 + kLn2Q30 / 2) / kLn2Q30 : (x_q30 - kLn2Q30 / 2) / kLn2Q30;
    const int64_t remainder = x_q30 - n * kLn2Q30;

    int64_t term = kOneQ30;
    int64_t sum = term;
    for (int64_t divisor = 1; divisor <= 7; ++divisor) {
        term = (term * remainder / kOneQ30) / divisor;
        sum += term;
    }
    if (n >= 0) {
        sum <<= static_cast<unsigned>(n);
    } else {
        sum >>= static_cast<unsigned>(-n);
    }
    const int64_t q16 = (sum + (int64_t{1} << 13)) >> 14;
    return static_cast<uint32_t>(std::clamp<int64_t>(q16, 1, UINT32_MAX));
}

// Twelve independent U(0,1) variates minus six approximate N(0,1), while
// remaining bounded. The explicit cap removes the lognormal's extreme tail.
[[nodiscard]] inline uint32_t lognormalQ16(uint64_t seed, uint64_t domain, uint64_t index,
                                           uint32_t sigma_milli,
                                           uint32_t normal_cap_milli) noexcept {
    int64_t normal_q16 = -6 * 65'535;
    for (uint64_t sample = 0; sample < 12; ++sample) {
        normal_q16 +=
            static_cast<int64_t>(randomWord(seed, domain + sample, index) & uint64_t{0xffff});
    }
    const int64_t cap_q16 = static_cast<int64_t>(normal_cap_milli) * 65'536 / 1'000;
    normal_q16 = std::clamp(normal_q16, -cap_q16, cap_q16);
    const int64_t exponent_q16 = normal_q16 * sigma_milli / 1'000;
    return expQ16(exponent_q16);
}

[[nodiscard]] inline uint32_t scaleByQ16(uint32_t value, uint32_t multiplier) noexcept {
    const uint64_t scaled = (static_cast<uint64_t>(value) * multiplier + 32'768) >> 16;
    return static_cast<uint32_t>(std::clamp<uint64_t>(scaled, 1, UINT32_MAX));
}

[[nodiscard]] inline uint32_t roundUpPowerOfTwo(uint32_t value) noexcept {
    value = std::max<uint32_t>(value, 64);
    if (std::has_single_bit(value)) return value;
    if (value > (uint32_t{1} << 30)) return uint32_t{1} << 30;
    return std::bit_ceil(value);
}

[[nodiscard]] inline PayloadClass choosePayload(const ScenarioConfig& config, uint64_t word) {
    uint64_t total = 0;
    for (uint32_t weight : config.payload_weights) total += weight;
    if (total == 0) throw std::invalid_argument("payload weights must not all be zero");
    uint64_t draw = word % total;
    for (size_t i = 0; i < config.payload_weights.size(); ++i) {
        if (draw < config.payload_weights[i]) return static_cast<PayloadClass>(i);
        draw -= config.payload_weights[i];
    }
    return PayloadClass::Bytes256;
}

[[nodiscard]] inline uint32_t payloadBytes(PayloadClass payload) noexcept {
    return PAYLOAD_BYTES[static_cast<size_t>(payload)];
}

inline void setScheduled(std::vector<uint64_t>& schedule, uint32_t slot) noexcept {
    schedule[slot / 64] |= uint64_t{1} << (slot % 64);
}

inline void buildSchedule(ChannelSpec& channel, const ScenarioConfig& config) {
    channel.send_schedule.assign((config.send_period + 63) / 64, 0);
    constexpr uint64_t kOfferDomain = 0x4f4646455253ULL;
    constexpr uint64_t kBurstDomain = 0x425552535453ULL;
    for (uint32_t slot = 0; slot < config.send_period; ++slot) {
        const uint64_t index = static_cast<uint64_t>(channel.id) * config.send_period + slot;
        if (chance(randomWord(config.seed, kOfferDomain, index), channel.send_probability_ppm)) {
            setScheduled(channel.send_schedule, slot);
        }
        if (chance(randomWord(config.seed, kBurstDomain, index), config.burst_probability_ppm)) {
            for (uint32_t offset = 0; offset < config.burst_length; ++offset) {
                setScheduled(channel.send_schedule, (slot + offset) % config.send_period);
            }
        }
    }
}

[[nodiscard]] inline uint32_t chooseDestination(const ScenarioConfig& config, uint32_t source,
                                                uint32_t delay, uint32_t channel_id,
                                                uint32_t fanout_slot,
                                                const std::vector<uint32_t>& used) noexcept {
    constexpr uint64_t kHotspotDomain = 0x484f5453504f54ULL;
    constexpr uint64_t kTargetDomain = 0x544152474554ULL;
    const uint64_t base_index = static_cast<uint64_t>(channel_id) * 64 + fanout_slot;
    for (uint32_t attempt = 0; attempt < 32; ++attempt) {
        const uint64_t index = base_index + static_cast<uint64_t>(attempt) * 0x10000ULL;
        uint32_t candidate = 0;
        if (chance(randomWord(config.seed, kHotspotDomain, index),
                   config.hotspot_probability_ppm)) {
            const uint32_t hotspot_count = std::max<uint32_t>(1, config.num_units / 16);
            const uint32_t hotspot =
                bounded(randomWord(config.seed, kTargetDomain, index), hotspot_count);
            candidate = (hotspot * config.num_units / hotspot_count) % config.num_units;
        } else if (delay == 0 && source + 1 < config.num_units) {
            candidate = source + 1 +
                        bounded(randomWord(config.seed, kTargetDomain, index),
                                config.num_units - source - 1);
        } else {
            candidate = bounded(randomWord(config.seed, kTargetDomain, index), config.num_units);
        }
        if (candidate == source || (delay == 0 && candidate <= source)) continue;
        if (std::find(used.begin(), used.end(), candidate) == used.end()) return candidate;
    }

    for (uint32_t candidate = 0; candidate < config.num_units; ++candidate) {
        if (candidate == source || (delay == 0 && candidate <= source)) continue;
        if (std::find(used.begin(), used.end(), candidate) == used.end()) return candidate;
    }
    return source;
}

class Fingerprint {
public:
    void add(uint64_t value) noexcept {
        for (unsigned byte = 0; byte < 8; ++byte) {
            hash_ ^= static_cast<uint8_t>(value >> (byte * 8));
            hash_ *= 1'099'511'628'211ULL;
        }
    }
    [[nodiscard]] uint64_t value() const noexcept { return hash_; }

private:
    uint64_t hash_ = 1'469'598'103'934'665'603ULL;
};

inline void validateConfig(const ScenarioConfig& config) {
    if (config.num_units == 0 || (config.num_units == 1 && config.channels_per_unit != 0)) {
        throw std::invalid_argument("at least two units are required when channels are enabled");
    }
    if (config.work_period == 0 || config.send_period == 0 || config.drain_limit == 0) {
        throw std::invalid_argument("periods and drain limit must be positive");
    }
    if (config.working_set_bytes > (uint32_t{1} << 28)) {
        throw std::invalid_argument("base working set is too large");
    }
    if (config.normal_cap_milli > 6'000 || config.unit_sigma_milli > 2'000 ||
        config.cycle_sigma_milli > 2'000) {
        throw std::invalid_argument("load distribution parameters exceed deterministic range");
    }
    const std::array probabilities = {
        config.send_probability_ppm, config.burst_probability_ppm, config.hotspot_probability_ppm,
        config.broadcast_probability_ppm, config.zero_delay_probability_ppm};
    for (uint32_t probability : probabilities) {
        if (probability > PROBABILITY_SCALE) {
            throw std::invalid_argument(
                "probabilities are expressed in ppm and must be <= 1000000");
        }
    }
}

inline uint64_t fingerprintScenario(const Scenario& scenario) noexcept {
    Fingerprint hash;
    hash.add(REPRESENTATIVE_WORKLOAD_VERSION);
    const auto& c = scenario.config;
    hash.add(c.seed);
    hash.add(c.num_units);
    hash.add(c.channels_per_unit);
    hash.add(c.active_source_count);
    hash.add(c.max_fanout);
    hash.add(c.forced_delay);
    hash.add(c.send_probability_ppm);
    hash.add(c.burst_probability_ppm);
    hash.add(c.burst_length);
    hash.add(c.hotspot_probability_ppm);
    hash.add(c.broadcast_probability_ppm);
    hash.add(c.zero_delay_probability_ppm);
    hash.add(c.queue_capacity);
    hash.add(c.drain_limit);
    hash.add(c.median_work);
    hash.add(c.unit_sigma_milli);
    hash.add(c.cycle_sigma_milli);
    hash.add(c.normal_cap_milli);
    hash.add(c.working_set_bytes);
    hash.add(c.work_period);
    hash.add(c.send_period);
    hash.add(c.ensure_ring);
    for (uint32_t weight : c.payload_weights) hash.add(weight);
    for (const auto& unit : scenario.units) {
        hash.add(unit.id);
        hash.add(unit.base_work);
        hash.add(unit.drain_limit);
        hash.add(unit.working_set_bytes);
        for (uint32_t work : unit.work_schedule) hash.add(work);
    }
    for (const auto& channel : scenario.channels) {
        hash.add(channel.id);
        hash.add(channel.source);
        hash.add(static_cast<uint8_t>(channel.payload));
        hash.add(channel.delay);
        hash.add(channel.send_probability_ppm);
        hash.add(channel.destinations.size());
        for (uint32_t destination : channel.destinations) hash.add(destination);
        for (uint64_t word : channel.send_schedule) hash.add(word);
    }
    return hash.value();
}

}  // namespace detail

[[nodiscard]] inline uint32_t payloadBytes(PayloadClass payload) noexcept {
    return detail::payloadBytes(payload);
}

[[nodiscard]] inline Scenario generateScenario(ScenarioConfig config) {
    detail::validateConfig(config);
    Scenario scenario;
    scenario.config = config;
    scenario.units.reserve(config.num_units);

    constexpr uint64_t kUnitLoadDomain = 0x554e49544c4f4144ULL;
    constexpr uint64_t kCycleLoadDomain = 0x4359434c454c4f41ULL;
    constexpr uint64_t kWorkingSetDomain = 0x574f524b534554ULL;
    for (uint32_t unit_id = 0; unit_id < config.num_units; ++unit_id) {
        UnitSpec unit;
        unit.id = unit_id;
        unit.base_work = detail::scaleByQ16(
            config.median_work,
            detail::lognormalQ16(config.seed, kUnitLoadDomain, unit_id, config.unit_sigma_milli,
                                 config.normal_cap_milli));
        unit.drain_limit = std::max<uint32_t>(
            1, detail::scaleByQ16(
                   config.drain_limit,
                   detail::lognormalQ16(config.seed, kUnitLoadDomain + 32, unit_id, 180, 1'500)));
        const uint32_t working_set_scale =
            1U << bounded(randomWord(config.seed, kWorkingSetDomain, unit_id), 3);
        unit.working_set_bytes =
            detail::roundUpPowerOfTwo(config.working_set_bytes * working_set_scale);
        unit.work_schedule.reserve(config.work_period);
        for (uint32_t slot = 0; slot < config.work_period; ++slot) {
            const uint64_t index = static_cast<uint64_t>(unit_id) * config.work_period + slot;
            unit.work_schedule.push_back(detail::scaleByQ16(
                unit.base_work,
                detail::lognormalQ16(config.seed, kCycleLoadDomain, index, config.cycle_sigma_milli,
                                     config.normal_cap_milli)));
        }
        scenario.units.push_back(std::move(unit));
    }

    constexpr uint64_t kPayloadDomain = 0x5041594c4f4144ULL;
    constexpr uint64_t kDelayDomain = 0x44454c4159ULL;
    constexpr uint64_t kFanoutDomain = 0x46414e4f5554ULL;
    constexpr uint64_t kRateDomain = 0x52415445ULL;
    const uint32_t source_count = config.active_source_count == 0
                                      ? config.num_units
                                      : std::min(config.num_units, config.active_source_count);
    for (uint32_t source = 0; source < source_count; ++source) {
        for (uint32_t local = 0; local < config.channels_per_unit; ++local) {
            ChannelSpec channel;
            channel.id = static_cast<uint32_t>(scenario.channels.size());
            channel.source = source;
            channel.payload =
                detail::choosePayload(config, randomWord(config.seed, kPayloadDomain, channel.id));

            const bool ring = config.ensure_ring && local == 0;
            if (ring) {
                channel.delay = 1;
                channel.destinations.push_back((source + 1) % config.num_units);
            } else {
                if (config.forced_delay != std::numeric_limits<uint32_t>::max()) {
                    channel.delay = config.forced_delay;
                } else {
                    const bool wants_zero =
                        chance(randomWord(config.seed, kDelayDomain, channel.id),
                               config.zero_delay_probability_ppm) &&
                        source + 1 < config.num_units;
                    if (wants_zero) {
                        channel.delay = 0;
                    } else {
                        constexpr std::array<uint32_t, 8> kDelays = {1, 1, 1, 1, 2, 2, 4, 8};
                        channel.delay = kDelays[bounded(
                            randomWord(config.seed, kDelayDomain + 1, channel.id), kDelays.size())];
                    }
                }
                if (channel.delay == 0 && source + 1 >= config.num_units) channel.delay = 1;
                uint32_t fanout = 1;
                if (config.max_fanout > 1 &&
                    chance(randomWord(config.seed, kFanoutDomain, channel.id),
                           config.broadcast_probability_ppm)) {
                    fanout = 2 + bounded(randomWord(config.seed, kFanoutDomain + 1, channel.id),
                                         config.max_fanout - 1);
                }
                fanout = std::min<uint32_t>(fanout, config.num_units - 1);
                for (uint32_t slot = 0; slot < fanout; ++slot) {
                    const uint32_t destination = detail::chooseDestination(
                        config, source, channel.delay, channel.id, slot, channel.destinations);
                    if (destination != source) channel.destinations.push_back(destination);
                }
            }

            const uint32_t rate_multiplier =
                detail::lognormalQ16(config.seed, kRateDomain, channel.id, 300, 2'000);
            channel.send_probability_ppm = std::min<uint32_t>(
                950'000, detail::scaleByQ16(config.send_probability_ppm, rate_multiplier));
            detail::buildSchedule(channel, config);
            scenario.channels.push_back(std::move(channel));
        }
    }

    std::vector<uint32_t> indegree(config.num_units, 0);
    std::vector<uint32_t> outdegree(config.num_units, 0);
    for (const auto& channel : scenario.channels) {
        if (channel.delay == 0) ++scenario.summary.zero_delay_channels;
        if (channel.destinations.size() > 1) ++scenario.summary.broadcast_channels;
        outdegree[channel.source] += static_cast<uint32_t>(channel.destinations.size());
        uint64_t slots = 0;
        for (uint64_t word : channel.send_schedule) slots += std::popcount(word);
        scenario.summary.scheduled_slots += slots;
        scenario.summary.scheduled_deliveries += slots * channel.destinations.size();
        scenario.summary.scheduled_payload_bytes +=
            slots * channel.destinations.size() * payloadBytes(channel.payload);
        for (uint32_t destination : channel.destinations) ++indegree[destination];
    }
    scenario.summary.max_indegree = *std::max_element(indegree.begin(), indegree.end());
    scenario.summary.max_outdegree = *std::max_element(outdegree.begin(), outdegree.end());
    scenario.fingerprint = detail::fingerprintScenario(scenario);
    return scenario;
}

}  // namespace chronon::benchmark
