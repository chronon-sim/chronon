// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <cstdint>
#include <iostream>
#include <stdexcept>

#include "RepresentativeWorkload.hpp"

namespace {

using chronon::benchmark::generateScenario;
using chronon::benchmark::PayloadClass;
using chronon::benchmark::ScenarioConfig;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void testStableFingerprint() {
    ScenarioConfig config;
    config.seed = 0x0123456789abcdefULL;
    config.num_units = 12;
    config.channels_per_unit = 3;
    config.work_period = 31;
    config.send_period = 73;
    const auto first = generateScenario(config);
    const auto second = generateScenario(config);
    require(first == second, "same config did not produce the same scenario");
    require(first.fingerprint == 0xbc3249d0d072cc13ULL,
            "generator output changed without a version bump");

    config.seed ^= 1;
    const auto changed = generateScenario(config);
    require(changed.fingerprint != first.fingerprint, "different seed reused fingerprint");
}

void testTopologyAndLoadInvariants() {
    ScenarioConfig config;
    config.seed = 987654321;
    config.num_units = 40;
    config.channels_per_unit = 4;
    config.zero_delay_probability_ppm = 600'000;
    config.broadcast_probability_ppm = 700'000;
    config.max_fanout = 7;
    const auto scenario = generateScenario(config);

    require(scenario.units.size() == config.num_units, "wrong unit count");
    require(scenario.channels.size() == config.num_units * config.channels_per_unit,
            "wrong channel count");
    for (const auto& unit : scenario.units) {
        require(unit.work_schedule.size() == config.work_period, "wrong work period");
        require(unit.working_set_bytes >= 64, "working set too small");
        require((unit.working_set_bytes & (unit.working_set_bytes - 1)) == 0,
                "working set is not a power of two");
        for (uint32_t work : unit.work_schedule) require(work > 0, "zero work sample");
    }
    for (const auto& channel : scenario.channels) {
        require(!channel.destinations.empty(), "channel has no destination");
        require(channel.send_schedule.size() == (config.send_period + 63) / 64,
                "wrong send period");
        require(
            static_cast<uint8_t>(channel.payload) <= static_cast<uint8_t>(PayloadClass::Bytes256),
            "invalid payload class");
        for (uint32_t destination : channel.destinations) {
            require(destination != channel.source, "self edge generated");
            if (channel.delay == 0) {
                require(channel.source < destination, "zero-delay graph is not a DAG");
            }
        }
    }
}

}  // namespace

int main() {
    try {
        testStableFingerprint();
        testTopologyAndLoadInvariants();
        std::cout << "Representative workload generator tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
