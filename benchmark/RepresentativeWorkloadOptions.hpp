// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "RepresentativeWorkload.hpp"

namespace chronon::benchmark {

struct RunOptions {
    uint64_t warmup_cycles = 8192;
    uint64_t measured_cycles = 50'000;
    bool dynamic_rebalance = false;
    bool precomputed_costs = true;
    uint32_t max_lookahead = 32;
};

struct CliOptions {
    std::string profile = "nucleus";
    uint64_t seed = 0x4348524f4e4f4eULL;
    bool random_seed = false;
    uint64_t scenario_offset = 0;
    uint32_t scenario_count = 1;
    uint32_t repetitions = 5;
    std::vector<size_t> workers = {1, 2, 4, 8};
    RunOptions run;
    bool describe_only = false;
    bool verbose = false;
};

struct ScenarioOverrides {
    std::optional<uint32_t> num_units;
    std::optional<uint32_t> channels_per_unit;
    std::optional<uint32_t> active_source_count;
    std::optional<uint32_t> forced_delay;
    std::optional<uint32_t> max_fanout;
    std::optional<uint32_t> send_probability_ppm;
    std::optional<uint32_t> burst_probability_ppm;
    std::optional<uint32_t> hotspot_probability_ppm;
    std::optional<uint32_t> broadcast_probability_ppm;
    std::optional<uint32_t> zero_delay_probability_ppm;
    std::optional<uint32_t> queue_capacity;
    std::optional<uint32_t> drain_limit;
    std::optional<uint32_t> median_work;
    std::optional<uint32_t> unit_sigma_milli;
    std::optional<uint32_t> cycle_sigma_milli;
    std::optional<uint32_t> working_set_bytes;
    std::optional<std::array<uint32_t, PAYLOAD_BYTES.size()>> payload_weights;

    void apply(ScenarioConfig& config) const noexcept {
        if (num_units) config.num_units = *num_units;
        if (channels_per_unit) config.channels_per_unit = *channels_per_unit;
        if (active_source_count) config.active_source_count = *active_source_count;
        if (forced_delay) config.forced_delay = *forced_delay;
        if (max_fanout) config.max_fanout = *max_fanout;
        if (send_probability_ppm) config.send_probability_ppm = *send_probability_ppm;
        if (burst_probability_ppm) config.burst_probability_ppm = *burst_probability_ppm;
        if (hotspot_probability_ppm) config.hotspot_probability_ppm = *hotspot_probability_ppm;
        if (broadcast_probability_ppm)
            config.broadcast_probability_ppm = *broadcast_probability_ppm;
        if (zero_delay_probability_ppm)
            config.zero_delay_probability_ppm = *zero_delay_probability_ppm;
        if (queue_capacity) config.queue_capacity = *queue_capacity;
        if (drain_limit) config.drain_limit = *drain_limit;
        if (median_work) config.median_work = *median_work;
        if (unit_sigma_milli) config.unit_sigma_milli = *unit_sigma_milli;
        if (cycle_sigma_milli) config.cycle_sigma_milli = *cycle_sigma_milli;
        if (working_set_bytes) config.working_set_bytes = *working_set_bytes;
        if (payload_weights) config.payload_weights = *payload_weights;
    }

    void print(std::ostream& output) const {
        const auto scalar = [&](std::string_view option, const std::optional<uint32_t>& value) {
            if (value) output << ' ' << option << ' ' << *value;
        };
        scalar("--units", num_units);
        scalar("--channels", channels_per_unit);
        scalar("--active-sources", active_source_count);
        scalar("--fixed-delay", forced_delay);
        scalar("--max-fanout", max_fanout);
        scalar("--send-ppm", send_probability_ppm);
        scalar("--burst-ppm", burst_probability_ppm);
        scalar("--hotspot-ppm", hotspot_probability_ppm);
        scalar("--broadcast-ppm", broadcast_probability_ppm);
        scalar("--zero-delay-ppm", zero_delay_probability_ppm);
        scalar("--queue-capacity", queue_capacity);
        scalar("--drain-limit", drain_limit);
        scalar("--work", median_work);
        scalar("--unit-sigma", unit_sigma_milli);
        scalar("--cycle-sigma", cycle_sigma_milli);
        scalar("--working-set", working_set_bytes);
        if (payload_weights) {
            output << " --payload-weights ";
            for (size_t i = 0; i < payload_weights->size(); ++i) {
                if (i != 0) output << ',';
                output << (*payload_weights)[i];
            }
        }
    }
};

struct ParsedOptions {
    CliOptions cli;
    ScenarioOverrides overrides;
};

[[nodiscard]] inline uint64_t parseInteger(std::string_view value, std::string_view option) {
    size_t consumed = 0;
    const uint64_t result = std::stoull(std::string(value), &consumed, 0);
    if (consumed != value.size()) {
        throw std::invalid_argument("invalid value for " + std::string(option));
    }
    return result;
}

[[nodiscard]] inline uint32_t parseU32(std::string_view value, std::string_view option) {
    const uint64_t result = parseInteger(value, option);
    if (result > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("value for " + std::string(option) + " exceeds uint32_t range");
    }
    return static_cast<uint32_t>(result);
}

[[nodiscard]] inline std::vector<uint32_t> parseU32List(std::string_view text,
                                                        std::string_view option) {
    std::vector<uint32_t> result;
    std::stringstream stream{std::string(text)};
    std::string item;
    while (std::getline(stream, item, ',')) result.push_back(parseU32(item, option));
    return result;
}

[[nodiscard]] inline std::vector<size_t> parseWorkers(std::string_view text) {
    const auto values = parseU32List(text, "--threads");
    std::vector<size_t> workers(values.begin(), values.end());
    if (workers.empty() || std::find(workers.begin(), workers.end(), 0) != workers.end()) {
        throw std::invalid_argument("worker counts must be positive");
    }
    std::sort(workers.begin(), workers.end());
    workers.erase(std::unique(workers.begin(), workers.end()), workers.end());
    return workers;
}

[[nodiscard]] inline ScenarioConfig profileConfig(std::string_view profile, uint64_t seed) {
    ScenarioConfig config;
    config.seed = seed;
    if (profile == "nucleus") return config;
    if (profile == "scheduler") {
        config.channels_per_unit = 0;
        config.ensure_ring = false;
        config.median_work = 16;
        config.working_set_bytes = 4096;
        config.drain_limit = 1;
        return config;
    }
    if (profile == "port") {
        config.channels_per_unit = 3;
        config.median_work = 1;
        config.unit_sigma_milli = 100;
        config.cycle_sigma_milli = 0;
        config.send_probability_ppm = 450'000;
        config.drain_limit = 32;
        config.queue_capacity = 1024;
        return config;
    }
    if (profile == "hotspot") {
        config.channels_per_unit = 3;
        config.hotspot_probability_ppm = 750'000;
        config.send_probability_ppm = 300'000;
        config.queue_capacity = 128;
        config.drain_limit = 4;
        return config;
    }
    if (profile == "broadcast") {
        config.channels_per_unit = 1;
        config.active_source_count = 1;
        config.ensure_ring = false;
        config.broadcast_probability_ppm = 1'000'000;
        config.hotspot_probability_ppm = 0;
        config.max_fanout = 16;
        config.forced_delay = 1;
        config.zero_delay_probability_ppm = 0;
        config.send_probability_ppm = 250'000;
        config.queue_capacity = 0;
        config.drain_limit = 32;
        return config;
    }
    if (profile == "backpressure") {
        config.channels_per_unit = 3;
        config.send_probability_ppm = 500'000;
        config.burst_probability_ppm = 15'000;
        config.burst_length = 8;
        config.hotspot_probability_ppm = 500'000;
        config.queue_capacity = 64;
        config.drain_limit = 2;
        return config;
    }
    if (profile == "saturation") {
        config.channels_per_unit = 3;
        config.send_probability_ppm = 700'000;
        config.burst_probability_ppm = 30'000;
        config.burst_length = 8;
        config.hotspot_probability_ppm = 600'000;
        config.queue_capacity = 32;
        config.drain_limit = 1;
        return config;
    }
    if (profile == "random") {
        config.num_units = 32 + bounded(randomWord(seed, 1, 0), 97);
        // Reserve one ring channel for connectivity and at least one independently
        // generated channel for topology, hotspot, and fan-out coverage.
        config.channels_per_unit = 2 + bounded(randomWord(seed, 1, 1), 3);
        config.max_fanout = 2 + bounded(randomWord(seed, 1, 2), 7);
        config.send_probability_ppm = 50'000 + bounded(randomWord(seed, 1, 3), 600'001);
        config.hotspot_probability_ppm = bounded(randomWord(seed, 1, 4), 700'001);
        config.broadcast_probability_ppm = bounded(randomWord(seed, 1, 5), 600'001);
        config.zero_delay_probability_ppm = bounded(randomWord(seed, 1, 6), 150'001);
        constexpr std::array<uint32_t, 4> capacities = {32, 128, 512, 0};
        config.queue_capacity = capacities[bounded(randomWord(seed, 1, 7), capacities.size())];
        config.median_work = 4 + bounded(randomWord(seed, 1, 8), 61);
        config.unit_sigma_milli = 250 + bounded(randomWord(seed, 1, 9), 651);
        config.cycle_sigma_milli = 50 + bounded(randomWord(seed, 1, 10), 351);
        config.drain_limit = 1 + bounded(randomWord(seed, 1, 11), 16);
        return config;
    }
    throw std::invalid_argument("unknown profile: " + std::string(profile));
}

inline void printHelp(const char* program) {
    std::cout << "Usage: " << program << " [options]\n\n"
              << "Profiles: nucleus, scheduler, port, hotspot, broadcast, backpressure, "
                 "saturation, random\n"
              << "Core options:\n"
              << "  --profile NAME              scenario profile (default nucleus)\n"
              << "  --seed N | --random-seed    reproducible or newly sampled base seed\n"
              << "  --scenario-offset N         first derived scenario index (default 0)\n"
              << "  --scenario-count N          independent derived scenarios (default 1)\n"
              << "  --threads 1,2,4,8           worker sweep; CPU affinity is external\n"
              << "  --cycles N --warmup N       measured and warmup cycles\n"
              << "  --repetitions N             interleaved repetitions (default 5)\n"
              << "  --rebalance                 enable runtime dynamic rebalance\n"
              << "  --no-precomputed-costs      start from uniform unit costs\n"
              << "  --describe-only             generate/validate without running\n"
              << "Scenario overrides:\n"
              << "  --units N --channels N --max-fanout N --send-ppm N --burst-ppm N\n"
              << "  --active-sources N --fixed-delay N\n"
              << "  --hotspot-ppm N --broadcast-ppm N --zero-delay-ppm N\n"
              << "  --queue-capacity N --drain-limit N --work N --unit-sigma N\n"
              << "  --cycle-sigma N --working-set N --payload-weights a,b,c,d,e,f\n"
              << "Other: --max-lookahead N --verbose --quick --help\n";
}

[[nodiscard]] inline ParsedOptions parseCommandLine(int argc, char** argv) {
    CliOptions cli;
    const auto requireValue = [&](int& index, std::string_view option) -> std::string_view {
        if (index + 1 >= argc)
            throw std::invalid_argument("missing value for " + std::string(option));
        return argv[++index];
    };

    for (int i = 1; i < argc; ++i) {
        const std::string_view option = argv[i];
        if (option == "--profile")
            cli.profile = requireValue(i, option);
        else if (option == "--seed")
            cli.seed = parseInteger(requireValue(i, option), option);
        else if (option == "--random-seed")
            cli.random_seed = true;
    }
    if (cli.random_seed) {
        std::random_device device;
        cli.seed = (static_cast<uint64_t>(device()) << 32) ^ device();
    }

    ScenarioOverrides overrides;
    for (int i = 1; i < argc; ++i) {
        const std::string_view option = argv[i];
        if (option == "--help") {
            printHelp(argv[0]);
            std::exit(0);
        } else if (option == "--profile" || option == "--seed") {
            ++i;
        } else if (option == "--random-seed") {
        } else if (option == "--scenario-offset") {
            cli.scenario_offset = parseInteger(requireValue(i, option), option);
        } else if (option == "--scenario-count") {
            cli.scenario_count = parseU32(requireValue(i, option), option);
        } else if (option == "--threads") {
            cli.workers = parseWorkers(requireValue(i, option));
        } else if (option == "--cycles") {
            cli.run.measured_cycles = parseInteger(requireValue(i, option), option);
        } else if (option == "--warmup") {
            cli.run.warmup_cycles = parseInteger(requireValue(i, option), option);
        } else if (option == "--repetitions") {
            cli.repetitions = parseU32(requireValue(i, option), option);
        } else if (option == "--max-lookahead") {
            cli.run.max_lookahead = parseU32(requireValue(i, option), option);
        } else if (option == "--rebalance") {
            cli.run.dynamic_rebalance = true;
        } else if (option == "--no-precomputed-costs") {
            cli.run.precomputed_costs = false;
        } else if (option == "--describe-only") {
            cli.describe_only = true;
        } else if (option == "--verbose") {
            cli.verbose = true;
        } else if (option == "--quick") {
            cli.run.warmup_cycles = 256;
            cli.run.measured_cycles = 2'000;
            cli.repetitions = 1;
        } else if (option == "--units") {
            overrides.num_units = parseU32(requireValue(i, option), option);
        } else if (option == "--channels") {
            overrides.channels_per_unit = parseU32(requireValue(i, option), option);
        } else if (option == "--active-sources") {
            overrides.active_source_count = parseU32(requireValue(i, option), option);
        } else if (option == "--fixed-delay") {
            overrides.forced_delay = parseU32(requireValue(i, option), option);
        } else if (option == "--max-fanout") {
            overrides.max_fanout = parseU32(requireValue(i, option), option);
        } else if (option == "--send-ppm") {
            overrides.send_probability_ppm = parseU32(requireValue(i, option), option);
        } else if (option == "--burst-ppm") {
            overrides.burst_probability_ppm = parseU32(requireValue(i, option), option);
        } else if (option == "--hotspot-ppm") {
            overrides.hotspot_probability_ppm = parseU32(requireValue(i, option), option);
        } else if (option == "--broadcast-ppm") {
            overrides.broadcast_probability_ppm = parseU32(requireValue(i, option), option);
        } else if (option == "--zero-delay-ppm") {
            overrides.zero_delay_probability_ppm = parseU32(requireValue(i, option), option);
        } else if (option == "--queue-capacity") {
            overrides.queue_capacity = parseU32(requireValue(i, option), option);
        } else if (option == "--drain-limit") {
            overrides.drain_limit = parseU32(requireValue(i, option), option);
        } else if (option == "--work") {
            overrides.median_work = parseU32(requireValue(i, option), option);
        } else if (option == "--unit-sigma") {
            overrides.unit_sigma_milli = parseU32(requireValue(i, option), option);
        } else if (option == "--cycle-sigma") {
            overrides.cycle_sigma_milli = parseU32(requireValue(i, option), option);
        } else if (option == "--working-set") {
            overrides.working_set_bytes = parseU32(requireValue(i, option), option);
        } else if (option == "--payload-weights") {
            const auto values = parseU32List(requireValue(i, option), option);
            if (values.size() != PAYLOAD_BYTES.size()) {
                throw std::invalid_argument(
                    "--payload-weights requires six comma-separated values");
            }
            std::array<uint32_t, PAYLOAD_BYTES.size()> weights{};
            std::copy(values.begin(), values.end(), weights.begin());
            overrides.payload_weights = weights;
        } else {
            throw std::invalid_argument("unknown option: " + std::string(option));
        }
    }
    if (cli.scenario_count == 0 || cli.repetitions == 0 || cli.run.measured_cycles == 0) {
        throw std::invalid_argument(
            "scenario count, repetitions, and measured cycles must be positive");
    }
    return {std::move(cli), std::move(overrides)};
}

[[nodiscard]] inline uint64_t scenarioSeed(uint64_t base_seed, uint64_t scenario_index) noexcept {
    return scenario_index == 0 ? base_seed : splitMix64(base_seed + scenario_index);
}

[[nodiscard]] inline ScenarioConfig scenarioConfigFor(const ParsedOptions& options,
                                                      uint64_t scenario_index) {
    ScenarioConfig config =
        profileConfig(options.cli.profile, scenarioSeed(options.cli.seed, scenario_index));
    options.overrides.apply(config);
    return config;
}

}  // namespace chronon::benchmark
