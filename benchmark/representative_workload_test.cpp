// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <algorithm>
#include <array>
#include <cstdint>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "RepresentativeWorkload.hpp"
#include "RepresentativeWorkloadCalibration.hpp"
#include "RepresentativeWorkloadOptions.hpp"

namespace {

using chronon::benchmark::calibrateMeasuredCycles;
using chronon::benchmark::generateScenario;
using chronon::benchmark::inputScratchBytesPerSlot;
using chronon::benchmark::MAX_BENCHMARK_CYCLES;
using chronon::benchmark::MAX_BENCHMARK_LOOKAHEAD;
using chronon::benchmark::MAX_BENCHMARK_REPETITIONS;
using chronon::benchmark::MAX_BENCHMARK_SCENARIOS;
using chronon::benchmark::MAX_BENCHMARK_TARGET_SECONDS;
using chronon::benchmark::MAX_BENCHMARK_WORKERS;
using chronon::benchmark::MAX_DRAIN_LIMIT;
using chronon::benchmark::MAX_FINITE_QUEUE_CAPACITY;
using chronon::benchmark::MAX_FORCED_DELAY;
using chronon::benchmark::MAX_GENERATED_WORK;
using chronon::benchmark::MAX_MEDIAN_WORK;
using chronon::benchmark::MAX_SCENARIO_UNITS;
using chronon::benchmark::MAX_TOTAL_PORT_STORAGE_BYTES;
using chronon::benchmark::MAX_TOTAL_WORKING_SET_BYTES;
using chronon::benchmark::MEMORY_ACCESS_PATTERN_COUNT;
using chronon::benchmark::MemoryAccessPattern;
using chronon::benchmark::parseCommandLine;
using chronon::benchmark::ParsedOptions;
using chronon::benchmark::PAYLOAD_BYTES;
using chronon::benchmark::PayloadClass;
using chronon::benchmark::payloadMask;
using chronon::benchmark::printReplayCommand;
using chronon::benchmark::RunOptions;
using chronon::benchmark::ScenarioConfig;
using chronon::benchmark::scenarioConfigFor;
using chronon::benchmark::transportFifoStorageBytes;
using chronon::benchmark::transportLaneStorageBytes;
using chronon::benchmark::UnitKernel;

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
    require(first.fingerprint == 0xd0c1ef7aa6cccc29ULL,
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
    uint64_t expected_unit_working_set = 0;
    std::array<uint32_t, MEMORY_ACCESS_PATTERN_COUNT> expected_pattern_counts{};
    std::vector<uint32_t> working_sets;
    for (const auto& unit : scenario.units) {
        require(unit.work_schedule.size() == config.work_period, "wrong work period");
        require(unit.working_set_bytes >= 64, "working set too small");
        require((unit.working_set_bytes & (unit.working_set_bytes - 1)) == 0,
                "working set is not a power of two");
        require(unit.hot_set_bytes >= 64 && unit.hot_set_bytes <= unit.working_set_bytes,
                "hot set is outside the unit footprint");
        require(static_cast<size_t>(unit.memory_pattern) < MEMORY_ACCESS_PATTERN_COUNT,
                "invalid memory access pattern");
        if (unit.memory_pattern != MemoryAccessPattern::HotColdRmw) {
            require(unit.hot_set_bytes == unit.working_set_bytes,
                    "non-hot/cold unit generated a smaller hot set");
        }
        expected_unit_working_set += unit.working_set_bytes;
        ++expected_pattern_counts[static_cast<size_t>(unit.memory_pattern)];
        working_sets.push_back(unit.working_set_bytes);
        for (uint32_t work : unit.work_schedule) require(work > 0, "zero work sample");
    }
    std::sort(working_sets.begin(), working_sets.end());
    require(scenario.summary.unit_working_set_bytes == expected_unit_working_set,
            "unit footprint total is inaccurate");
    require(scenario.summary.min_unit_working_set_bytes == working_sets.front() &&
                scenario.summary.median_unit_working_set_bytes ==
                    working_sets[(working_sets.size() + 1) / 2 - 1] &&
                scenario.summary.p90_unit_working_set_bytes ==
                    working_sets[(9 * working_sets.size() + 9) / 10 - 1] &&
                scenario.summary.max_unit_working_set_bytes == working_sets.back(),
            "unit footprint distribution summary is inaccurate");
    require(scenario.summary.memory_pattern_counts == expected_pattern_counts,
            "memory pattern summary is inaccurate");
    for (const auto& channel : scenario.channels) {
        require(!channel.destinations.empty(), "channel has no destination");
        require(channel.send_schedule.size() == (config.send_period + 63) / 64,
                "wrong send period");
        require(
            static_cast<uint8_t>(channel.payload) <= static_cast<uint8_t>(PayloadClass::Bytes256),
            "invalid payload class");
        for (uint32_t destination : channel.destinations) {
            require(destination != channel.source, "self edge generated");
            require((scenario.units[destination].incoming_payload_mask &
                     payloadMask(channel.payload)) != 0,
                    "destination payload mask omitted a connected type");
            if (channel.delay == 0) {
                require(channel.source < destination, "zero-delay graph is not a DAG");
            }
        }
    }

    uint64_t expected_input_scratch = 0;
    uint64_t expected_transport = 0;
    for (const auto& channel : scenario.channels) {
        expected_transport += channel.destinations.size() *
                              transportLaneStorageBytes(channel.payload, config.queue_capacity);
    }
    for (const auto& unit : scenario.units) {
        for (size_t payload = 0; payload < PAYLOAD_BYTES.size(); ++payload) {
            const auto payload_class = static_cast<PayloadClass>(payload);
            if ((unit.incoming_payload_mask & payloadMask(payload_class)) == 0) continue;
            expected_input_scratch += static_cast<uint64_t>(config.queue_capacity) *
                                      inputScratchBytesPerSlot(payload_class);
            expected_transport += transportFifoStorageBytes(payload_class, config.queue_capacity);
        }
    }
    require(scenario.summary.estimated_input_scratch_reserve_bytes == expected_input_scratch,
            "input scratch estimate did not match connected payload types");
    require(scenario.summary.estimated_transport_reserve_bytes == expected_transport,
            "transport estimate did not match connections and bounded input FIFOs");
    require(expected_input_scratch + expected_transport <= MAX_TOTAL_PORT_STORAGE_BYTES,
            "accepted scenario exceeded aggregate port storage budget");
}

ParsedOptions parseArgs(std::vector<std::string> arguments) {
    std::vector<char*> argv;
    argv.reserve(arguments.size());
    for (auto& argument : arguments) argv.push_back(argument.data());
    return parseCommandLine(static_cast<int>(argv.size()), argv.data());
}

bool rejects(std::vector<std::string> arguments) {
    try {
        (void)parseArgs(std::move(arguments));
        return false;
    } catch (const std::invalid_argument&) {
        return true;
    }
}

bool rejectsScenario(std::vector<std::string> arguments) {
    try {
        const auto options = parseArgs(std::move(arguments));
        (void)generateScenario(scenarioConfigFor(options, 0));
        return false;
    } catch (const std::invalid_argument&) {
        return true;
    }
}

void testRandomProfileResamplingAndOverrides() {
    ParsedOptions options;
    options.cli.profile = "random";
    options.cli.seed = 20260719;
    const auto first = scenarioConfigFor(options, 0);
    const auto second = scenarioConfigFor(options, 1);
    require(first.seed != second.seed, "derived scenario reused the base seed");
    require(first.num_units != second.num_units ||
                first.channels_per_unit != second.channels_per_unit ||
                first.max_fanout != second.max_fanout ||
                first.send_probability_ppm != second.send_probability_ppm ||
                first.queue_capacity != second.queue_capacity ||
                first.median_work != second.median_work ||
                first.working_set_bytes != second.working_set_bytes ||
                first.working_set_sigma_milli != second.working_set_sigma_milli ||
                first.memory_pattern_weights != second.memory_pattern_weights,
            "random scenario did not resample its parameter envelope");

    options.overrides.num_units = 17;
    options.overrides.send_probability_ppm = 123'456;
    options.overrides.queue_capacity = 64;
    options.overrides.working_set_bytes = 32 * 1024;
    options.overrides.working_set_sigma_milli = 333;
    options.overrides.memory_pattern_weights = std::array<uint32_t, 4>{1, 2, 3, 4};
    for (uint64_t index = 0; index < 3; ++index) {
        const auto overridden = scenarioConfigFor(options, index);
        require(overridden.num_units == 17, "unit override was not replayed");
        require(overridden.send_probability_ppm == 123'456, "send override was not replayed");
        require(overridden.queue_capacity == 64, "capacity override was not replayed");
        require(
            overridden.working_set_bytes == 32 * 1024 && overridden.working_set_sigma_milli == 333,
            "working-set override was not replayed");
        require(overridden.memory_pattern_weights == std::array<uint32_t, 4>{1, 2, 3, 4},
                "memory-pattern override was not replayed");
    }
}

void testSchedulerFloorProfile() {
    ParsedOptions options;
    options.cli.profile = "scheduler-floor";
    options.cli.seed = 24;
    const auto config = scenarioConfigFor(options, 0);
    require(config.unit_kernel == UnitKernel::SchedulerFloor,
            "scheduler-floor profile selected the representative unit kernel");
    require(config.channels_per_unit == 0 && !config.ensure_ring,
            "scheduler-floor profile enabled port topology");
    require(
        config.unit_sigma_milli == 0 && config.cycle_sigma_milli == 0 && config.work_period == 1,
        "scheduler-floor profile generated variable work");

    const auto scenario = generateScenario(config);
    require(scenario.channels.empty(), "scheduler-floor scenario generated channels");
    require(scenario.summary.estimated_input_scratch_reserve_bytes == 0 &&
                scenario.summary.estimated_transport_reserve_bytes == 0,
            "scheduler-floor scenario reserved port storage");
    require(scenario.summary.unit_working_set_bytes == 0,
            "scheduler-floor scenario materialized a unit footprint");
    for (const auto& unit : scenario.units) {
        require(unit.work_schedule == std::vector<uint32_t>{config.median_work},
                "scheduler-floor unit did not preserve fixed work");
        require(unit.working_set_bytes == 0 && unit.hot_set_bytes == 0 &&
                    unit.memory_pattern == MemoryAccessPattern::None,
                "scheduler-floor unit enabled the memory model");
    }

    ScenarioConfig invalid = config;
    invalid.channels_per_unit = 1;
    try {
        (void)generateScenario(invalid);
        require(false, "scheduler-floor scenario accepted channels");
    } catch (const std::invalid_argument&) {
    }
}

void testMemoryFootprintModel() {
    ParsedOptions profile_options;
    profile_options.cli.profile = "memory";
    const auto memory_profile = scenarioConfigFor(profile_options, 0);
    require(memory_profile.channels_per_unit == 0 && !memory_profile.ensure_ring &&
                memory_profile.working_set_bytes == 1024 * 1024,
            "memory profile did not select its connection-free large footprint");

    ScenarioConfig config;
    config.seed = 0x4d454d4f5259ULL;
    config.num_units = 256;
    config.channels_per_unit = 0;
    config.ensure_ring = false;
    config.working_set_bytes = 64 * 1024;
    config.working_set_sigma_milli = 650;
    config.work_period = 1;
    config.send_period = 1;
    const auto first = generateScenario(config);
    const auto second = generateScenario(config);
    require(first == second, "memory footprint generation is not reproducible");

    bool observed_below_median = false;
    bool observed_above_median = false;
    uint32_t pattern_kinds = 0;
    for (uint32_t count : first.summary.memory_pattern_counts) pattern_kinds += count != 0;
    for (const auto& unit : first.units) {
        observed_below_median =
            observed_below_median || unit.working_set_bytes < config.working_set_bytes;
        observed_above_median =
            observed_above_median || unit.working_set_bytes > config.working_set_bytes;
    }
    require(observed_below_median && observed_above_median,
            "bounded lognormal footprint did not vary around its median");
    require(pattern_kinds == MEMORY_ACCESS_PATTERN_COUNT,
            "mixed memory model did not generate every access pattern");
    require(first.summary.unit_working_set_bytes <= MAX_TOTAL_WORKING_SET_BYTES,
            "memory model exceeded its aggregate footprint budget");
    require(first.summary.min_unit_working_set_bytes >= config.working_set_bytes / 8 &&
                first.summary.max_unit_working_set_bytes <= config.working_set_bytes * 8,
            "bounded footprint distribution developed an extreme tail");

    config.num_units = 8;
    config.working_set_bytes = 4 * 1024;
    config.working_set_sigma_milli = 0;
    config.memory_pattern_weights = {0, 0, 1, 0};
    const auto pointer_only = generateScenario(config);
    for (const auto& unit : pointer_only.units) {
        require(unit.working_set_bytes == config.working_set_bytes &&
                    unit.memory_pattern == MemoryAccessPattern::PointerChase,
                "forced pointer-chase footprint was not preserved");
    }

    config.memory_pattern_weights = {0, 0, 0, 0};
    try {
        (void)generateScenario(config);
        require(false, "zero memory-pattern weights were accepted");
    } catch (const std::invalid_argument&) {
    }

    config.memory_pattern_weights = {1, 1, 1, 1};
    config.working_set_sigma_milli = 2'001;
    try {
        (void)generateScenario(config);
        require(false, "oversized working-set sigma was accepted");
    } catch (const std::invalid_argument&) {
    }
    config.working_set_sigma_milli = 0;
    config.hot_access_probability_ppm = 1'000'001;
    try {
        (void)generateScenario(config);
        require(false, "oversized hot-access probability was accepted");
    } catch (const std::invalid_argument&) {
    }
}

void testUint32CliBounds() {
    constexpr const char* too_large = "4294967296";
    constexpr std::array scalar_options = {
        "--scenario-count", "--repetitions",    "--max-lookahead", "--units",
        "--channels",       "--active-sources", "--fixed-delay",   "--max-fanout",
        "--send-ppm",       "--burst-ppm",      "--hotspot-ppm",   "--broadcast-ppm",
        "--zero-delay-ppm", "--queue-capacity", "--drain-limit",   "--work",
        "--unit-sigma",     "--cycle-sigma",    "--working-set",   "--working-set-sigma",
        "--hot-access-ppm"};
    for (const char* option : scalar_options) {
        require(rejects({"benchmark", option, too_large}), "uint32 scalar overflow was accepted");
    }
    require(rejects({"benchmark", "--threads", "1,4294967296"}),
            "worker-list overflow was accepted");
    require(rejects({"benchmark", "--payload-weights", "1,2,3,4,5,4294967296"}),
            "payload-list overflow was accepted");
    require(rejects({"benchmark", "--memory-pattern-weights", "1,2,3,4294967296"}),
            "memory-pattern-list overflow was accepted");
    require(rejects({"benchmark", "--memory-pattern-weights", "1,2,3"}),
            "short memory-pattern list was accepted");

    require(chronon::benchmark::parseU32("4294967295", "--test") ==
                std::numeric_limits<uint32_t>::max(),
            "UINT32_MAX was rejected by generic uint32 parsing");

    require(rejects({"benchmark", "--fixed-delay", "4294967295"}),
            "oversized fixed delay was accepted");
    const auto largest_delay = parseArgs(
        {"benchmark", "--fixed-delay", std::to_string(MAX_FORCED_DELAY), "--describe-only"});
    require(largest_delay.overrides.forced_delay == MAX_FORCED_DELAY,
            "largest benchmark fixed delay was rejected");
    require(rejects({"benchmark", "--fixed-delay", std::to_string(MAX_FORCED_DELAY + 1)}),
            "fixed delay above the transport headroom budget was accepted");
}

void testQueueCapacityBounds() {
    const auto maximum =
        parseArgs({"benchmark", "--queue-capacity", std::to_string(MAX_FINITE_QUEUE_CAPACITY)});
    require(maximum.overrides.queue_capacity == MAX_FINITE_QUEUE_CAPACITY,
            "maximum finite queue capacity was rejected");
    require(
        rejects({"benchmark", "--queue-capacity", std::to_string(MAX_FINITE_QUEUE_CAPACITY + 1)}),
        "oversized finite queue capacity was accepted");

    const auto unlimited = parseArgs({"benchmark", "--queue-capacity", "0"});
    require(
        unlimited.overrides.queue_capacity.has_value() && *unlimited.overrides.queue_capacity == 0,
        "unlimited queue capacity was not preserved");

    ScenarioConfig config;
    config.queue_capacity = MAX_FINITE_QUEUE_CAPACITY + 1;
    try {
        (void)generateScenario(config);
        require(false, "programmatic oversized queue capacity was accepted");
    } catch (const std::invalid_argument&) {
    }
}

void testPortStorageBudget() {
    ScenarioConfig disconnected;
    disconnected.num_units = MAX_SCENARIO_UNITS;
    disconnected.channels_per_unit = 0;
    disconnected.ensure_ring = false;
    disconnected.queue_capacity = MAX_FINITE_QUEUE_CAPACITY;
    disconnected.working_set_bytes = 64;
    disconnected.work_period = 1;
    disconnected.send_period = 1;
    const auto accepted = generateScenario(disconnected);
    require(accepted.summary.estimated_input_scratch_reserve_bytes == 0,
            "disconnected units reserved finite input scratch");
    require(accepted.summary.estimated_transport_reserve_bytes == 0,
            "disconnected units reserved transport storage");
    for (const auto& unit : accepted.units) {
        require(unit.incoming_payload_mask == 0,
                "disconnected unit advertised an incoming payload type");
    }

    ScenarioConfig oversized;
    oversized.num_units = 16;
    oversized.channels_per_unit = 64;
    oversized.max_fanout = 1;
    oversized.broadcast_probability_ppm = 0;
    oversized.payload_weights = {1, 0, 0, 0, 0, 0};
    oversized.queue_capacity = MAX_FINITE_QUEUE_CAPACITY;
    oversized.working_set_bytes = 64;
    oversized.work_period = 1;
    oversized.send_period = 1;
    try {
        (void)generateScenario(oversized);
        require(false, "oversized aggregate port storage was accepted");
    } catch (const std::invalid_argument&) {
    }
}

void testWorkerBounds() {
    const auto maximum =
        parseArgs({"benchmark", "--threads", std::to_string(MAX_BENCHMARK_WORKERS)});
    require(maximum.cli.workers == std::vector<size_t>{MAX_BENCHMARK_WORKERS},
            "maximum benchmark worker count was rejected");
    require(rejects({"benchmark", "--threads", std::to_string(MAX_BENCHMARK_WORKERS + 1)}),
            "oversized benchmark worker count was accepted");
    require(rejects({"benchmark", "--threads", "4294967295"}),
            "UINT32_MAX worker count was accepted");
}

void testExecutionBounds() {
    const auto defaults = parseArgs({"benchmark"});
    require(defaults.cli.run.target_seconds == std::optional<double>{1.0},
            "default benchmark duration is not one second");

    const auto maximum_scenarios =
        parseArgs({"benchmark", "--scenario-count", std::to_string(MAX_BENCHMARK_SCENARIOS)});
    require(maximum_scenarios.cli.scenario_count == MAX_BENCHMARK_SCENARIOS,
            "maximum scenario count was rejected");
    require(rejects({"benchmark", "--scenario-count", std::to_string(MAX_BENCHMARK_SCENARIOS + 1)}),
            "oversized scenario count was accepted");
    require(rejects({"benchmark", "--scenario-count", "4294967295"}),
            "UINT32_MAX scenario count was accepted");

    const auto maximum_repetitions =
        parseArgs({"benchmark", "--repetitions", std::to_string(MAX_BENCHMARK_REPETITIONS)});
    require(maximum_repetitions.cli.repetitions == MAX_BENCHMARK_REPETITIONS,
            "maximum repetition count was rejected");
    require(rejects({"benchmark", "--repetitions", std::to_string(MAX_BENCHMARK_REPETITIONS + 1)}),
            "oversized repetition count was accepted");

    const auto maximum_cycles =
        parseArgs({"benchmark", "--cycles", std::to_string(MAX_BENCHMARK_CYCLES), "--warmup",
                   std::to_string(MAX_BENCHMARK_CYCLES)});
    require(maximum_cycles.cli.run.measured_cycles == MAX_BENCHMARK_CYCLES &&
                maximum_cycles.cli.run.warmup_cycles == MAX_BENCHMARK_CYCLES &&
                !maximum_cycles.cli.run.target_seconds,
            "maximum cycle count was rejected");
    require(rejects({"benchmark", "--cycles", std::to_string(MAX_BENCHMARK_CYCLES + 1)}),
            "oversized measured cycle count was accepted");
    require(rejects({"benchmark", "--warmup", std::to_string(MAX_BENCHMARK_CYCLES + 1)}),
            "oversized warmup cycle count was accepted");

    const auto maximum_target =
        parseArgs({"benchmark", "--target-seconds", std::to_string(MAX_BENCHMARK_TARGET_SECONDS)});
    require(maximum_target.cli.run.target_seconds ==
                std::optional<double>{MAX_BENCHMARK_TARGET_SECONDS},
            "maximum target duration was rejected");
    require(rejects({"benchmark", "--target-seconds",
                     std::to_string(MAX_BENCHMARK_TARGET_SECONDS + 1.0)}),
            "oversized target duration was accepted");
    for (const char* invalid : {"0", "-1", "nan", "inf"}) {
        require(rejects({"benchmark", "--target-seconds", invalid}),
                "invalid target duration was accepted");
    }
    const auto quick = parseArgs({"benchmark", "--quick"});
    require(!quick.cli.run.target_seconds && quick.cli.run.measured_cycles == 2'000 &&
                quick.cli.repetitions == 1,
            "quick mode did not select its fixed-cycle smoke configuration");

    const auto maximum_lookahead =
        parseArgs({"benchmark", "--max-lookahead", std::to_string(MAX_BENCHMARK_LOOKAHEAD)});
    require(maximum_lookahead.cli.run.max_lookahead == MAX_BENCHMARK_LOOKAHEAD,
            "maximum benchmark lookahead was rejected");
    require(rejects({"benchmark", "--max-lookahead", std::to_string(MAX_BENCHMARK_LOOKAHEAD + 1)}),
            "lookahead above the transport headroom budget was accepted");
    require(rejects({"benchmark", "--max-lookahead", "4294967295"}),
            "UINT32_MAX lookahead was accepted");

    require(rejects({"benchmark", "--scenario-count", "0"}), "zero scenario count was accepted");
    require(rejects({"benchmark", "--repetitions", "0"}), "zero repetition count was accepted");
    require(rejects({"benchmark", "--cycles", "0"}), "zero measured cycle count was accepted");

    const std::string max_u64 = std::to_string(std::numeric_limits<uint64_t>::max());
    const auto maximum_offset =
        parseArgs({"benchmark", "--scenario-offset", max_u64, "--scenario-count", "1"});
    require(maximum_offset.cli.scenario_offset == std::numeric_limits<uint64_t>::max(),
            "maximum single scenario offset was rejected");
    require(rejects({"benchmark", "--scenario-offset", max_u64, "--scenario-count", "2"}),
            "overflowing scenario index range was accepted");
}

void testCalibrationUsesMeasuredProbe() {
    RunOptions options;
    options.target_seconds = 0.1;
    uint64_t last_measured_cycles = 0;
    uint32_t probe_count = 0;
    constexpr double PROBE_SECONDS = 0.009;
    const auto calibration = calibrateMeasuredCycles(
        std::vector<size_t>{1}, options, false,
        [&](size_t workers, const RunOptions& probe_options) {
            require(workers == 1, "calibration changed the requested worker count");
            last_measured_cycles = probe_options.measured_cycles;
            ++probe_count;
            return PROBE_SECONDS;
        });
    const double expected_rate = static_cast<double>(last_measured_cycles) / PROBE_SECONDS;
    require(probe_count == 3, "calibration did not exhaust the short-probe attempts");
    require(std::abs(calibration.fastest_cycles_per_second - expected_rate) < 1e-9,
            "calibration used an unmeasured cycle count for its rate");
}

void testWorkAndDrainBounds() {
    const auto maximum = parseArgs({"benchmark", "--work", std::to_string(MAX_MEDIAN_WORK),
                                    "--drain-limit", std::to_string(MAX_DRAIN_LIMIT)});
    require(maximum.overrides.median_work == MAX_MEDIAN_WORK, "maximum median work was rejected");
    require(maximum.overrides.drain_limit == MAX_DRAIN_LIMIT, "maximum drain limit was rejected");
    require(rejects({"benchmark", "--work", std::to_string(MAX_MEDIAN_WORK + 1)}),
            "oversized median work was accepted");
    require(rejects({"benchmark", "--drain-limit", std::to_string(MAX_DRAIN_LIMIT + 1)}),
            "oversized drain limit was accepted");
    require(rejects({"benchmark", "--work", "0"}), "zero median work was accepted");
    require(rejects({"benchmark", "--drain-limit", "0"}), "zero drain limit was accepted");

    ScenarioConfig config;
    config.num_units = 1;
    config.channels_per_unit = 0;
    config.ensure_ring = false;
    config.work_period = 1;
    config.send_period = 1;
    config.median_work = MAX_MEDIAN_WORK;
    config.drain_limit = MAX_DRAIN_LIMIT;
    (void)generateScenario(config);

    config.median_work = MAX_MEDIAN_WORK + 1;
    try {
        (void)generateScenario(config);
        require(false, "programmatic oversized median work was accepted");
    } catch (const std::invalid_argument&) {
    }

    config.median_work = 1;
    config.drain_limit = MAX_DRAIN_LIMIT + 1;
    try {
        (void)generateScenario(config);
        require(false, "programmatic oversized drain limit was accepted");
    } catch (const std::invalid_argument&) {
    }

    config.num_units = 16;
    config.median_work = MAX_MEDIAN_WORK;
    config.drain_limit = MAX_DRAIN_LIMIT;
    config.unit_sigma_milli = 2'000;
    config.cycle_sigma_milli = 2'000;
    config.work_period = 64;
    const auto bounded = generateScenario(config);
    bool observed_work_cap = false;
    for (const auto& unit : bounded.units) {
        require(unit.base_work <= MAX_GENERATED_WORK,
                "unit variance exceeded generated base-work limit");
        require(unit.drain_limit <= MAX_DRAIN_LIMIT,
                "unit variance exceeded generated drain limit");
        for (uint32_t work : unit.work_schedule) {
            require(work <= MAX_GENERATED_WORK,
                    "cycle variance exceeded generated work-sample limit");
            observed_work_cap = observed_work_cap || work == MAX_GENERATED_WORK;
        }
    }
    require(observed_work_cap, "extreme variance did not exercise generated work cap");
}

void testWorkingSetBudget() {
    ScenarioConfig config;
    config.num_units = 2;
    config.channels_per_unit = 0;
    config.ensure_ring = false;
    config.working_set_bytes = static_cast<uint32_t>(MAX_TOTAL_WORKING_SET_BYTES / 2);
    config.working_set_sigma_milli = 0;
    config.work_period = 1;
    config.send_period = 1;
    const auto scenario = generateScenario(config);
    require(scenario.summary.unit_working_set_bytes == MAX_TOTAL_WORKING_SET_BYTES,
            "exact aggregate working-set budget was rejected or misreported");

    config.num_units = 3;
    try {
        (void)generateScenario(config);
        require(false, "oversized exact aggregate working set was accepted");
    } catch (const std::invalid_argument&) {
    }

    require(rejectsScenario({"benchmark", "--working-set", "268435456"}),
            "oversized aggregate working set was accepted");
    require(rejectsScenario({"benchmark", "--working-set", "0"}),
            "zero representative working set was accepted");
}

void testFixedDelayOverridesAllChannels() {
    ScenarioConfig config;
    config.num_units = 8;
    config.channels_per_unit = 2;
    config.forced_delay = 5;
    const auto delayed = generateScenario(config);
    for (const auto& channel : delayed.channels) {
        require(channel.delay == 5, "ring channel ignored positive fixed delay");
    }

    config.forced_delay = 0;
    config.active_source_count = config.num_units - 1;
    const auto zero_delay = generateScenario(config);
    for (const auto& channel : zero_delay.channels) {
        require(channel.delay == 0, "channel ignored zero fixed delay");
        for (uint32_t destination : channel.destinations) {
            require(channel.source < destination, "fixed zero-delay topology is cyclic");
        }
    }

    config.active_source_count = 0;
    try {
        (void)generateScenario(config);
        require(false, "cyclic all-source zero fixed delay was accepted");
    } catch (const std::invalid_argument&) {
    }

    config.forced_delay = MAX_FORCED_DELAY + 1;
    config.active_source_count = 1;
    try {
        (void)generateScenario(config);
        require(false, "programmatic fixed delay above transport budget was accepted");
    } catch (const std::invalid_argument&) {
    }
}

void testScenarioResourceBounds() {
    require(rejectsScenario(
                {"benchmark", "--units", "2", "--channels", "4294967295", "--describe-only"}),
            "oversized generated channel count was accepted");
    require(rejectsScenario({"benchmark", "--units", std::to_string(MAX_SCENARIO_UNITS + 1),
                             "--channels", "0", "--describe-only"}),
            "oversized unit count was accepted");

    ScenarioConfig schedule_heavy;
    schedule_heavy.num_units = 2;
    schedule_heavy.channels_per_unit = 1;
    schedule_heavy.send_period = std::numeric_limits<uint32_t>::max();
    try {
        (void)generateScenario(schedule_heavy);
        require(false, "oversized generated schedule was accepted");
    } catch (const std::invalid_argument&) {
    }
}

void testCompleteReplayCommand() {
    const auto options = parseArgs({"benchmark",
                                    "--profile",
                                    "port",
                                    "--seed",
                                    "123",
                                    "--scenario-offset",
                                    "7",
                                    "--scenario-count",
                                    "3",
                                    "--threads",
                                    "8,2,8",
                                    "--warmup",
                                    "32",
                                    "--cycles",
                                    "128",
                                    "--repetitions",
                                    "1",
                                    "--max-lookahead",
                                    "4",
                                    "--rebalance",
                                    "--no-precomputed-costs",
                                    "--describe-only",
                                    "--verbose",
                                    "--queue-capacity",
                                    "64",
                                    "--working-set",
                                    "4096",
                                    "--working-set-sigma",
                                    "333",
                                    "--hot-access-ppm",
                                    "800000",
                                    "--memory-pattern-weights",
                                    "1,2,3,4"});
    std::ostringstream replay;
    printReplayCommand(replay, "benchmark", options, 9);
    require(replay.str() ==
                "benchmark --profile port --seed 123 --scenario-offset 9 --scenario-count 1"
                " --threads 2,8 --warmup 32 --cycles 128 --repetitions 1 --max-lookahead 4"
                " --rebalance --no-precomputed-costs --describe-only --verbose"
                " --queue-capacity 64 --working-set 4096 --working-set-sigma 333"
                " --hot-access-ppm 800000 --memory-pattern-weights 1,2,3,4",
            "replay command omitted or reordered effective options");
}

void testUnsignedCliRejectsNegativeValues() {
    constexpr std::array unsigned_options = {"--seed", "--scenario-offset", "--cycles", "--warmup"};
    for (const char* option : unsigned_options) {
        require(rejects({"benchmark", option, "-1"}), "negative unsigned value was accepted");
        require(rejects({"benchmark", option, " -1"}),
                "whitespace-prefixed negative unsigned value was accepted");
    }
}

}  // namespace

int main() {
    try {
        testStableFingerprint();
        testTopologyAndLoadInvariants();
        testRandomProfileResamplingAndOverrides();
        testSchedulerFloorProfile();
        testMemoryFootprintModel();
        testUint32CliBounds();
        testQueueCapacityBounds();
        testPortStorageBudget();
        testWorkerBounds();
        testExecutionBounds();
        testCalibrationUsesMeasuredProbe();
        testWorkAndDrainBounds();
        testWorkingSetBudget();
        testFixedDelayOverridesAllChannels();
        testScenarioResourceBounds();
        testCompleteReplayCommand();
        testUnsignedCliRejectsNegativeValues();
        std::cout << "Representative workload generator tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
