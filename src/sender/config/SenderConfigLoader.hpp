// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <yaml-cpp/yaml.h>

#include <charconv>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>

#include "SenderUnitConfig.hpp"

namespace chronon::sender::config {

/** @brief Thrown when YAML configuration loading or parsing fails. */
class ConfigLoadError : public std::runtime_error {
public:
    explicit ConfigLoadError(const std::string& message) : std::runtime_error(message) {}

    ConfigLoadError(const std::string& context, const std::string& detail)
        : std::runtime_error(context + ": " + detail) {}
};

/**
 * @brief Parses YAML files/strings into SimulationYAMLConfig.
 *
 * Supports the unified logging format: each event type (debug/info/warn/error/trace) is an
 * independent channel under one `logging:` key with shared categories and temporal filters.
 */
class SenderConfigLoader {
public:
    SimulationYAMLConfig loadFromFile(const std::string& filepath) {
        std::ifstream file(filepath);
        if (!file.is_open()) {
            throw ConfigLoadError("Failed to open config file", filepath);
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        return loadFromString(buffer.str(), filepath);
    }

    SimulationYAMLConfig loadFromString(const std::string& yaml_content,
                                        const std::string& source_name = "<string>") {
        try {
            YAML::Node root = YAML::Load(yaml_content);
            return parseRoot(root, source_name);
        } catch (const YAML::Exception& e) {
            throw ConfigLoadError("YAML parse error in " + source_name, e.what());
        }
    }

    /// Useful for applying runtime overrides before parsing.
    SimulationYAMLConfig loadFromNode(const YAML::Node& root,
                                      const std::string& source_name = "<node>") {
        try {
            return parseRoot(root, source_name);
        } catch (const YAML::Exception& e) {
            throw ConfigLoadError("YAML parse error in " + source_name, e.what());
        }
    }

private:
    static void checkKeys(const YAML::Node& node, std::initializer_list<const char*> allowed,
                          const std::string& path) {
        if (!node.IsMap()) throw ConfigLoadError(path, "must be a map");
        std::set<std::string> seen;
        for (const auto& pair : node) {
            const auto key = pair.first.as<std::string>();
            if (!seen.insert(key).second)
                throw ConfigLoadError(path, "duplicate key '" + key + "'");
            if (std::none_of(allowed.begin(), allowed.end(),
                             [&](const char* a) { return key == a; }))
                throw ConfigLoadError(path, "unknown field '" + key + "'");
        }
    }

    static uint64_t exactUnsigned(const std::string& value, const std::string& path) {
        uint64_t result = 0;
        const auto [end, error] =
            std::from_chars(value.data(), value.data() + value.size(), result);
        if (value.empty() || error != std::errc{} || end != value.data() + value.size())
            throw ConfigLoadError(path, "expected an unsigned decimal integer within uint64 range");
        return result;
    }

    static SimTime exactRational(const YAML::Node& node, const std::string& path) {
        if (!node.IsScalar())
            throw ConfigLoadError(path, "expected an integer or 'numerator/denominator'");
        const auto value = node.Scalar();
        const auto slash = value.find('/');
        const auto numerator = exactUnsigned(value.substr(0, slash), path);
        const auto denominator =
            slash == std::string::npos ? 1 : exactUnsigned(value.substr(slash + 1), path);
        if (!denominator) throw ConfigLoadError(path, "denominator must be positive");
        return {numerator, denominator};
    }

    void parseClocksAndRun(const YAML::Node& sim, SimulationYAMLConfig& config,
                           const std::string& source) {
        if (const auto clocks = sim["clocks"]) {
            if (!clocks.IsMap() || clocks.size() == 0)
                throw ConfigLoadError(source, "simulation.clocks must be a nonempty map");
            std::set<std::string> names{"default"};
            for (const auto& pair : clocks) {
                const auto name = pair.first.as<std::string>();
                const auto path = source + ": simulation.clocks." + name;
                if (!names.insert(name).second)
                    throw ConfigLoadError(
                        path, "duplicate or reserved clock name ('default' is built in)");
                const auto& node = pair.second;
                checkKeys(node, {"frequency_hz", "period_s", "phase_s"}, path);
                if (bool(node["frequency_hz"]) == bool(node["period_s"]))
                    throw ConfigLoadError(path, "specify exactly one of frequency_hz or period_s");
                auto period =
                    exactRational(node[node["frequency_hz"] ? "frequency_hz" : "period_s"], path);
                if (!period.numerator())
                    throw ConfigLoadError(path, "frequency/period must be positive");
                if (node["frequency_hz"])
                    period = SimTime(period.denominator(), period.numerator());
                const auto phase =
                    node["phase_s"] ? exactRational(node["phase_s"], path + ".phase_s") : SimTime{};
                try {
                    if (config.clocks.size() >= UINT32_MAX - 1)
                        throw std::invalid_argument("too many clock domains");
                    config.clocks.emplace_back(static_cast<ClockDomainId>(config.clocks.size() + 1),
                                               name, period, phase);
                } catch (const std::exception& error) {
                    throw ConfigLoadError(path, error.what());
                }
            }
        }
        if (const auto run = sim["run"]) {
            const auto path = source + ": simulation.run";
            checkKeys(run, {"until_time_s", "event_batches", "domain_cycles"}, path);
            if (run.size() != 1) throw ConfigLoadError(path, "specify exactly one run limit");
            if (sim["run_cycles"])
                throw ConfigLoadError(
                    path,
                    "cannot combine with run_cycles/--run-cycles; use an explicit clock run limit");
            ClockRunLimit limit;
            if (run["until_time_s"]) {
                limit.kind = ClockRunLimit::Kind::UntilTime;
                limit.until_time = exactRational(run["until_time_s"], path + ".until_time_s");
            } else if (run["event_batches"]) {
                limit.kind = ClockRunLimit::Kind::EventBatches;
                limit.count =
                    exactUnsigned(run["event_batches"].as<std::string>(), path + ".event_batches");
            } else {
                const auto domain = run["domain_cycles"];
                checkKeys(domain, {"clock", "count"}, path + ".domain_cycles");
                if (!domain["clock"] || !domain["count"])
                    throw ConfigLoadError(path, "domain_cycles requires clock and count");
                limit.kind = ClockRunLimit::Kind::DomainCycles;
                limit.clock = domain["clock"].as<std::string>();
                limit.count =
                    exactUnsigned(domain["count"].as<std::string>(), path + ".domain_cycles.count");
                try {
                    (void)config.clockId(limit.clock);
                } catch (const std::exception& error) {
                    throw ConfigLoadError(path, error.what());
                }
            }
            config.run = limit;
        }
        if (!config.clocks.empty() && !config.run)
            throw ConfigLoadError(source,
                                  "simulation.clocks requires an explicit simulation.run limit "
                                  "(until_time_s, event_batches or domain_cycles)");
        if (config.clocks.empty() && config.run)
            throw ConfigLoadError(source,
                                  "simulation.run requires simulation.clocks; use run_cycles for "
                                  "legacy single-clock models");
    }

    static std::optional<size_t> parseDestinationDepth(const YAML::Node& node,
                                                       const std::string& source) {
        std::optional<size_t> depth;
        if (node["capacity"]) depth = node["capacity"].as<size_t>();
        if (node["destination_depth"]) {
            const auto canonical = node["destination_depth"].as<size_t>();
            if (depth && *depth != canonical)
                throw ConfigLoadError(source, "destination_depth conflicts with capacity");
            depth = canonical;
        }
        return depth;
    }

    SimulationYAMLConfig parseRoot(const YAML::Node& root, const std::string& source) {
        SimulationYAMLConfig config;

        if (!root["simulation"]) {
            throw ConfigLoadError(source, "Missing 'simulation' key at root");
        }

        const YAML::Node& sim = root["simulation"];

// Loads a YAML field into a config struct member when the YAML key matches the field name.
#define LOAD_IF_PRESENT(node, cfg, field)                   \
    if (node[#field]) {                                     \
        cfg.field = node[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(sim, config, name);
        LOAD_IF_PRESENT(sim, config, num_workers);
        LOAD_IF_PRESENT(sim, config, enable_parallel);
        LOAD_IF_PRESENT(sim, config, enable_lookahead);
        LOAD_IF_PRESENT(sim, config, trace_execution);
        LOAD_IF_PRESENT(sim, config, max_lookahead_cycles);
        LOAD_IF_PRESENT(sim, config, epoch_size);
        LOAD_IF_PRESENT(sim, config, enable_epoch_free_lookahead);
        LOAD_IF_PRESENT(sim, config, run_cycles);
        LOAD_IF_PRESENT(sim, config, tick_frequency_hz);
        LOAD_IF_PRESENT(sim, config, enable_weighted_partitioning);
        LOAD_IF_PRESENT(sim, config, enable_dynamic_rebalance);
        LOAD_IF_PRESENT(sim, config, rebalance_imbalance_threshold);
        LOAD_IF_PRESENT(sim, config, rebalance_check_interval_cycles);
        LOAD_IF_PRESENT(sim, config, rebalance_min_gain);
        LOAD_IF_PRESENT(sim, config, rebalance_cooldown_cycles);
        LOAD_IF_PRESENT(sim, config, partition_solver);
        LOAD_IF_PRESENT(sim, config, sa_critical_path_weight);
        LOAD_IF_PRESENT(sim, config, initial_partition_sync_cost_ns);

        // Decode canonical names once; conflicting compatibility spellings
        // are errors, never an order-dependent override.
        if (sim["polling_interval_cycles"]) {
            const auto interval = sim["polling_interval_cycles"].as<uint64_t>();
            if (sim["epoch_size"] && interval != config.epoch_size)
                throw ConfigLoadError(source, "polling_interval_cycles conflicts with epoch_size");
            config.epoch_size = interval;
        }
        if (sim["execution_policy"]) {
            const auto policy = sim["execution_policy"].as<std::string>();
            if (policy != "auto" && policy != "sequential")
                throw ConfigLoadError(source, "execution_policy must be auto or sequential");
            for (const char* key :
                 {"enable_parallel", "enable_lookahead", "enable_epoch_free_lookahead"}) {
                if (sim[key])
                    throw ConfigLoadError(
                        source, "execution_policy cannot be mixed with legacy execution switches");
            }
            config.enable_parallel = policy == "auto";
            config.enable_lookahead = config.enable_epoch_free_lookahead = true;
        }

        parseClocksAndRun(sim, config, source);

        if (sim["observation"]) {
            parseObservation(sim["observation"], config, source);
        }

        if (sim["bus"]) {
            parseBuses(sim["bus"], config, source);
        }

        if (sim["unit"]) {
            parseUnits(sim["unit"], config, source);
        }

#undef LOAD_IF_PRESENT
        return config;
    }

    void parseTimelineTrace(const YAML::Node& trace_node,
                            SchedulerTimelineTraceConfig& trace_config) {
#define LOAD_IF_PRESENT(node, cfg, field)                   \
    if (node[#field]) {                                     \
        cfg.field = node[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(trace_node, trace_config, enabled);
        LOAD_IF_PRESENT(trace_node, trace_config, file);
        LOAD_IF_PRESENT(trace_node, trace_config, max_events);
        LOAD_IF_PRESENT(trace_node, trace_config, start_cycle);
        LOAD_IF_PRESENT(trace_node, trace_config, end_cycle);
        LOAD_IF_PRESENT(trace_node, trace_config, trace_units);
        LOAD_IF_PRESENT(trace_node, trace_config, trace_waits);
        LOAD_IF_PRESENT(trace_node, trace_config, trace_epochs);
        LOAD_IF_PRESENT(trace_node, trace_config, trace_thread_cpu_time);
        LOAD_IF_PRESENT(trace_node, trace_config, min_duration_ns);

#undef LOAD_IF_PRESENT
    }

    void parseObservation(const YAML::Node& obs_node, SimulationYAMLConfig& config,
                          const std::string& source) {
        observe::ObservationYAMLConfig obs_config;

#define LOAD_IF_PRESENT(node, cfg, field)                   \
    if (node[#field]) {                                     \
        cfg.field = node[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(obs_node, obs_config, enabled);
        LOAD_IF_PRESENT(obs_node, obs_config, output_dir);
        LOAD_IF_PRESENT(obs_node, obs_config, queue_capacity);
        if (obs_node["scheduler_service"] && !obs_node["scheduler_service"].as<bool>())
            throw std::invalid_argument(
                "observation.scheduler_service=false was removed; remove this setting");
        LOAD_IF_PRESENT(obs_node, obs_config, service_buffer_bytes);
        LOAD_IF_PRESENT(obs_node, obs_config, backpressure_max_spins);

#undef LOAD_IF_PRESENT

        // backpressure needs string-to-enum conversion, handled separately.
        if (obs_node["backpressure"]) {
            obs_config.backpressure =
                parseBackpressurePolicy(obs_node["backpressure"].as<std::string>());
        }

        if (obs_node["counters"]) {
            parseCountersConfig(obs_node["counters"], obs_config.counters, source);
        }

        if (obs_node["logging"]) {
            parseUnifiedLogging(obs_node["logging"], obs_config.unified_logging, source);
        }

        if (obs_node["timeline"]) {
            parseTimeline(obs_node["timeline"], obs_config.timeline, config);
        }

        if (obs_node["unit_overrides"]) {
            parseUnitOverrides(obs_node["unit_overrides"], obs_config.unit_overrides, source);
        }

        config.observation = std::move(obs_config);
    }

    /**
     * @brief Parse the unified Perfetto timeline section.
     *
     * @code{.yaml}
     * timeline:
     *   enabled: true
     *   file: timeline.pftrace
     *   counters: true
     *   scheduler: { enabled: true, trace_units: true, ... }   # execution timeline
     * @endcode
     *
     * The scheduler sub-section configures the wall-clock scheduler execution
     * timeline; it lands in SimulationYAMLConfig::timeline_trace because the
     * recorder lives in the scheduler, but its output merges into the same
     * timeline.pftrace.
     */
    void parseTimeline(const YAML::Node& node, observe::TimelineYAMLConfig& config,
                       SimulationYAMLConfig& sim_config) {
#define LOAD_IF_PRESENT(n, cfg, field)                   \
    if (n[#field]) {                                     \
        cfg.field = n[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(node, config, enabled);
        LOAD_IF_PRESENT(node, config, file);
        LOAD_IF_PRESENT(node, config, counters);
        LOAD_IF_PRESENT(node, config, compress);

#undef LOAD_IF_PRESENT

        if (node["scheduler"]) {
            parseTimelineTrace(node["scheduler"], sim_config.timeline_trace);
        }
    }

    void parseCountersConfig(const YAML::Node& node, observe::CountersYAMLConfig& config,
                             const std::string& /*source*/) {
#define LOAD_IF_PRESENT(n, cfg, field)                   \
    if (n[#field]) {                                     \
        cfg.field = n[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(node, config, enabled);
        LOAD_IF_PRESENT(node, config, csv_output);
        LOAD_IF_PRESENT(node, config, periodic_dump_cycles);
        LOAD_IF_PRESENT(node, config, reference_clock);
        LOAD_IF_PRESENT(node, config, dump_on_shutdown);

#undef LOAD_IF_PRESENT

        // csv_format needs string-to-enum conversion, handled separately.
        if (node["csv_format"]) {
            auto fmt_str = node["csv_format"].as<std::string>();
            if (fmt_str == "long") {
                config.csv_format = observe::CounterCsvFormat::Long;
            } else if (fmt_str == "pivoted" || fmt_str == "wide") {
                config.csv_format = observe::CounterCsvFormat::Pivoted;
            }
        }
    }

    /**
     * @brief Parse the unified logging section.
     *
     * YAML shape:
     * @code{.yaml}
     * logging:
     *   enabled: true
     *   debug:  { enabled: true }
     *   trace:  { enabled: true }   # gates events written to timeline.pftrace
     *   info:   { enabled: true }
     *   warn:   { enabled: true }
     *   error:  { enabled: true }
     *   temporal:   [ { range: [0, 100000] } ]
     *   categories: [ { pattern: "verif" } ]
     * @endcode
     */
    void parseUnifiedLogging(const YAML::Node& node, observe::UnifiedLoggingConfig& config,
                             const std::string& source) {
        if (node["enabled"]) {
            config.enabled = node["enabled"].as<bool>();
        }

        if (node["debug"]) {
            parseChannelConfig(node["debug"], config.debug_channel);
        }
        if (node["info"]) {
            parseChannelConfig(node["info"], config.info_channel);
        }
        if (node["warn"]) {
            parseChannelConfig(node["warn"], config.warn_channel);
        }
        if (node["error"]) {
            parseChannelConfig(node["error"], config.error_channel);
        }

        if (node["trace"]) {
            parseTraceChannelConfig(node["trace"], config.trace_channel);
        }

        if (node["temporal"]) {
            parseTemporalFilters(node["temporal"], config.temporal, source);
        }

        if (node["categories"]) {
            parseCategoryPatterns(node["categories"], config.categories, source);
        }
    }

    void parseChannelConfig(const YAML::Node& node, observe::ChannelConfig& config) {
#define LOAD_IF_PRESENT(n, cfg, field)                   \
    if (n[#field]) {                                     \
        cfg.field = n[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(node, config, enabled);
        LOAD_IF_PRESENT(node, config, file);

#undef LOAD_IF_PRESENT

        if (node["backpressure"]) {
            config.backpressure = parseBackpressurePolicy(node["backpressure"].as<std::string>());
        }
        // backpressure_max_spins is std::optional<uint32_t>; yaml-cpp cannot convert directly.
        if (node["backpressure_max_spins"]) {
            config.backpressure_max_spins = node["backpressure_max_spins"].as<uint32_t>();
        }
    }

    void parseTraceChannelConfig(const YAML::Node& node, observe::TraceChannelConfig& config) {
#define LOAD_IF_PRESENT(n, cfg, field)                   \
    if (n[#field]) {                                     \
        cfg.field = n[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(node, config, enabled);
#undef LOAD_IF_PRESENT

        if (node["backpressure"]) {
            config.backpressure = parseBackpressurePolicy(node["backpressure"].as<std::string>());
        }
        // backpressure_max_spins is std::optional<uint32_t>; yaml-cpp cannot convert directly.
        if (node["backpressure_max_spins"]) {
            config.backpressure_max_spins = node["backpressure_max_spins"].as<uint32_t>();
        }
    }

    static observe::BackpressurePolicy parseBackpressurePolicy(const std::string& bp) {
        if (bp == "drop") return observe::BackpressurePolicy::Drop;
        if (bp == "spin_wait") return observe::BackpressurePolicy::SpinWait;
        return observe::BackpressurePolicy::BoundedWait;
    }

    void parseTemporalFilters(const YAML::Node& node, std::vector<observe::TemporalFilter>& filters,
                              const std::string& source) {
        if (!node.IsSequence()) {
            throw ConfigLoadError(source, "'temporal' must be a sequence");
        }

        for (const auto& filter_node : node) {
            observe::TemporalFilter filter;

            if (filter_node["range"]) {
                const auto& range = filter_node["range"];
                if (!range.IsSequence() || range.size() != 2) {
                    throw ConfigLoadError(source, "temporal range must be [start, end]");
                }
                filter = observe::TemporalFilter::range(range[0].as<uint64_t>(),
                                                        range[1].as<uint64_t>());
            } else if (filter_node["periodic"]) {
                const auto& periodic = filter_node["periodic"];
                uint64_t window = periodic["window"].as<uint64_t>();
                uint64_t period = periodic["period"].as<uint64_t>();
                uint64_t offset = 0;
                if (periodic["offset"]) {
                    offset = periodic["offset"].as<uint64_t>();
                }
                filter = observe::TemporalFilter::periodic(window, period, offset);
            } else {
                throw ConfigLoadError(source,
                                      "temporal filter must have 'range' or 'periodic' key");
            }

            filters.push_back(filter);
        }
    }

    void parseCategoryPatterns(const YAML::Node& node,
                               std::vector<observe::CategoryPattern>& patterns,
                               const std::string& source) {
        if (!node.IsSequence()) {
            throw ConfigLoadError(source, "'categories' must be a sequence");
        }

        for (const auto& pattern_node : node) {
            observe::CategoryPattern pattern;

            if (!pattern_node["pattern"]) {
                throw ConfigLoadError(source, "category pattern missing required 'pattern' field");
            }
            pattern.pattern = pattern_node["pattern"].as<std::string>();

            if (pattern_node["enabled"]) {
                pattern.enabled = pattern_node["enabled"].as<bool>();
            }

            if (pattern_node["temporal"]) {
                parseTemporalFilters(pattern_node["temporal"], pattern.temporal, source);
            }

            patterns.push_back(std::move(pattern));
        }
    }

    void parseUnitOverrides(
        const YAML::Node& node,
        std::unordered_map<std::string, observe::UnitObservationOverride>& overrides,
        const std::string& source) {
        if (!node.IsMap()) {
            throw ConfigLoadError(source, "'unit_overrides' must be a map");
        }

        for (const auto& pair : node) {
            std::string unit_name = pair.first.as<std::string>();
            const auto& override_node = pair.second;

            observe::UnitObservationOverride unit_override;

            if (override_node["counters"]) {
                observe::CountersYAMLConfig counters;
                parseCountersConfig(override_node["counters"], counters, source);
                unit_override.counters = counters;
            }

            if (override_node["logging"]) {
                observe::UnifiedLoggingConfig logging;
                parseUnifiedLogging(override_node["logging"], logging, source);
                unit_override.logging = logging;
            }

            overrides[unit_name] = std::move(unit_override);
        }
    }

    /**
     * @brief Expand each bus definition into N×M direct port connections via OutPort fan-out.
     *
     * No bus unit is created — this is pure YAML syntactic sugar.
     *
     * @code{.yaml}
     * bus:
     *   wakeup:
     *     delay: 1
     *     inputs:  [exe0.out_wakeup, exe1.out_wakeup]
     *     outputs: [iq0.in_wakeup, iq1.in_wakeup, dispatch.in_wakeup]
     * @endcode
     */
    void parseBuses(const YAML::Node& bus_node, SimulationYAMLConfig& config,
                    const std::string& source) {
        if (!bus_node.IsMap()) {
            throw ConfigLoadError(source, "'bus' must be a map");
        }

        for (const auto& bus_pair : bus_node) {
            std::string bus_name = bus_pair.first.as<std::string>();
            const YAML::Node& bus_def = bus_pair.second;

            uint32_t delay = 1;
            if (bus_def["delay"]) {
                delay = bus_def["delay"].as<uint32_t>();
            }
            const auto capacity = parseDestinationDepth(bus_def, source);
            std::optional<size_t> rate;
            if (bus_def["rate"]) {
                rate = bus_def["rate"].as<size_t>();
            }

            if (!bus_def["inputs"] || !bus_def["inputs"].IsSequence()) {
                throw ConfigLoadError(source,
                                      "Bus '" + bus_name + "' missing required 'inputs' sequence");
            }
            if (!bus_def["outputs"] || !bus_def["outputs"].IsSequence()) {
                throw ConfigLoadError(source,
                                      "Bus '" + bus_name + "' missing required 'outputs' sequence");
            }

            for (const auto& input_node : bus_def["inputs"]) {
                for (const auto& output_node : bus_def["outputs"]) {
                    PortConnectionSpec spec;
                    spec.source_path = input_node.as<std::string>();
                    spec.dest_path = output_node.as<std::string>();
                    spec.delay = delay;
                    spec.capacity = capacity;
                    spec.rate = rate;
                    config.connections.push_back(std::move(spec));
                }
            }
        }
    }

    void parseUnits(const YAML::Node& units_node, SimulationYAMLConfig& config,
                    const std::string& source) {
        if (!units_node.IsMap()) {
            throw ConfigLoadError(source, "'unit' must be a map");
        }

        for (const auto& unit_pair : units_node) {
            std::string unit_name = unit_pair.first.as<std::string>();
            const YAML::Node& unit_node = unit_pair.second;

            parseUnit(unit_name, unit_node, config, source);
        }
    }

    void parseUnit(const std::string& unit_name, const YAML::Node& unit_node,
                   SimulationYAMLConfig& config, const std::string& source) {
        UnitConfig unit_config;
        unit_config.instance_name = unit_name;

        if (!unit_node["type"]) {
            throw ConfigLoadError(source, "Unit '" + unit_name + "' missing required 'type' field");
        }
        unit_config.type_name = unit_node["type"].as<std::string>();

        if (config.hasUnit(unit_name))
            throw ConfigLoadError(source, "duplicate unit '" + unit_name + "'");
        if (unit_node["clock"]) {
            if (config.clocks.empty())
                throw ConfigLoadError(source,
                                      "unit.clock requires simulation.clocks and simulation.run");
            unit_config.clock = unit_node["clock"].as<std::string>();
            try {
                (void)config.clockId(*unit_config.clock);
            } catch (const std::exception& error) {
                throw ConfigLoadError(source + ": unit '" + unit_name + "'", error.what());
            }
        }

        if (unit_node["params"]) {
            unit_config.params_yaml = unit_node["params"];
        }

        if (unit_node["tick_interval"]) {
            unit_config.tick_interval = unit_node["tick_interval"].as<uint32_t>();
            unit_config.has_tick_interval = true;
            if (unit_config.tick_interval == 0) {
                throw ConfigLoadError(source,
                                      "Unit '" + unit_name + "' tick_interval must be >= 1");
            }
        }

        if (!config.hasUnit(unit_name)) {
            config.unit_order.push_back(unit_name);
        }
        config.units[unit_name] = std::move(unit_config);

        if (unit_node["port"]) {
            parsePorts(unit_name, unit_node["port"], config, source);
        }
    }

    void parsePorts(const std::string& unit_name, const YAML::Node& ports_node,
                    SimulationYAMLConfig& config, const std::string& source) {
        if (!ports_node.IsMap()) {
            throw ConfigLoadError(source, "Unit '" + unit_name + "' port section must be a map");
        }

        for (const auto& port_pair : ports_node) {
            std::string port_name = port_pair.first.as<std::string>();
            const YAML::Node& port_node = port_pair.second;

            parsePortConnection(unit_name, port_name, port_node, config, source);
        }
    }

    /// Port node may be a single map (one connection) or a sequence (fan-out).
    void parsePortConnection(const std::string& unit_name, const std::string& port_name,
                             const YAML::Node& port_node, SimulationYAMLConfig& config,
                             const std::string& source) {
        std::string source_path = unit_name + "." + port_name;

        if (port_node.IsSequence()) {
            for (const auto& conn_node : port_node) {
                addConnection(source_path, conn_node, config, source);
            }
        } else if (port_node.IsMap()) {
            addConnection(source_path, port_node, config, source);
        } else {
            throw ConfigLoadError(source, "Port '" + source_path + "' must be a map or sequence");
        }
    }

    void addConnection(const std::string& source_path, const YAML::Node& conn_node,
                       SimulationYAMLConfig& config, const std::string& source) {
        PortConnectionSpec spec;
        spec.source_path = source_path;

        if (!conn_node["to"]) {
            throw ConfigLoadError(
                source, "Connection from '" + source_path + "' missing required 'to' field");
        }
        spec.dest_path = conn_node["to"].as<std::string>();

        if (const auto cdc = conn_node["cdc"]) {
            const auto path =
                source + ": connection '" + source_path + "' -> '" + spec.dest_path + "'";
            checkKeys(conn_node, {"to", "cdc", "delay", "capacity", "destination_depth", "rate"},
                      path);
            checkKeys(cdc, {"type", "depth", "synchronizer_stages"}, path + ".cdc");
            if (!cdc["type"] || cdc["type"].as<std::string>() != "async_fifo")
                throw ConfigLoadError(path, "cdc.type must be async_fifo");
            for (const char* key : {"delay", "capacity", "destination_depth", "rate"})
                if (conn_node[key])
                    throw ConfigLoadError(
                        path, "CDC cannot be combined with ordinary connection field '" +
                                  std::string(key) + "'");
            if (config.clocks.empty())
                throw ConfigLoadError(path,
                                      "CDC requires explicit simulation.clocks and simulation.run");
            AsyncFifoConfig fifo;
            const auto sizeValue = [&](const YAML::Node& value, const std::string& field) {
                const auto parsed = exactUnsigned(value.as<std::string>(), path + field);
                if (parsed > std::numeric_limits<size_t>::max())
                    throw ConfigLoadError(path + field, "value exceeds size_t range");
                return static_cast<size_t>(parsed);
            };
            if (cdc["depth"]) fifo.depth = sizeValue(cdc["depth"], ".cdc.depth");
            if (cdc["synchronizer_stages"])
                fifo.synchronizer_stages =
                    sizeValue(cdc["synchronizer_stages"], ".cdc.synchronizer_stages");
            if (fifo.depth < 2 || !std::has_single_bit(fifo.depth) ||
                fifo.depth > (size_t{1} << 30))
                throw ConfigLoadError(path, "CDC depth must be a power of two in [2, 2^30]");
            if (fifo.synchronizer_stages < 2 || fifo.synchronizer_stages > 64)
                throw ConfigLoadError(path, "CDC synchronizer_stages must be in [2, 64]");
            spec.cdc = fifo;
        }

        // Defaults to 1; 0 = tight coupling / INLINE.
        if (conn_node["delay"]) {
            spec.delay = conn_node["delay"].as<uint32_t>();
        }
        spec.capacity = parseDestinationDepth(conn_node, source);
        if (conn_node["rate"]) {
            spec.rate = conn_node["rate"].as<size_t>();
        }

        config.connections.push_back(std::move(spec));
    }
};

}  // namespace chronon::sender::config
