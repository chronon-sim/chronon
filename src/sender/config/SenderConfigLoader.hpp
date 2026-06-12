// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <yaml-cpp/yaml.h>

#include <fstream>
#include <iostream>
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

        // Deprecated alias for observation.timeline.scheduler (kept for migration).
        if (sim["timeline_trace"]) {
            std::cerr << "[config] 'simulation.timeline_trace' is deprecated; move it to "
                         "'simulation.observation.timeline.scheduler'\n";
            parseTimelineTrace(sim["timeline_trace"], config.timeline_trace);
        }

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
        LOAD_IF_PRESENT(trace_node, trace_config, trace_arbitration);
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
     *   trace_events: true
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
        LOAD_IF_PRESENT(node, config, trace_events);
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
     *   trace:  { enabled: true, text: false }   # trace events go to timeline.pftrace
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
            parseChannelConfig(node["debug"], config.debug_channel, source);
        }
        if (node["info"]) {
            parseChannelConfig(node["info"], config.info_channel, source);
        }
        if (node["warn"]) {
            parseChannelConfig(node["warn"], config.warn_channel, source);
        }
        if (node["error"]) {
            parseChannelConfig(node["error"], config.error_channel, source);
        }

        // Trace has extra binary-specific settings (compression / schema / index).
        if (node["trace"]) {
            parseTraceChannelConfig(node["trace"], config.trace_channel, source);
        }

        if (node["temporal"]) {
            parseTemporalFilters(node["temporal"], config.temporal, source);
        }

        if (node["categories"]) {
            parseCategoryPatterns(node["categories"], config.categories, source);
        }
    }

    void parseChannelConfig(const YAML::Node& node, observe::ChannelConfig& config,
                            const std::string& source) {
#define LOAD_IF_PRESENT(n, cfg, field)                   \
    if (n[#field]) {                                     \
        cfg.field = n[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(node, config, enabled);
        LOAD_IF_PRESENT(node, config, file);

#undef LOAD_IF_PRESENT

        warnDeprecatedFormatKey(node, source);
        if (node["backpressure"]) {
            config.backpressure = parseBackpressurePolicy(node["backpressure"].as<std::string>());
        }
        // backpressure_max_spins is std::optional<uint32_t>; yaml-cpp cannot convert directly.
        if (node["backpressure_max_spins"]) {
            config.backpressure_max_spins = node["backpressure_max_spins"].as<uint32_t>();
        }
    }

    void parseTraceChannelConfig(const YAML::Node& node, observe::TraceChannelConfig& config,
                                 const std::string& source) {
#define LOAD_IF_PRESENT(n, cfg, field)                   \
    if (n[#field]) {                                     \
        cfg.field = n[#field].as<decltype(cfg.field)>(); \
    }

        LOAD_IF_PRESENT(node, config, enabled);
        LOAD_IF_PRESENT(node, config, text);
        LOAD_IF_PRESENT(node, config, file);

#undef LOAD_IF_PRESENT

        warnDeprecatedFormatKey(node, source);
        for (const char* key : {"compression", "embed_schema", "generate_index"}) {
            if (node[key]) {
                std::cerr << "[config] " << source << ": 'logging.trace." << key
                          << "' is deprecated (binary .ctrace output was replaced by "
                             "Perfetto timeline.pftrace) and is ignored\n";
            }
        }
        if (node["backpressure"]) {
            config.backpressure = parseBackpressurePolicy(node["backpressure"].as<std::string>());
        }
        // backpressure_max_spins is std::optional<uint32_t>; yaml-cpp cannot convert directly.
        if (node["backpressure_max_spins"]) {
            config.backpressure_max_spins = node["backpressure_max_spins"].as<uint32_t>();
        }
    }

    /// Channel output is text-only since the Perfetto migration; trace events go
    /// to timeline.pftrace. Old `format:` keys are accepted but ignored.
    static void warnDeprecatedFormatKey(const YAML::Node& node, const std::string& source) {
        if (node["format"]) {
            std::cerr << "[config] " << source
                      << ": per-channel 'format' is deprecated and ignored (logs are text; "
                         "trace events go to timeline.pftrace — see observation.timeline)\n";
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

            for (const char* key : {"format", "trace_format", "debug_format", "info_format",
                                    "warn_format", "error_format"}) {
                if (pattern_node[key]) {
                    std::cerr << "[config] " << source << ": category '" << key
                              << "' is deprecated and ignored (output formats are no longer "
                                 "configured per category)\n";
                }
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

        if (unit_node["params"]) {
            unit_config.params_yaml = unit_node["params"];
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

        // Defaults to 1; 0 = tight coupling / INLINE.
        if (conn_node["delay"]) {
            spec.delay = conn_node["delay"].as<uint32_t>();
        }

        config.connections.push_back(std::move(spec));
    }
};

}  // namespace chronon::sender::config
