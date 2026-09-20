// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../observe/ObservationYAMLConfig.hpp"
#include "../core/TickSimulationConfig.hpp"

namespace chronon::sender::config {

/** @brief Parsed YAML config for one unit instance — type, name, and raw params. */
struct UnitConfig {
    std::string instance_name;
    std::string type_name;       ///< Factory type name, e.g. "FetchUnit".
    YAML::Node params_yaml;      ///< Raw YAML node for parameter deserialization.
    uint32_t tick_interval = 1;  ///< Execute tick() only on global cycles divisible by this.
    bool has_tick_interval = false;

    bool isValid() const { return !instance_name.empty() && !type_name.empty(); }
};

/**
 * @brief Directional connection from an OutPort to an InPort.
 *
 * Paths use dot notation ("unit_name.port_name" or "parent.child.port_name").
 * delay=0 permits same-cycle delivery; queue selection depends on topology.
 */
struct PortConnectionSpec {
    std::string source_path;
    std::string dest_path;
    uint32_t delay = 1;
    std::optional<size_t> capacity;
    std::optional<size_t> rate;

    bool isValid() const { return !source_path.empty() && !dest_path.empty(); }
};

/**
 * @brief Top-level YAML simulation config: settings, observation, units, connections.
 *
 * Example:
 * @code{.yaml}
 * simulation:
 *   num_workers: 4
 *   execution_policy: auto
 *   polling_interval_cycles: 64
 *   unit:
 *     fetch:
 *       type: FetchUnit
 *       params: { max_instructions: 100 }
 *       port: { out_instr: { to: decode.in_instr, delay: 1 } }
 * @endcode
 */
struct SimulationYAMLConfig {
    /// Legacy YAML default; C++ defaults to hardware_concurrency(). Set explicitly for parity.
    uint32_t num_workers = 4;
    bool enable_parallel = TickSimulationConfig{}.enable_parallel;
    /// Compatibility switch. False forces Sequential; Barrier was removed.
    bool enable_lookahead = TickSimulationConfig{}.enable_lookahead;
    bool trace_execution =
        TickSimulationConfig{}.trace_execution;  ///< Print execution policy details.
    uint32_t max_lookahead_cycles = TickSimulationConfig{}.max_lookahead_cycles;
    /// Host predicate and Sequential termination polling interval.
    uint64_t epoch_size = TickSimulationConfig{}.epoch_size;
    /// Compatibility switch for epoch-free execution. False forces Sequential;
    /// no epoch-boundary fallback remains.
    bool enable_epoch_free_lookahead = TickSimulationConfig{}.enable_epoch_free_lookahead;
    uint64_t run_cycles = 0;  ///< 0 uses the application default (10 million if unset).
    std::string name = "simulation";
    uint64_t tick_frequency_hz = TickSimulationConfig{}.tick_frequency_hz;  ///< Default 1 GHz.

    /// Enables cluster-aware partitioning. False keeps the legacy topology-only path.
    bool enable_weighted_partitioning = TickSimulationConfig{}.enable_weighted_partitioning;
    bool enable_dynamic_rebalance = TickSimulationConfig{}.enable_dynamic_rebalance;
    double rebalance_imbalance_threshold = TickSimulationConfig{}.rebalance_imbalance_threshold;
    uint64_t rebalance_check_interval_cycles =
        TickSimulationConfig{}.rebalance_check_interval_cycles;
    double rebalance_min_gain = TickSimulationConfig{}.rebalance_min_gain;
    uint64_t rebalance_cooldown_cycles = TickSimulationConfig{}.rebalance_cooldown_cycles;
    /// Initial cluster-aware partition solver: "SA" (default) or "Weighted".
    std::string partition_solver = "SA";
    double sa_critical_path_weight =
        TickSimulationConfig{}.sa_critical_path_weight;  ///< 0 disables the SA critical-path term.
    double initial_partition_sync_cost_ns =
        TickSimulationConfig{}
            .initial_partition_sync_cost_ns;  ///< Locality weight for the initial partition; 0 =
                                              ///< pure load balance.
    SchedulerTimelineTraceConfig timeline_trace;

    /// Builder auto-creates observation contexts when present and enabled.
    std::optional<observe::ObservationYAMLConfig> observation;

    /// Keyed by instance name (path) used for port resolution.
    std::unordered_map<std::string, UnitConfig> units;
    /// Unit instance names in YAML declaration order.
    std::vector<std::string> unit_order;

    std::vector<PortConnectionSpec> connections;

    /// One translation boundary for YAML compatibility names and runtime settings.
    TickSimulationConfig toRuntimeConfig() const {
        TickSimulationConfig result;
        result.num_threads = num_workers;
        result.enable_lookahead = enable_lookahead;
        result.trace_execution = trace_execution;
        result.max_lookahead_cycles = max_lookahead_cycles;
        result.epoch_size = epoch_size;
        result.enable_epoch_free_lookahead = enable_epoch_free_lookahead;
        result.tick_frequency_hz = tick_frequency_hz;
        result.enable_weighted_partitioning = enable_weighted_partitioning;
        result.enable_dynamic_rebalance = enable_dynamic_rebalance;
        result.rebalance_imbalance_threshold = rebalance_imbalance_threshold;
        result.rebalance_check_interval_cycles = rebalance_check_interval_cycles;
        result.rebalance_min_gain = rebalance_min_gain;
        result.rebalance_cooldown_cycles = rebalance_cooldown_cycles;
        result.sa_critical_path_weight = sa_critical_path_weight;
        result.initial_partition_sync_cost_ns = initial_partition_sync_cost_ns;
        result.enable_parallel = enable_parallel && num_workers > 1;
        result.timeline_trace = timeline_trace;
        std::string solver = partition_solver;
        std::transform(solver.begin(), solver.end(), solver.begin(),
                       [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
        if (solver == "sa")
            result.partition_solver = TickSimulationConfig::PartitionSolverType::SA;
        else if (solver == "weighted")
            result.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
        else
            throw std::invalid_argument("unknown partition_solver '" + partition_solver +
                                        "' (expected 'Weighted' or 'SA')");
        return result;
    }

    size_t unitCount() const { return units.size(); }
    size_t connectionCount() const { return connections.size(); }
    bool hasUnit(const std::string& name) const { return units.count(name) > 0; }

    /// Returns nullptr if not found.
    const UnitConfig* getUnit(const std::string& name) const {
        auto it = units.find(name);
        return it != units.end() ? &it->second : nullptr;
    }

    std::vector<std::string> unitNames() const {
        std::vector<std::string> names;
        names.reserve(units.size());

        for (const auto& name : unit_order) {
            if (hasUnit(name)) {
                names.push_back(name);
            }
        }

        std::vector<std::string> remaining;
        for (const auto& [name, _] : units) {
            if (std::find(names.begin(), names.end(), name) == names.end()) {
                remaining.push_back(name);
            }
        }
        std::sort(remaining.begin(), remaining.end());
        names.insert(names.end(), remaining.begin(), remaining.end());
        return names;
    }
};

}  // namespace chronon::sender::config
