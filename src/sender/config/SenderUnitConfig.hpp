// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <yaml-cpp/yaml.h>

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../observe/ObservationYAMLConfig.hpp"
#include "../schedule/SchedulerTimelineTrace.hpp"

namespace chronon::sender::config {

/** @brief Parsed YAML config for one unit instance — type, name, and raw params. */
struct UnitConfig {
    std::string instance_name;
    std::string type_name;   ///< Factory type name, e.g. "FetchUnit".
    YAML::Node params_yaml;  ///< Raw YAML node for parameter deserialization.

    bool isValid() const { return !instance_name.empty() && !type_name.empty(); }
};

/**
 * @brief Directional connection from an OutPort to an InPort.
 *
 * Paths use dot notation ("unit_name.port_name" or "parent.child.port_name").
 * delay=0 means tight coupling / INLINE.
 */
struct PortConnectionSpec {
    std::string source_path;
    std::string dest_path;
    uint32_t delay = 1;

    bool isValid() const { return !source_path.empty() && !dest_path.empty(); }
};

/**
 * @brief Top-level YAML simulation config: settings, observation, units, connections.
 *
 * Example:
 * @code{.yaml}
 * simulation:
 *   num_workers: 4
 *   epoch_size: 64
 *   unit:
 *     fetch:
 *       type: FetchUnit
 *       params: { max_instructions: 100 }
 *       port: { out_instr: { to: decode.in_instr, delay: 1 } }
 * @endcode
 */
struct SimulationYAMLConfig {
    uint32_t num_workers = 4;
    bool enable_parallel = true;
    bool enable_lookahead = true;
    bool trace_execution = false;  ///< Print execution policy details.
    uint32_t max_lookahead_cycles = 100;
    uint64_t epoch_size = 64;  ///< Synchronization period in cycles.
    uint64_t run_cycles = 0;   ///< 0 = run until completion.
    std::string name = "simulation";
    uint64_t tick_frequency_hz = 1'000'000'000;  ///< Default 1 GHz.

    bool enable_weighted_partitioning = true;
    uint64_t profiling_warmup_cycles = 512;
    uint64_t profiling_measurement_cycles = 1024;
    bool deterministic_partitioning = false;  ///< Skip live profile; use constant costs.
    std::string cost_profile_cache_path;      ///< Empty = disabled.
    bool enable_dynamic_rebalance = true;
    double rebalance_imbalance_threshold = 1.3;
    uint64_t rebalance_check_interval_cycles = 8192;
    double rebalance_min_gain = 0.05;
    uint64_t rebalance_cooldown_cycles = 0;
    std::string partition_solver = "Weighted";
    double sa_critical_path_weight = 0.0;  ///< 0 disables the SA critical-path term.
    SchedulerTimelineTraceConfig timeline_trace;

    /// Builder auto-creates observation contexts when present and enabled.
    std::optional<observe::ObservationYAMLConfig> observation;

    /// Keyed by instance name (path) used for port resolution.
    std::unordered_map<std::string, UnitConfig> units;

    std::vector<PortConnectionSpec> connections;

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
        for (const auto& [name, _] : units) {
            names.push_back(name);
        }
        return names;
    }

    void clear() {
        num_workers = 4;
        enable_parallel = true;
        enable_lookahead = true;
        trace_execution = false;
        max_lookahead_cycles = 100;
        epoch_size = 64;
        run_cycles = 0;
        name = "simulation";
        tick_frequency_hz = 1'000'000'000;
        enable_weighted_partitioning = true;
        profiling_warmup_cycles = 512;
        profiling_measurement_cycles = 1024;
        enable_dynamic_rebalance = true;
        rebalance_imbalance_threshold = 1.3;
        rebalance_check_interval_cycles = 8192;
        rebalance_min_gain = 0.05;
        rebalance_cooldown_cycles = 0;
        partition_solver = "Weighted";
        sa_critical_path_weight = 0.0;
        timeline_trace = SchedulerTimelineTraceConfig{};
        observation.reset();
        units.clear();
        connections.clear();
    }
};

}  // namespace chronon::sender::config
