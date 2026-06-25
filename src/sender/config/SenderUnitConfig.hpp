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
    uint64_t epoch_size = 64;                 ///< Synchronization period in cycles.
    bool enable_epoch_free_lookahead = true;  ///< Drop the per-epoch barrier when safe.
    uint64_t run_cycles = 0;                  ///< 0 = run until completion.
    std::string name = "simulation";
    uint64_t tick_frequency_hz = 1'000'000'000;  ///< Default 1 GHz.

    bool enable_weighted_partitioning = true;
    bool enable_dynamic_rebalance = true;
    double rebalance_imbalance_threshold = 1.3;
    uint64_t rebalance_check_interval_cycles = 8192;
    double rebalance_min_gain = 0.05;
    uint64_t rebalance_cooldown_cycles = 0;
    std::string partition_solver = "SA";
    double sa_critical_path_weight = 0.0;  ///< 0 disables the SA critical-path term.
    double initial_partition_sync_cost_ns =
        8.0;  ///< Locality weight for the initial partition; 0 = pure load balance.
    SchedulerTimelineTraceConfig timeline_trace;

    /// Builder auto-creates observation contexts when present and enabled.
    std::optional<observe::ObservationYAMLConfig> observation;

    /// Keyed by instance name (path) used for port resolution.
    std::unordered_map<std::string, UnitConfig> units;
    /// Unit instance names in YAML declaration order.
    std::vector<std::string> unit_order;

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

        for (const auto& name : unit_order) {
            if (hasUnit(name)) {
                names.push_back(name);
            }
        }

        for (const auto& [name, _] : units) {
            if (std::find(names.begin(), names.end(), name) == names.end()) {
                names.push_back(name);
            }
        }
        return names;
    }
};

}  // namespace chronon::sender::config
