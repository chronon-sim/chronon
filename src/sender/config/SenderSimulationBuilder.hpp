// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <algorithm>
#include <cctype>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../observe/ObservableUnit.hpp"
#include "../../observe/ObservationManager.hpp"
#include "../../tree/TreeNode.hpp"
#include "../core/TickSimulation.hpp"
#include "../factory/SenderFactory.hpp"
#include "../port/PortDirectory.hpp"
#include "SenderConfigLoader.hpp"
#include "SenderUnitConfig.hpp"

namespace chronon::sender::config {

/** @brief Thrown when SenderSimulationBuilder fails during one of its build phases. */
class BuildError : public std::runtime_error {
public:
    explicit BuildError(const std::string& message) : std::runtime_error(message) {}

    BuildError(const std::string& phase, const std::string& detail)
        : std::runtime_error("Build error in " + phase + " phase: " + detail) {}
};

/**
 * @brief Constructs simulations from YAML configuration in four phases.
 *
 * Phases: BUILDING (parse) → CONFIGURING (create units via factory) →
 * BINDING (register ports, establish connections) → FINALIZED.
 */
class SenderSimulationBuilder {
public:
    /** @brief Build output: simulation, tree, and per-phase statistics. */
    struct Result {
        std::unique_ptr<TickSimulation> simulation;
        std::unique_ptr<tree::TreeNode> root_node;
        SimulationYAMLConfig config;

        size_t units_created = 0;
        size_t ports_registered = 0;
        size_t connections_made = 0;

        std::unordered_map<std::string, Unit*> unit_map;

        bool observation_enabled = false;
    };

    Result buildFromYAML(const std::string& filepath) {
        SenderConfigLoader loader;
        SimulationYAMLConfig config = loader.loadFromFile(filepath);
        return buildFromConfig(std::move(config));
    }

    Result buildFromYAMLString(const std::string& yaml_content,
                               const std::string& source_name = "<string>") {
        SenderConfigLoader loader;
        SimulationYAMLConfig config = loader.loadFromString(yaml_content, source_name);
        return buildFromConfig(std::move(config));
    }

    /// Useful for applying runtime overrides before building.
    Result buildFromYAMLNode(const YAML::Node& yaml_node,
                             const std::string& source_name = "<node>") {
        SenderConfigLoader loader;
        SimulationYAMLConfig config = loader.loadFromNode(yaml_node, source_name);
        return buildFromConfig(std::move(config));
    }

    Result buildFromConfig(SimulationYAMLConfig config) {
        Result result;
        result.config = std::move(config);

        phaseBuild(result);
        phaseConfigure(result);
        phaseBind(result);

        return result;
    }

private:
    static TickSimulationConfig::PartitionSolverType parsePartitionSolverType_(
        const std::string& solver_name) {
        std::string normalized = solver_name;
        std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                       [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
        if (normalized == "weighted") {
            return TickSimulationConfig::PartitionSolverType::Weighted;
        }
        if (normalized == "sa") {
            return TickSimulationConfig::PartitionSolverType::SA;
        }
        throw BuildError("building", "unknown partition_solver '" + solver_name +
                                         "' (expected 'Weighted' or 'SA')");
    }

    void phaseBuild(Result& result) {
        TickSimulationConfig sim_config;
        sim_config.num_threads = result.config.num_workers;
        sim_config.enable_parallel = result.config.enable_parallel && result.config.num_workers > 1;
        sim_config.enable_lookahead = result.config.enable_lookahead;
        sim_config.trace_execution = result.config.trace_execution;
        sim_config.max_lookahead_cycles = result.config.max_lookahead_cycles;
        sim_config.epoch_size = result.config.epoch_size;
        sim_config.enable_epoch_free_lookahead = result.config.enable_epoch_free_lookahead;
        sim_config.enable_weighted_partitioning = result.config.enable_weighted_partitioning;
        sim_config.enable_dynamic_rebalance = result.config.enable_dynamic_rebalance;
        sim_config.rebalance_imbalance_threshold = result.config.rebalance_imbalance_threshold;
        sim_config.rebalance_check_interval_cycles = result.config.rebalance_check_interval_cycles;
        sim_config.rebalance_min_gain = result.config.rebalance_min_gain;
        sim_config.rebalance_cooldown_cycles = result.config.rebalance_cooldown_cycles;
        sim_config.partition_solver = parsePartitionSolverType_(result.config.partition_solver);
        sim_config.sa_critical_path_weight = result.config.sa_critical_path_weight;
        sim_config.initial_partition_sync_cost_ns = result.config.initial_partition_sync_cost_ns;
        sim_config.tick_frequency_hz = result.config.tick_frequency_hz;
        sim_config.timeline_trace = result.config.timeline_trace;

        result.simulation = std::make_unique<TickSimulation>(sim_config);

        result.root_node = std::make_unique<tree::TreeNode>(result.config.name);

        if (result.config.observation && result.config.observation->enabled) {
            observe::ObservationManager::instance().initialize(*result.config.observation);
            result.observation_enabled = true;
        }
    }

    void phaseConfigure(Result& result) {
        auto& registry = factory::SenderFactoryRegistry::instance();

        for (const auto& [name, unit_config] : result.config.units) {
            auto* factory = registry.getFactory(unit_config.type_name);
            if (!factory) {
                throw BuildError("CONFIGURING", "Unknown unit type '" + unit_config.type_name +
                                                    "' for unit '" + name + "'");
            }

            Unit* unit =
                factory->createUnit(result.simulation.get(), name, unit_config.params_yaml);

            if (!unit) {
                throw BuildError("CONFIGURING", "Factory returned null for unit '" + name + "'");
            }

            auto unit_node = std::make_unique<tree::TreeNode>(name, result.root_node.get());
            tree::TreeNode* unit_node_ptr = unit_node.get();
            result.root_node->addChild(name, std::move(unit_node));
            unit->setTreeNode(unit_node_ptr);

            result.unit_map[name] = unit;
            result.units_created++;
        }

        // Contexts depend on full unit set, so attach after all units are created.
        if (result.observation_enabled) {
            attachObservationContexts(result);
        }
    }

    void attachObservationContexts(Result& result) {
        auto& obs_mgr = observe::ObservationManager::instance();

        for (auto& [name, unit] : result.unit_map) {
            auto* obs_unit = dynamic_cast<observe::ObservableUnit*>(unit);
            if (obs_unit) {
                // Hierarchical fullPath() (e.g. "cpu0.fetch") gives per-instance counters.
                auto* ctx = obs_mgr.createContextForUnit(
                    unit->fullPath(),
                    [sim = result.simulation.get()]() { return sim->currentCycle(); }, 0);

                if (ctx) {
                    obs_unit->setObservationContext(ctx);
                }
            }
        }
    }

    void phaseBind(Result& result) {
        auto& port_dir = PortDirectory::instance();
        auto& bind_registry = PortBindingRegistry::instance();

        for (const auto& [name, unit] : result.unit_map) {
            registerUnitPorts(unit, port_dir, result.ports_registered);
        }

        std::string root_prefix = result.root_node->name() + ".";

        for (const auto& conn_spec : result.config.connections) {
            std::string source_path = conn_spec.source_path;
            std::string dest_path = conn_spec.dest_path;

            if (source_path.find(root_prefix) != 0) {
                source_path = root_prefix + source_path;
            }
            if (dest_path.find(root_prefix) != 0) {
                dest_path = root_prefix + dest_path;
            }

            IPortHandle* source = port_dir.findPort(source_path);
            if (!source) {
                throw BuildError("BINDING", "Source port not found: '" + source_path + "' (from '" +
                                                conn_spec.source_path + "')");
            }
            if (!source->isOutPort()) {
                throw BuildError("BINDING", "Source must be an OutPort: '" + source_path + "'");
            }

            IPortHandle* dest = port_dir.findPort(dest_path);
            if (!dest) {
                throw BuildError("BINDING", "Destination port not found: '" + dest_path +
                                                "' (from '" + conn_spec.dest_path + "')");
            }
            if (!dest->isInPort()) {
                throw BuildError("BINDING", "Destination must be an InPort: '" + dest_path + "'");
            }

            if (source->dataTypeIndex() != dest->dataTypeIndex()) {
                throw BuildError("BINDING", "Type mismatch: '" + source_path + "' (" +
                                                source->dataType().name() + ") -> '" + dest_path +
                                                "' (" + dest->dataType().name() + ")");
            }

            auto* conn = bind_registry.bind(source, dest, conn_spec.delay);

            result.simulation->registerConnection(conn);
            result.connections_made++;
        }
    }

    /// Ports auto-register via Unit::addPendingPortRegistration when setTreeNode() is called.
    /// Validate those registrations here so binding failures point at the unit.
    void registerUnitPorts(Unit* unit, PortDirectory& port_dir, size_t& count) {
        std::string prefix = unit->fullPath();

        for (PortBase* port : unit->ports()) {
            std::string full_path = prefix + "." + port->name();
            if (!port_dir.hasPort(full_path)) {
                throw BuildError("BINDING", "Port was not registered: '" + full_path + "'");
            }
            count++;
        }
    }
};

}  // namespace chronon::sender::config
