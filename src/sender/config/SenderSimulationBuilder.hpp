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
        std::unique_ptr<tree::TreeNode> root_node;
        std::unique_ptr<TickSimulation> simulation;
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
    void phaseBuild(Result& result) {
        try {
            result.simulation = std::make_unique<TickSimulation>(result.config.toRuntimeConfig());
            for (const auto& clock : result.config.clocks) result.simulation->addClockDomain(clock);
        } catch (const std::invalid_argument& error) {
            throw BuildError("building", error.what());
        }

        result.root_node = std::make_unique<tree::TreeNode>(result.config.name);

        if (result.config.observation && result.config.observation->enabled) {
            result.simulation->configureObservation(*result.config.observation);
            result.observation_enabled = true;
        }
    }

    void phaseConfigure(Result& result) {
        auto& registry = factory::SenderFactoryRegistry::instance();

        for (const auto& name : result.config.unitNames()) {
            const auto* unit_config_ptr = result.config.getUnit(name);
            if (!unit_config_ptr) {
                throw BuildError("CONFIGURING",
                                 "Unit order references missing unit '" + name + "'");
            }
            const auto& unit_config = *unit_config_ptr;
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
            if (unit_config.clock) {
                try {
                    result.simulation->assignClockDomain(*unit,
                                                         result.config.clockId(*unit_config.clock));
                } catch (const std::exception& error) {
                    throw BuildError("CONFIGURING", "unit '" + name + "': " + error.what());
                }
            }
            if (unit_config.has_tick_interval) {
                unit->setTickInterval(unit_config.tick_interval);
            }

            auto unit_node = std::make_unique<tree::TreeNode>(name, result.root_node.get());
            tree::TreeNode* unit_node_ptr = unit_node.get();
            result.root_node->addChild(name, std::move(unit_node));
            result.simulation->bindTreeNode(*unit, *unit_node_ptr);

            result.unit_map[name] = unit;
            result.units_created++;
        }

        if (result.config.run && result.config.run->kind == ClockRunLimit::Kind::DomainCycles) {
            const auto id = result.config.clockId(result.config.run->clock);
            if (std::none_of(
                    result.unit_map.begin(), result.unit_map.end(),
                    [id](const auto& entry) { return entry.second->clockDomainId() == id; }))
                throw BuildError("CONFIGURING", "run.domain_cycles clock '" +
                                                    result.config.run->clock + "' has no units");
        }

        // Contexts depend on full unit set, so attach after all units are created.
        if (result.observation_enabled) {
            attachObservationContexts(result);
        }
    }

    void attachObservationContexts(Result& result) {
        auto& obs_mgr = observe::ObservationManager::instance();

        for (const auto& name : result.config.unitNames()) {
            auto it = result.unit_map.find(name);
            if (it == result.unit_map.end()) {
                continue;
            }
            auto* unit = it->second;
            auto* obs_unit = dynamic_cast<observe::ObservableUnit*>(unit);
            if (obs_unit) {
                // Hierarchical fullPath() (e.g. "cpu0.fetch") gives per-instance counters.
                auto* ctx = obs_mgr.createContextForUnit(
                    unit->fullPath(), [unit]() { return unit->localCycle(); }, 0);

                if (ctx) {
                    obs_unit->setObservationContext(ctx);
                }
            }
        }
    }

    void phaseBind(Result& result) {
        auto& port_dir = result.simulation->portDirectory();
        auto& bind_registry = PortBindingRegistry::instance();

        for (const auto& [name, unit] : result.unit_map) {
            registerUnitPorts(unit, port_dir, result.ports_registered);
        }

        std::string root_prefix = result.root_node->name() + ".";

        uint32_t cdc_id = 1;
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

            try {
                auto* async_source = dynamic_cast<IAsyncPortHandle*>(source);
                auto* async_dest = dynamic_cast<IAsyncPortHandle*>(dest);
                if (conn_spec.cdc) {
                    if (!async_source || !async_dest)
                        throw std::invalid_argument(
                            "cdc requires AsyncWritePort<T> -> AsyncReadPort<T>; ordinary ports "
                            "cannot cross a CDC bridge");
                    result.simulation->connectAsyncFifo(cdc_id++, *async_source, *async_dest,
                                                        *conn_spec.cdc);
                } else {
                    if (async_source || async_dest)
                        throw std::invalid_argument(
                            "async endpoints require explicit cdc: {type: async_fifo}");
                    if (source->owner()->clockDomainId() != dest->owner()->clockDomainId())
                        throw std::invalid_argument(
                            "ordinary connection crosses clock domains; use "
                            "AsyncWritePort/AsyncReadPort and cdc: {type: async_fifo}");
                    auto* conn = bind_registry.bind(source, dest, conn_spec.delay);
                    conn->configureRegisteredEdge(conn_spec.capacity, conn_spec.rate);
                    result.simulation->registerConnection(conn);
                }
            } catch (const std::exception& error) {
                throw BuildError("BINDING",
                                 "'" + source_path + "' -> '" + dest_path + "': " + error.what());
            }
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
