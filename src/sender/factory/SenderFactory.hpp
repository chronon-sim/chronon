// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <yaml-cpp/yaml.h>

#include <concepts>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "../../params/ParameterSet.hpp"
#include "../../tree/TreeNode.hpp"
#include "../core/PhasedTickableUnit.hpp"
#include "../core/TickSimulation.hpp"
#include "../core/TickableUnit.hpp"
#include "../core/Unit.hpp"
#include "../util/ThreadSafeRegistry.hpp"

namespace chronon::sender::factory {

template <typename UnitT>
concept SenderFactoryUnit =
    std::derived_from<UnitT, TickableUnit> &&
    requires {
        typename UnitT::ParameterSet;
        { UnitT::unit_type_name } -> std::convertible_to<std::string_view>;
        { UnitT::unit_description } -> std::convertible_to<std::string_view>;
    } && std::derived_from<typename UnitT::ParameterSet, params::ParameterSet> &&
    std::constructible_from<UnitT, const typename UnitT::ParameterSet*>;

/** @brief Type-erased factory interface for runtime unit creation by YAML type name. */
class ISenderFactory {
public:
    virtual ~ISenderFactory() = default;

    virtual const std::string& typeName() const = 0;
    virtual const std::string& description() const = 0;

    /// @returns Pointer to the created unit (owned by @p sim).
    virtual Unit* createUnit(TickSimulation* sim, const std::string& name,
                             const YAML::Node& yaml_params) = 0;
};

/**
 * @brief Template factory that creates instances of a specific Unit subclass.
 *
 * UnitT must derive from sender::Unit, declare `using ParameterSet = ...`, define
 * `static constexpr const char* unit_type_name` and `unit_description`, and accept
 * a `const ParameterSet*` via `sim->createUnit<UnitT>(params)`.
 */
template <SenderFactoryUnit UnitT>
class SenderFactory : public ISenderFactory {
public:
    using ParameterSetT = typename UnitT::ParameterSet;

    SenderFactory(std::string type_name, std::string description)
        : type_name_(std::move(type_name)), description_(std::move(description)) {}

    const std::string& typeName() const override { return type_name_; }
    const std::string& description() const override { return description_; }

    Unit* createUnit(TickSimulation* sim, [[maybe_unused]] const std::string& name,
                     const YAML::Node& yaml_params) override {
        auto params = std::make_shared<ParameterSetT>();
        if (yaml_params.IsDefined() && !yaml_params.IsNull()) {
            params->deserializeYAML(&yaml_params);
        }

        // Keep params alive for the unit's full lifetime.
        params_storage_.push_back(params);

        UnitT* unit = sim->createUnit<UnitT>(params.get());

        return unit;
    }

private:
    std::string type_name_;
    std::string description_;

    std::vector<std::shared_ptr<ParameterSetT>> params_storage_;
};

/** @brief Thread-safe singleton registry of unit factories keyed by YAML type name. */
class SenderFactoryRegistry : public chronon::ThreadSafeRegistry<ISenderFactory> {
public:
    static SenderFactoryRegistry& instance() {
        static SenderFactoryRegistry registry;
        return registry;
    }

    template <SenderFactoryUnit UnitT>
    void registerFactory(const std::string& type_name, const std::string& description) {
        insert(type_name, std::make_unique<SenderFactory<UnitT>>(type_name, description));
    }

    ISenderFactory* getFactory(const std::string& type_name) { return find(type_name); }
    bool hasFactory(const std::string& type_name) const { return has(type_name); }
    std::vector<std::string> listFactories() const { return keys(); }

    std::vector<std::pair<std::string, std::string>> listFactoriesWithDescriptions() const {
        std::vector<std::pair<std::string, std::string>> result;
        forEach([&](const std::string& name, const ISenderFactory& f) {
            result.emplace_back(name, f.description());
        });
        return result;
    }

private:
    SenderFactoryRegistry() = default;
};

/**
 * @brief CRTP base that auto-registers @p Derived with SenderFactoryRegistry at program load.
 *
 * @p Derived must declare static `unit_type_name` / `unit_description`, a `ParameterSet`
 * type, a constructor accepting `const ParameterSet*`, and override `tick()` /
 * `isCompleted()`.
 */
template <typename Derived>
class AutoRegisteredUnit : public TickableUnit {
public:
    explicit AutoRegisteredUnit(std::string name) : TickableUnit(std::move(name)) {
        // Force instantiation of registrar_ so the side effect runs.
        (void)registrar_;
    }

protected:
    /// Tag overload for derived classes needing custom construction.
    AutoRegisteredUnit(std::string name, bool /*tag*/) : TickableUnit(std::move(name)) {
        (void)registrar_;
    }

private:
    struct Registrar {
        Registrar() {
            SenderFactoryRegistry::instance().registerFactory<Derived>(Derived::unit_type_name,
                                                                       Derived::unit_description);
        }
    };

    // inline static guarantees a single registrar instance across TUs.
    static inline Registrar registrar_{};
};

/**
 * @brief AutoRegisteredUnit variant with automatic Phase0/Phase1 dispatch.
 *
 * For units that combine YAML factory registration with phase-aware pipeline registers.
 * Subclasses implement `tickPhase<P>()` instead of `tick()` — dispatch boilerplate is
 * supplied by PhasedTickableUnit.
 */
template <typename Derived>
class PhasedAutoRegisteredUnit : public PhasedTickableUnit<Derived> {
public:
    explicit PhasedAutoRegisteredUnit(std::string name)
        : PhasedTickableUnit<Derived>(std::move(name)) {
        (void)registrar_;
    }

protected:
    PhasedAutoRegisteredUnit(std::string name, bool /*tag*/)
        : PhasedTickableUnit<Derived>(std::move(name)) {
        (void)registrar_;
    }

private:
    struct Registrar {
        Registrar() {
            SenderFactoryRegistry::instance().registerFactory<Derived>(Derived::unit_type_name,
                                                                       Derived::unit_description);
        }
    };

    static inline Registrar registrar_{};
};

}  // namespace chronon::sender::factory
