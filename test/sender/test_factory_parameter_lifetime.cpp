// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "../TestAssertions.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;
namespace factory = chronon::sender::factory;
namespace config = chronon::sender::config;

namespace {

struct LifetimeParams : ParameterSet {
    inline static std::unordered_set<const LifetimeParams*> live;
    inline static size_t created = 0;
    inline static size_t destroyed = 0;
    Param<int> value{this, "value", 7, "Value read through the retained parameter pointer"};

    LifetimeParams() {
        live.insert(this);
        ++created;
    }
    ~LifetimeParams() override {
        CHECK(live.erase(this) == 1);
        ++destroyed;
    }
};

class LifetimeUnit final : public TickableUnit {
public:
    using ParameterSet = LifetimeParams;
    static constexpr const char* unit_type_name = "FactoryLifetimeTest";
    static constexpr const char* unit_description = "Parameter lifetime regression";
    inline static size_t live = 0;
    inline static std::function<void()> constructor_hook;

    explicit LifetimeUnit(const ParameterSet* params)
        : TickableUnit("lifetime"), params_(params), expected_(params->value) {
        auto hook = std::move(constructor_hook);
        constructor_hook = {};
        if (hook) hook();
        if (expected_ < 0) throw std::runtime_error("deliberate constructor failure");
        ++live;
    }

    ~LifetimeUnit() override {
        checkParameters();
        --live;
    }

    void tick() override { checkParameters(); }

    void checkParameters() const {
        CHECK(LifetimeParams::live.contains(params_));
        CHECK(params_->value.value() == expected_);
    }

private:
    const ParameterSet* params_;
    int expected_;
};

TickSimulationConfig simulationConfig() {
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    return config;
}

factory::ISenderFactory& registeredFactory() {
    auto& registry = factory::SenderFactoryRegistry::instance();
    if (!registry.hasFactory(LifetimeUnit::unit_type_name)) {
        registry.registerFactory<LifetimeUnit>(LifetimeUnit::unit_type_name,
                                               LifetimeUnit::unit_description);
    }
    return *registry.getFactory(LifetimeUnit::unit_type_name);
}

LifetimeUnit* create(TickSimulation& simulation, int value = 7) {
    auto yaml = YAML::Load("value: " + std::to_string(value));
    return static_cast<LifetimeUnit*>(
        registeredFactory().createUnit(&simulation, "lifetime", yaml));
}

template <typename Exception, typename Fn>
void expectError(Fn&& fn, const std::string& message) {
    bool threw = false;
    try {
        fn();
    } catch (const Exception& error) {
        CHECK(std::string(error.what()).find(message) != std::string::npos);
        threw = true;
    }
    CHECK(threw);
}

void checkReleased() {
    CHECK(LifetimeUnit::live == 0);
    CHECK(LifetimeParams::live.empty());
    CHECK(LifetimeParams::created == LifetimeParams::destroyed);
}

void repeatedSuccess() {
    auto* factory = &registeredFactory();
    for (int i = 0; i < 200; ++i) {
        {
            TickSimulation simulation(simulationConfig());
            create(simulation, i)->checkParameters();
            create(simulation, i + 1)->checkParameters();
            CHECK(LifetimeParams::live.size() == 2);
            simulation.initialize();
            CHECK(simulation.run(2) == 2);
        }
        checkReleased();
        CHECK(&registeredFactory() == factory);
    }
}

void constructorFailure() {
    TickSimulation simulation(simulationConfig());
    auto* survivor = create(simulation, 42);
    for (int i = 0; i < 200; ++i) {
        expectError<std::runtime_error>([&] { create(simulation, -1); },
                                        "deliberate constructor failure");
        CHECK(simulation.unitCount() == 1);
        CHECK(LifetimeParams::live.size() == 1);
        survivor->checkParameters();
    }
    auto* next = create(simulation, 99);
    CHECK(next->id() == 1);
    CHECK(simulation.unitCount() == 2);
    simulation.initialize();
    CHECK(simulation.run(2) == 2);
}

void deserializationFailure() {
    TickSimulation simulation(simulationConfig());
    auto* survivor = create(simulation);
    const auto invalid = YAML::Load("value: not_an_integer");
    for (int i = 0; i < 200; ++i) {
        expectError<std::exception>(
            [&] { registeredFactory().createUnit(&simulation, "bad", invalid); }, "");
        CHECK(LifetimeParams::live.size() == 1);
        CHECK(simulation.unitCount() == 1);
        survivor->checkParameters();
    }
}

std::string yamlConfig() {
    return R"yaml(
simulation:
  name: factory_lifetime
  num_workers: 1
  enable_parallel: false
  observation:
    enabled: false
  unit:
    good:
      type: FactoryLifetimeTest
      params:
        value: 13
)yaml";
}

void builderFailures() {
    auto* factory = &registeredFactory();
    config::SenderSimulationBuilder builder;
    for (int i = 0; i < 100; ++i) {
        expectError<config::BuildError>(
            [&] {
                (void)builder.buildFromYAMLString(yamlConfig() + R"yaml(
    missing_type:
      type: FactoryLifetimeMissingType
)yaml");
            },
            "CONFIGURING");
        checkReleased();
        expectError<config::BuildError>(
            [&] {
                (void)builder.buildFromYAMLString(yamlConfig() + R"yaml(
      port:
        missing_out:
          to: good.missing_in
          delay: 1
)yaml");
            },
            "BINDING");
        checkReleased();
        {
            auto result = builder.buildFromYAMLString(yamlConfig());
            CHECK(result.units_created == 1);
            CHECK(LifetimeParams::live.size() == 1);
            static_cast<LifetimeUnit*>(result.unit_map.at("good"))->checkParameters();
        }
        checkReleased();
        CHECK(&registeredFactory() == factory);
    }
}

void overlappingSimulations() {
    auto first = std::make_unique<TickSimulation>(simulationConfig());
    auto second = std::make_unique<TickSimulation>(simulationConfig());
    for (int i = 0; i < 3; ++i) create(*first, 10 + i);
    auto* a = create(*second, 20);
    auto* b = create(*second, 21);
    CHECK(LifetimeParams::live.size() == 5);
    first.reset();
    CHECK(LifetimeParams::live.size() == 2);
    a->checkParameters();
    b->checkParameters();
    {
        TickSimulation third(simulationConfig());
        create(third, 30)->checkParameters();
        CHECK(LifetimeParams::live.size() == 3);
    }
    CHECK(LifetimeParams::live.size() == 2);
    second->initialize();
    CHECK(second->run(2) == 2);
    second.reset();
    checkReleased();
}

void nestedConstruction() {
    TickSimulation simulation(simulationConfig());
    auto* survivor = create(simulation, 42);
    LifetimeUnit* child = nullptr;
    LifetimeUnit::constructor_hook = [&] { child = create(simulation, 17); };
    expectError<std::runtime_error>([&] { create(simulation, -1); },
                                    "deliberate constructor failure");
    CHECK(child != nullptr);
    CHECK(simulation.unitCount() == 2);
    CHECK(LifetimeParams::live.size() == 2);
    survivor->checkParameters();
    child->checkParameters();
    CHECK(child->id() == 1);
    CHECK(create(simulation, 23)->id() == 2);
    simulation.initialize();
    CHECK(simulation.run(2) == 2);
}

void initializedSimulation() {
    TickSimulation simulation(simulationConfig());
    auto* survivor = create(simulation);
    simulation.initialize();
    for (int i = 0; i < 100; ++i) {
        expectError<std::logic_error>([&] { create(simulation); },
                                      "cannot create units after initialization");
        CHECK(LifetimeParams::live.size() == 1);
        survivor->checkParameters();
    }
    CHECK(simulation.run(2) == 2);
}

}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string mode = argv[1];
    if (mode == "success")
        repeatedSuccess();
    else if (mode == "constructor")
        constructorFailure();
    else if (mode == "deserialize")
        deserializationFailure();
    else if (mode == "builder")
        builderFailures();
    else if (mode == "overlapping")
        overlappingSimulations();
    else if (mode == "nested")
        nestedConstruction();
    else if (mode == "initialized")
        initializedSimulation();
    else
        return 2;
    checkReleased();
    std::cout << "PASSED " << mode << ": created=" << LifetimeParams::created
              << " destroyed=" << LifetimeParams::destroyed << " live_params=0 live_units=0\n";
}
