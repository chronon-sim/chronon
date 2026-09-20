// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "../TestAssertions.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;

namespace {

template <typename Fn>
void rejects(Fn&& fn) {
    bool threw = false;
    try {
        fn();
    } catch (const std::exception&) {
        threw = true;
    }
    CHECK(threw);
}

TickSimulationConfig config(size_t threads = 1) {
    TickSimulationConfig result;
    result.num_threads = threads;
    result.enable_parallel = threads > 1;
    result.enable_dynamic_rebalance = false;
    return result;
}

class LifecycleUnit : public TickableUnit, public ObservableUnit {
public:
    LifecycleUnit(std::string name, std::vector<std::string>& events, bool fail_init = false,
                  bool fail_finish = false)
        : TickableUnit(std::move(name)),
          events_(events),
          fail_init_(fail_init),
          fail_finish_(fail_finish) {}

    void initialize() override {
        CHECK(state() == UnitState::Created);
        events_.push_back("init:" + name());
        if (fail_init_) throw std::runtime_error("init failed");
    }
    void tick() override {
        CHECK(state() == UnitState::Initialized);
        CHECK(getObserveCycle() == localCycle());
        ++ticks;
    }
    void finalize() override {
        CHECK(state() == UnitState::Finalized);
        events_.push_back("finish:" + fullPath());
        if (fail_finish_) throw std::runtime_error("finish failed");
    }

    uint64_t ticks = 0;

private:
    std::vector<std::string>& events_;
    bool fail_init_, fail_finish_;
};

void lifecycle(size_t threads) {
    std::vector<std::string> events;
    {
        TickSimulation sim(config(threads));
        auto* a = sim.createUnit<LifecycleUnit>("a", events);
        auto* b = sim.createUnit<LifecycleUnit>("b", events);
        CHECK(a->state() == UnitState::Created);
        sim.initialize();
        sim.initialize();
        CHECK(a->state() == UnitState::Initialized);
        CHECK(sim.run(3) == 3);
        CHECK(sim.run(5) == 5);
        CHECK(a->ticks == 8 && b->ticks == 8);
        CHECK(events == (std::vector<std::string>{"init:a", "init:b"}));
        CHECK(a->getObserveCycle() == 8);
        sim.finalize();
        sim.finalize();
        CHECK(sim.isFinalized());
        CHECK(a->state() == UnitState::Finalized);
        rejects([&] { sim.run(1); });
        rejects([&] { sim.runUntil([] { return true; }); });
        rejects([&] { sim.createUnit<LifecycleUnit>("late", events); });
    }
    CHECK(events == (std::vector<std::string>{"init:a", "init:b", "finish:a", "finish:b"}));
}

// Type-erased ownership must preserve the original derived pointer and the
// observation cross-cast, including a virtual TickableUnit base.
void virtualUnitRegistration() {
    class VirtualUnit : public virtual TickableUnit, public ObservableUnit {
    public:
        VirtualUnit() : TickableUnit("virtual") {}
        void tick() override { CHECK(getObserveCycle() == localCycle()); }
    };
    TickSimulation sim(config());
    auto* unit = sim.createUnit<VirtualUnit>();
    CHECK(sim.getUnit<VirtualUnit>("virtual") == unit);
    CHECK(unit->id() == 0);
    sim.run(3);
    CHECK(unit->getObserveCycle() == 3);
    sim.finalize();
    CHECK(unit->state() == UnitState::Finalized);
}

void lifecycleFailures() {
    std::vector<std::string> events;
    {
        TickSimulation sim(config());
        sim.createUnit<LifecycleUnit>("a", events, false, true);
        sim.createUnit<LifecycleUnit>("b", events, true);
        sim.createUnit<LifecycleUnit>("c", events);
        rejects([&] { sim.initialize(); });
        rejects([&] { sim.initialize(); });
        rejects([&] { sim.run(1); });
        rejects([&] { sim.finalize(); });
        sim.finalize();
    }
    CHECK(events == (std::vector<std::string>{"init:a", "init:b", "finish:a"}));
    events.clear();
    {
        TickSimulation sim(config());
        sim.createUnit<LifecycleUnit>("a", events, false, true);
        sim.createUnit<LifecycleUnit>("b", events);
        sim.run(1);
        rejects([&] { sim.finalize(); });
    }
    CHECK(events == (std::vector<std::string>{"init:a", "init:b", "finish:a", "finish:b"}));
    events.clear();
    {
        TickSimulation sim(config());
        sim.createUnit<LifecycleUnit>("a", events);
        sim.run(1);
    }
    CHECK(events == (std::vector<std::string>{"init:a", "finish:a"}));
}

void pollingPredicateBoundaries() {
    for (size_t threads : {size_t{1}, size_t{4}}) {
        auto cfg = config(threads);
        cfg.enable_weighted_partitioning = false;
        cfg.setPollingIntervalCycles(2);
        std::vector<std::string> events;
        TickSimulation sim(cfg);
        auto* a = sim.createUnit<LifecycleUnit>("a", events);
        auto* b = sim.createUnit<LifecycleUnit>("b", events);
        for (size_t i = 2; i < 12; ++i)
            sim.createUnit<LifecycleUnit>("extra_" + std::to_string(i), events);
        sim.initialize();
        CHECK(sim.useParallelExecution() == (threads > 1));
        std::vector<uint64_t> observed;
        CHECK(sim.runUntil(
                  [&] {
                      observed.push_back(sim.currentCycle());
                      // A predicate may advance the simulation itself. That
                      // advancement is outside this runUntil call's limit.
                      if (observed.size() == 1) CHECK(sim.run(1) == 1);
                      return false;
                  },
                  5) == 5);
        CHECK(observed == (std::vector<uint64_t>{0, 3, 5}));
        CHECK(sim.currentCycle() == 6 && a->ticks == 6 && b->ticks == 6);
        // The next advancement must still reject finalization by a predicate.
        rejects([&] {
            sim.runUntil(
                [&] {
                    sim.finalize();
                    return false;
                },
                1);
        });
        CHECK(a->ticks == 6 && b->ticks == 6);
    }
}

class NamedUnit : public AutoRegisteredUnit<NamedUnit>, public ObservableUnit {
public:
    using ParameterSet = chronon::ParameterSet;
    static constexpr const char* unit_type_name = "APIContractNamedUnit";
    static constexpr const char* unit_description = "Instance identity and time binding";
    explicit NamedUnit(const ParameterSet*) : AutoRegisteredUnit("default_name") {}
    OutPort<int> out{this, "out"};
    InPort<int> in{this, "in"};
    void tick() override {
        CHECK(getObserveCycle() == localCycle());
        if (auto value = in.tryReceive()) received.push_back(*value);
        if (out.send(next)) ++next;
    }
    int next = 0;
    std::vector<int> received;
};

void identityAndReceive() {
    SenderSimulationBuilder builder;
    auto result = builder.buildFromYAMLString(R"yaml(
simulation:
  name: api_contract
  num_workers: 1
  unit:
    producer:
      type: APIContractNamedUnit
      port:
        out: {to: consumer.in, delay: 1}
    consumer:
      type: APIContractNamedUnit
)yaml");
    auto* producer = result.simulation->getUnit<NamedUnit>("producer");
    auto* consumer = result.simulation->getUnit<NamedUnit>("api_contract.consumer");
    CHECK(producer && consumer);
    CHECK(producer->name() == "producer");
    CHECK(std::string(producer->crashName()) == "producer");
    CHECK(result.unit_map.at("producer") == producer);
    result.simulation->run(5);
    CHECK(consumer->received == (std::vector<int>{0, 1, 2, 3}));
    rejects([&] { result.simulation->setUnitName(*producer, "changed"); });
    TickSimulation other(config());
    auto* outside = other.createUnit<NamedUnit>(nullptr);
    rejects([&] { other.connect(outside->out, consumer->in); });
}

void independentPortDirectories() {
    SenderSimulationBuilder builder;
    constexpr auto yaml = R"yaml(
simulation:
  name: same_name
  num_workers: 1
  unit:
    producer:
      type: APIContractNamedUnit
      port:
        out: {to: consumer.in, delay: 1}
    consumer:
      type: APIContractNamedUnit
)yaml";
    auto first = builder.buildFromYAMLString(yaml);
    auto* first_producer = first.simulation->getUnit<NamedUnit>("producer");
    auto* first_consumer = first.simulation->getUnit<NamedUnit>("consumer");
    {
        auto second = builder.buildFromYAMLString(yaml);
        auto* second_producer = second.simulation->getUnit<NamedUnit>("producer");
        auto* second_consumer = second.simulation->getUnit<NamedUnit>("consumer");
        CHECK(first.simulation->portDirectory().findPort("same_name.producer.out")->owner() ==
              first_producer);
        CHECK(second.simulation->portDirectory().findPort("same_name.producer.out")->owner() ==
              second_producer);
        first_producer->next = 100;
        second_producer->next = 200;
        first.simulation->run(3);
        second.simulation->run(3);
        CHECK(first_consumer->received == (std::vector<int>{100, 101}));
        CHECK(second_consumer->received == (std::vector<int>{200, 201}));
    }
    first.simulation->run(2);
    CHECK(first_consumer->received == (std::vector<int>{100, 101, 102, 103}));
    CHECK(first.simulation->portDirectory().findPort("same_name.producer.out")->owner() ==
          first_producer);
}

void configurationContracts() {
    TickSimulationConfig runtime;
    runtime.setExecutionPolicy(ExecutionPolicy::Sequential);
    CHECK(runtime.executionPolicy() == ExecutionPolicy::Sequential);
    runtime.setExecutionPolicy(ExecutionPolicy::Auto);
    CHECK(runtime.enable_parallel && runtime.enable_lookahead &&
          runtime.enable_epoch_free_lookahead);
    runtime.setPollingIntervalCycles(0);
    CHECK(runtime.epoch_size == 0 && runtime.pollingIntervalCycles() == 0);
    chronon::sender::config::SenderConfigLoader loader;
    const auto canonical = loader.loadFromString(
        "simulation: {num_workers: 4, execution_policy: sequential, polling_interval_cycles: 7}");
    const auto legacy = loader.loadFromString(
        "simulation: {num_workers: 4, enable_lookahead: false, epoch_size: 7}");
    CHECK(canonical.toRuntimeConfig().executionPolicy() ==
          legacy.toRuntimeConfig().executionPolicy());
    CHECK(canonical.toRuntimeConfig().pollingIntervalCycles() ==
          legacy.toRuntimeConfig().pollingIntervalCycles());
    rejects([&] { loader.loadFromString("simulation: {execution_policy: unknown}"); });
    rejects([&] {
        loader.loadFromString("simulation: {execution_policy: auto, enable_parallel: false}");
    });
    rejects(
        [&] { loader.loadFromString("simulation: {polling_interval_cycles: 7, epoch_size: 8}"); });
    auto manual = canonical;
    manual.units["z"].instance_name = "z";
    manual.units["a"].instance_name = "a";
    CHECK(manual.unitNames() == (std::vector<std::string>{"a", "z"}));
    manual.unit_order = {"z"};
    CHECK(manual.unitNames() == (std::vector<std::string>{"z", "a"}));
    const auto port_config = loader.loadFromString(R"yaml(
simulation:
  unit:
    source:
      type: APIContractNamedUnit
      port:
        out: {to: target.in, destination_depth: 8, capacity: 8}
    target: {type: APIContractNamedUnit}
)yaml");
    CHECK(port_config.connections.at(0).capacity == 8);
    rejects([&] {
        loader.loadFromString(R"yaml(
simulation:
  unit:
    source:
      type: APIContractNamedUnit
      port:
        out: {to: target.in, destination_depth: 8, capacity: 4}
)yaml");
    });
}

void pollingCLIOverrides() {
    const auto path = std::filesystem::temp_directory_path() / "chronon-api-polling.yaml";
    for (bool canonical_yaml : {false, true}) {
        {
            std::ofstream file(path);
            file << "simulation: {num_workers: 1, run_cycles: 1, "
                 << (canonical_yaml ? "polling_interval_cycles" : "epoch_size") << ": 64}\n";
        }
        for (const char* option : {"--epoch-size", "--polling-interval-cycles"}) {
            for (bool equals : {false, true}) {
                bool built = false;
                SimulationApp app("polling override");
                app.setDefaultConfig(path.string()).onPostBuild([&](auto& result) {
                    built = true;
                    CHECK(result.config.epoch_size == 0);
                });
                std::string argument = option;
                if (equals) argument += "=0";
                char program[] = "polling_test", value[] = "0";
                char* argv[] = {program, argument.data(), value};
                CHECK(app.run(equals ? 2 : 3, argv) == 0);
                CHECK(built);
            }
        }
    }
    std::filesystem::remove(path);
}

void portCapacityContracts() {
    OutPort<int> standalone_out{nullptr, "out", SendRate{3}};
    InPort<int> standalone_in{nullptr, "in", QueueDepth{8}};
    CHECK(standalone_out.sendRate().entries_per_cycle == 3);
    CHECK(standalone_in.queueDepth().entries == 8);
    for (bool reverse : {false, true}) {
        TickSimulation sim(config());
        auto* a = sim.createUnit<NamedUnit>(nullptr);
        auto* b = sim.createUnit<NamedUnit>(nullptr);
        auto* c = sim.createUnit<NamedUnit>(nullptr);
        auto* first = sim.connect(a->out, c->in);
        auto* second = sim.connect(b->out, c->in);
        first->configureRegisteredEdge(reverse ? 8 : 4, {});
        second->configureRegisteredEdge(reverse ? 4 : 8, {});
        rejects([&] { sim.registerConnection(first); });
        rejects([&] { sim.initialize(); });
        CHECK(c->in.queueDepth().entries == InPort<int>::UNLIMITED_CAPACITY);
    }
    TickSimulation sim(config());
    auto* a = sim.createUnit<NamedUnit>(nullptr);
    auto* b = sim.createUnit<NamedUnit>(nullptr);
    auto* c = sim.createUnit<NamedUnit>(nullptr);
    sim.connect(a->out, c->in)->configureRegisteredEdge(8, {});
    sim.connect(b->out, c->in)->configureRegisteredEdge(8, {});
    sim.run(4);
    CHECK(c->in.queueDepth().entries == 8);
}

void observationSessionLifetime() {
    auto& manager = chronon::observe::ObservationManager::instance();
    chronon::observe::ObservationYAMLConfig observation;
    observation.enabled = true;
    observation.output_dir = "/tmp/chronon-api-session-test";
    for (int iteration = 0; iteration != 2; ++iteration) {
        {
            TickSimulation owner(config());
            owner.configureObservation(observation);
            const auto* queue = manager.sharedQueue();
            {
                TickSimulation other(config());
                rejects([&] { other.configureObservation(observation); });
                rejects([&] { other.initialize(); });
                rejects([&] { manager.initialize(observation); });
                rejects([&] { manager.reset(); });
            }
            CHECK(manager.sharedQueue() == queue);
            auto* unit = owner.createUnit<NamedUnit>(nullptr);
            unit->setObservationContext(
                manager.createContextForUnit("named", [unit] { return unit->localCycle(); }));
            owner.run(3);
            CHECK(unit->getObserveCycle() == 3);
            owner.finalize();
        }
        CHECK(!manager.isInitialized());
    }
    {
        TickSimulation running(config());
        running.initialize();
        TickSimulation other(config());
        rejects([&] { other.configureObservation(observation); });
    }
    SenderSimulationBuilder builder;
    rejects([&] {
        builder.buildFromYAMLString(R"yaml(
simulation:
  num_workers: 1
  observation: {enabled: true}
  unit:
    invalid: {type: ThisFactoryDoesNotExist}
)yaml");
    });
    CHECK(!manager.isInitialized());
}

}  // namespace

int main() {
    virtualUnitRegistration();
    lifecycle(1);
    lifecycle(4);
    lifecycleFailures();
    pollingPredicateBoundaries();
    identityAndReceive();
    independentPortDirectories();
    configurationContracts();
    pollingCLIOverrides();
    portCapacityContracts();
    observationSessionLifetime();
    std::cout << "API contracts passed\n";
}
