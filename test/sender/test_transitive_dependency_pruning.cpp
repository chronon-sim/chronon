// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../TestAssertions.hpp"
#include "chronon/Chronon.hpp"
#include "sender/schedule/WeightedDependencyReduction.hpp"

namespace chronon::sender {

struct TransitiveDependencyPruneTestAccess {
    static const std::vector<std::vector<ThreadCrossDep>>& dependencies(const TickSimulation& sim) {
        return sim.thread_cross_deps_temp_;
    }

    static void rebuild(TickSimulation& sim) { sim.buildCrossThreadDependencies(); }

    static bool hasZeroDelayCycle(const TickSimulation& sim) {
        return sim.has_zero_delay_cross_thread_cycle_;
    }

    static const std::vector<std::vector<PartitionInput::EdgeInfo>>& rebalanceAdjacency(
        const TickSimulation& sim) {
        return sim.dynamic_rebalance_adjacency_;
    }
};

}  // namespace chronon::sender

namespace {

using namespace chronon::sender;
namespace reduction = chronon::sender::weighted_dependency_reduction;

class ScopedEnvironmentVariable {
public:
    ScopedEnvironmentVariable(const char* name, const char* value) : name_(name) {
        if (const char* previous = std::getenv(name)) previous_ = previous;
        const int rc = value ? setenv(name, value, 1) : unsetenv(name);
        CHECK(rc == 0);
    }

    ~ScopedEnvironmentVariable() {
        const int rc =
            previous_ ? setenv(name_.c_str(), previous_->c_str(), 1) : unsetenv(name_.c_str());
        CHECK(rc == 0);
    }

    ScopedEnvironmentVariable(const ScopedEnvironmentVariable&) = delete;
    ScopedEnvironmentVariable& operator=(const ScopedEnvironmentVariable&) = delete;

private:
    std::string name_;
    std::optional<std::string> previous_;
};

class PassUnit : public TickableUnit {
public:
    explicit PassUnit(std::string name) : TickableUnit(std::move(name)) {}

    InPort<int> in{this, "in"};
    OutPort<int> out{this, "out"};

    void tick() override {
        if (auto value = in.tryReceive(localCycle())) {
            [[maybe_unused]] const bool sent = out.send(*value);
        }
    }
};

std::vector<reduction::Edge> snapshotDependencies(const TickSimulation& sim) {
    std::vector<reduction::Edge> edges;
    const auto& dependencies = TransitiveDependencyPruneTestAccess::dependencies(sim);
    for (size_t dependent = 0; dependent < dependencies.size(); ++dependent) {
        for (const ThreadCrossDep& dependency : dependencies[dependent]) {
            edges.push_back({dependent, dependency.pred_thread, dependency.min_delay});
        }
    }
    std::sort(edges.begin(), edges.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.dependent != rhs.dependent) return lhs.dependent < rhs.dependent;
        if (lhs.predecessor != rhs.predecessor) return lhs.predecessor < rhs.predecessor;
        return lhs.delay < rhs.delay;
    });
    return edges;
}

std::vector<reduction::Edge> snapshotRebalanceAdjacency(const TickSimulation& sim) {
    std::vector<reduction::Edge> edges;
    const auto& adjacency = TransitiveDependencyPruneTestAccess::rebalanceAdjacency(sim);
    for (size_t predecessor = 0; predecessor < adjacency.size(); ++predecessor) {
        for (const auto& edge : adjacency[predecessor]) {
            edges.push_back({edge.neighbor, predecessor, edge.min_delay});
        }
    }
    std::sort(edges.begin(), edges.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.dependent != rhs.dependent) return lhs.dependent < rhs.dependent;
        if (lhs.predecessor != rhs.predecessor) return lhs.predecessor < rhs.predecessor;
        return lhs.delay < rhs.delay;
    });
    return edges;
}

struct RebalanceEdgeSnapshot {
    size_t dependent = 0;
    size_t predecessor = 0;
    size_t num_connections = 0;
    uint32_t min_delay = 0;

    bool operator==(const RebalanceEdgeSnapshot&) const = default;
};

std::vector<RebalanceEdgeSnapshot> snapshotRebalancePhysicalEdges(const TickSimulation& sim) {
    std::vector<RebalanceEdgeSnapshot> edges;
    const auto& adjacency = TransitiveDependencyPruneTestAccess::rebalanceAdjacency(sim);
    for (size_t predecessor = 0; predecessor < adjacency.size(); ++predecessor) {
        for (const auto& edge : adjacency[predecessor]) {
            edges.push_back({edge.neighbor, predecessor, edge.num_connections, edge.min_delay});
        }
    }
    std::sort(edges.begin(), edges.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.dependent != rhs.dependent) return lhs.dependent < rhs.dependent;
        if (lhs.predecessor != rhs.predecessor) return lhs.predecessor < rhs.predecessor;
        return lhs.min_delay < rhs.min_delay;
    });
    return edges;
}

std::vector<reduction::Edge> buildConstraintGraph(const char* prune_value,
                                                  bool verify_rebuild = false) {
    ScopedEnvironmentVariable prune("CHRONON_EXPERIMENTAL_TRANSITIVE_DEP_PRUNE", prune_value);

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_weighted_partitioning = false;
    config.enable_dynamic_rebalance = false;
    // Exceed the default lock-free ring headroom so this test continues to
    // exercise pruning of necessary reverse capacity constraints.
    config.max_lookahead_cycles = 5000;
    TickSimulation sim(config);
    auto* a = sim.createUnit<PassUnit>("a");
    auto* b = sim.createUnit<PassUnit>("b");
    auto* c = sim.createUnit<PassUnit>("c");

    sim.connect(a->out, b->in, 2);
    sim.connect(b->out, c->in, 3);
    sim.connect(a->out, c->in, 5);
    sim.initialize();

    const auto first = snapshotDependencies(sim);
    if (verify_rebuild) {
        TransitiveDependencyPruneTestAccess::rebuild(sim);
        CHECK(snapshotDependencies(sim) == first);
    }
    return first;
}

struct ZeroCycleSnapshot {
    std::vector<reduction::Edge> edges;
    bool has_zero_delay_cycle = false;
};

ZeroCycleSnapshot buildHeadroomZeroCycleGraph(const char* prune_value) {
    ScopedEnvironmentVariable prune("CHRONON_EXPERIMENTAL_TRANSITIVE_DEP_PRUNE", prune_value);

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_weighted_partitioning = false;
    config.enable_dynamic_rebalance = false;
    TickSimulation sim(config);
    auto* a = sim.createUnit<PassUnit>("a");
    auto* b = sim.createUnit<PassUnit>("b");

    auto* ab = sim.connect(a->out, b->in, 1);
    auto* ba = sim.connect(b->out, a->in, 1);
    ab->configureRegisteredEdge(/*capacity=*/1, /*rate=*/1);
    ba->configureRegisteredEdge(/*capacity=*/1, /*rate=*/1);
    sim.initialize();

    // Force bounded cross-thread transport. Each edge then contributes a reverse
    // headroom constraint with delay zero, creating a pair-min zero cycle.
    ab->optimizeForMPSC();
    ba->optimizeForMPSC();
    ab->setThreadQueueId(ab->registerProducerThread(/*thread_id=*/1));
    ba->setThreadQueueId(ba->registerProducerThread(/*thread_id=*/2));
    TransitiveDependencyPruneTestAccess::rebuild(sim);

    return {snapshotDependencies(sim), TransitiveDependencyPruneTestAccess::hasZeroDelayCycle(sim)};
}

std::pair<std::vector<reduction::Edge>, std::vector<reduction::Edge>>
buildDependencyOnlyHeadroomGraph(size_t headroom, uint32_t max_lookahead) {
    ScopedEnvironmentVariable prune("CHRONON_EXPERIMENTAL_TRANSITIVE_DEP_PRUNE", "0");

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_weighted_partitioning = false;
    config.enable_dynamic_rebalance = true;
    config.max_lookahead_cycles = max_lookahead;
    TickSimulation sim(config);
    auto* producer = sim.createUnit<PassUnit>("producer");
    auto* consumer = sim.createUnit<PassUnit>("consumer");
    sim.connect(producer->out, consumer->in, 1);
    producer->out.setDependencyOnlyTransport(true, headroom);
    sim.initialize();
    return {snapshotDependencies(sim), snapshotRebalanceAdjacency(sim)};
}

struct PhysicalRebalanceGraph {
    std::vector<reduction::Edge> execution;
    std::vector<RebalanceEdgeSnapshot> rebalance;
};

PhysicalRebalanceGraph buildPhysicalRebalanceGraph(const char* prune_value) {
    ScopedEnvironmentVariable prune("CHRONON_EXPERIMENTAL_TRANSITIVE_DEP_PRUNE", prune_value);

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_weighted_partitioning = false;
    config.enable_dynamic_rebalance = true;
    config.max_lookahead_cycles = 32;
    TickSimulation sim(config);
    auto* a = sim.createUnit<PassUnit>("physical_a");
    auto* b = sim.createUnit<PassUnit>("physical_b");
    auto* c = sim.createUnit<PassUnit>("physical_c");

    sim.connect(a->out, b->in, 1);
    sim.connect(b->out, c->in, 1);
    sim.connect(a->out, c->in, 2);
    sim.connect(a->out, c->in, 2);
    sim.initialize();

    return {snapshotDependencies(sim), snapshotRebalancePhysicalEdges(sim)};
}

void test_runtime_prunes_only_transitively_implied_constraints() {
    std::cout << "Testing runtime transitive dependency pruning... ";

    const auto baseline = buildConstraintGraph("0");
    const auto pruned = buildConstraintGraph("1", true);
    const auto non_exact_zero = buildConstraintGraph("00");
    const auto default_pruned = buildConstraintGraph(nullptr);

    // The three physical connections also create two finite-headroom reverse
    // constraints after queue selection. The reducer handles both kinds.
    CHECK(baseline.size() == 5);
    CHECK(pruned.size() == 3);
    CHECK(non_exact_zero == pruned);
    CHECK(default_pruned == pruned);
    CHECK(reduction::reduce(3, baseline).retained == pruned);
    CHECK(reduction::closure(3, baseline) == reduction::closure(3, pruned));

    const auto pruned_closure = reduction::closure(3, pruned);
    for (const auto& original : baseline) {
        CHECK(pruned_closure[original.dependent][original.predecessor] <= original.delay);
    }

    std::cout << "PASSED\n";
}

void test_pruning_does_not_hide_headroom_zero_delay_cycle() {
    std::cout << "Testing pruning preserves zero-delay execution-mode gate... ";

    const auto baseline = buildHeadroomZeroCycleGraph("0");
    const auto pruned = buildHeadroomZeroCycleGraph("1");

    CHECK(baseline.has_zero_delay_cycle);
    CHECK(pruned.has_zero_delay_cycle);
    CHECK(reduction::closure(2, baseline.edges) == reduction::closure(2, pruned.edges));

    std::cout << "PASSED\n";
}

void test_dependency_only_headroom_adds_reverse_edge_only_when_tighter() {
    std::cout << "Testing dependency-only headroom constraint selection... ";

    const auto tighter = buildDependencyOnlyHeadroomGraph(/*headroom=*/8, /*max_lookahead=*/100);
    CHECK(tighter.first.size() == 2);
    CHECK(tighter.first[0].delay == 7 || tighter.first[1].delay == 7);
    CHECK(tighter.second.size() == 2);

    const auto global_floor_suffices =
        buildDependencyOnlyHeadroomGraph(/*headroom=*/512, /*max_lookahead=*/100);
    CHECK(global_floor_suffices.first.size() == 1);
    CHECK(global_floor_suffices.first[0].delay == 1);
    CHECK(global_floor_suffices.second.size() == 2);
    CHECK(global_floor_suffices.second[0].delay == 1 || global_floor_suffices.second[1].delay == 1);
    CHECK(global_floor_suffices.second[0].delay == 511 ||
          global_floor_suffices.second[1].delay == 511);

    std::cout << "PASSED\n";
}

void test_rebalance_preserves_physical_edges_and_multiplicity() {
    std::cout << "Testing rebalance preserves physical topology... ";

    const auto unpruned = buildPhysicalRebalanceGraph("0");
    const auto pruned = buildPhysicalRebalanceGraph("1");
    CHECK(unpruned.execution.size() == 3);
    CHECK(pruned.execution.size() == 2);
    CHECK(unpruned.rebalance == pruned.rebalance);

    const auto direct = std::find_if(pruned.rebalance.begin(), pruned.rebalance.end(),
                                     [](const RebalanceEdgeSnapshot& edge) {
                                         return edge.dependent == 2 && edge.predecessor == 0;
                                     });
    CHECK(direct != pruned.rebalance.end());
    CHECK(direct->num_connections == 2);
    CHECK(direct->min_delay == 2);

    std::cout << "PASSED\n";
}

void test_zero_lookahead_window_selects_sequential() {
    std::cout << "Testing zero lookahead window selects Sequential... ";

    auto run = [](uint32_t max_lookahead) {
        TickSimulationConfig config;
        config.num_threads = 2;
        config.enable_weighted_partitioning = false;
        config.enable_dynamic_rebalance = false;
        config.max_lookahead_cycles = max_lookahead;

        TickSimulation sim(config);
        std::vector<PassUnit*> units;
        for (size_t i = 0; i < 6; ++i) {
            units.push_back(sim.createUnit<PassUnit>("unit" + std::to_string(i)));
        }
        sim.initialize();
        const bool parallel = sim.useParallelExecution();
        sim.run(8);
        for (const auto* unit : units) CHECK(unit->localCycle() == 8);
        return std::pair{parallel, sim.epochFreeRunCount()};
    };

    const auto enabled = run(8);
    CHECK(enabled.first);
    CHECK(enabled.second == 1);

    const auto disabled = run(0);
    CHECK(!disabled.first);
    CHECK(disabled.second == 0);

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    test_runtime_prunes_only_transitively_implied_constraints();
    test_pruning_does_not_hide_headroom_zero_delay_cycle();
    test_dependency_only_headroom_adds_reverse_edge_only_when_tighter();
    test_rebalance_preserves_physical_edges_and_multiplicity();
    test_zero_lookahead_window_selects_sequential();
    std::cout << "All transitive dependency pruning tests passed!\n";
    return 0;
}
