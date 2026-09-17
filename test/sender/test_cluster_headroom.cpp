// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "EpochFreeDifferentialHarness.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;
using namespace chronon::sender::test;
using chronon::sender::EpochFreeDifferentialTestAccess;

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

class Node final : public TickableUnit {
public:
    InPort<uint64_t> in;
    OutPort<uint64_t> out;
    UnitEventLog log;
    uint64_t sent = 0;
    uint64_t received = 0;
    uint64_t blocked = 0;
    uint64_t ticks = 0;
    std::optional<size_t> initialize_capacity;

    Node(std::string name, uint32_t id, size_t capacity, size_t rate, size_t attempts,
         bool stall = false)
        : TickableUnit(std::move(name)),
          in(this, "in", capacity),
          out(this, "out", rate),
          log(id),
          id_(id),
          attempts_(attempts),
          stall_(stall) {
        log.reserve(4096);
    }

    void initialize() override {
        if (initialize_capacity) in.setCapacity(*initialize_capacity);
    }

    void tick() override {
        const uint64_t cycle = localCycle();
        if (!stall_ || cycle % 7 >= 3) {
            while (auto value = in.tryReceive(cycle)) {
                log.record(cycle, ModelEventKind::Receive, *value, received++);
            }
        }
        for (size_t i = 0; i < attempts_; ++i) {
            const uint64_t value = (uint64_t{id_} << 48) | sent;
            const bool ready = out.canSend();
            const bool accepted = out.send(value);
            require(ready == accepted, "canSend/send disagree");
            log.record(cycle, ModelEventKind::SendResult, value, accepted);
            if (accepted)
                ++sent;
            else
                ++blocked;
        }
        log.record(cycle, ModelEventKind::State, sent, received);
        ++ticks;
    }

private:
    uint32_t id_;
    size_t attempts_;
    bool stall_;
};

class Independent final : public TickableUnit {
public:
    explicit Independent(std::string name) : TickableUnit(std::move(name)) {}
    void tick() override { ++ticks; }
    uint64_t ticks = 0;
};

enum class Placement { Weighted, SA, Topology, Precomputed };
enum class Shape { Pair, FanIn, Feedback, Transitive };

struct Case {
    size_t capacity;
    size_t rate;
    uint32_t delay;
    size_t attempts;
    bool local;
    Shape shape = Shape::Pair;
    bool registered_capacity = false;
    bool initialize_capacity = false;
};

std::vector<CanonicalEvent> run(const Case& spec, size_t workers, Placement placement, bool dynamic,
                                bool segmented, bool migrate) {
    TickSimulationConfig config;
    config.num_threads = workers;
    config.enable_parallel = workers > 1;
    config.enable_weighted_partitioning = placement != Placement::Topology;
    config.partition_solver = placement == Placement::SA
                                  ? TickSimulationConfig::PartitionSolverType::SA
                                  : TickSimulationConfig::PartitionSolverType::Weighted;
    config.initial_partition_sync_cost_ns = 0;
    config.max_lookahead_cycles = 16;
    config.enable_dynamic_rebalance = dynamic;
    // Use the real dynamic runtime with deterministic, quiescent migrations.
    config.rebalance_check_interval_cycles = UINT64_MAX;
    config.rebalance_imbalance_threshold = std::numeric_limits<double>::max();
    config.rebalance_min_gain = std::numeric_limits<double>::max();
    TickSimulation sim(config);
    auto* source = sim.createUnit<Node>("source", 0, spec.capacity, spec.rate, spec.attempts,
                                        spec.shape == Shape::Feedback);
    const size_t initial_capacity = spec.registered_capacity || spec.initialize_capacity
                                        ? InPort<uint64_t>::UNLIMITED_CAPACITY
                                        : spec.capacity;
    auto* target = sim.createUnit<Node>("target", 1, initial_capacity, spec.rate,
                                        spec.shape == Shape::Feedback ? spec.attempts : 0, true);
    auto* connection = sim.connect(source->out, target->in, spec.delay);
    if (spec.registered_capacity) connection->configureRegisteredEdge(spec.capacity, std::nullopt);
    if (spec.initialize_capacity) target->initialize_capacity = spec.capacity;
    std::vector<Node*> nodes{source, target};
    Node* extra = nullptr;
    if (spec.shape == Shape::FanIn) {
        extra = sim.createUnit<Node>("external", 2, 4, 1, 1);
        sim.connect(extra->out, target->in, 1);
    } else if (spec.shape == Shape::Feedback) {
        sim.connect(target->out, source->in, spec.delay);
    } else if (spec.shape == Shape::Transitive) {
        extra = sim.createUnit<Node>("upstream", 2, 1, 4, 1);
        sim.connect(extra->out, source->in, 0);
    }
    if (extra) nodes.push_back(extra);
    std::vector<Independent*> independent;
    // Keep the topology-only beneficial-work heuristic balanced at 2/4/8 workers.
    for (size_t i = nodes.size(); i < 32; ++i) {
        independent.push_back(sim.createUnit<Independent>("independent" + std::to_string(i)));
    }
    if (placement == Placement::Precomputed)
        sim.setPrecomputedUnitCosts(std::vector<double>(32, 1.0), {});
    sim.initialize();
    if (workers > 1) {
        require(sim.useParallelExecution(), "global fallback: " + sim.parallelFallbackReason());
        const size_t source_cluster = EpochFreeDifferentialTestAccess::clusterOf(sim, source);
        const size_t target_cluster = EpochFreeDifferentialTestAccess::clusterOf(sim, target);
        require((source_cluster == target_cluster) == spec.local, "unexpected endpoint clusters");
        if (extra) {
            require((EpochFreeDifferentialTestAccess::clusterOf(sim, extra) == source_cluster) ==
                        (spec.shape == Shape::Transitive),
                    "incorrect fan-in/transitive clustering");
        }
        require(
            EpochFreeDifferentialTestAccess::clusterOf(sim, independent.front()) != source_cluster,
            "independent work was absorbed into the constrained cluster");
    }
    require(connection->delay() == spec.delay, "clustering changed modeled delay");
    require(target->in.configuredCapacity() == spec.capacity, "clustering inflated capacity");

    constexpr uint64_t cycles = 100;
    if (segmented) {
        require(sim.run(1) == 1 && sim.run(16) == 16, "segmented run stopped early");
        if (migrate) {
            require(target->in.queuedMessageCount() + target->in.transportPendingMessageCount() > 0,
                    "migration did not exercise pending transport state");
            const size_t owner = sim.assignedThread(source);
            const size_t next = (owner + 1) % workers;
            require(EpochFreeDifferentialTestAccess::migrateAtRunBoundary(sim, source, next),
                    "cluster migration rejected");
            require(sim.assignedThread(source) == next, "source did not migrate");
            if (spec.local)
                require(sim.assignedThread(target) == next, "cluster split on migration");
            if (spec.shape == Shape::Transitive)
                require(sim.assignedThread(extra) == next, "transitive member did not migrate");
        }
        require(sim.run(1) == 1 && sim.run(82) == 82, "segmented run stopped early");
    } else {
        require(sim.run(cycles) == cycles, "run stopped early");
    }
    require(sim.epochFreeRunCount() == (workers == 1 ? 0u
                                        : segmented  ? 4u
                                                     : 1u),
            "epoch-free execution was not used");
    require(source->blocked > 0, "test did not exercise backpressure");
    for (auto* unit : independent)
        require(unit->ticks == cycles, "independent work did not finish");
    std::vector<UnitEventLog*> logs;
    for (auto* node : nodes) logs.push_back(&node->log);
    return canonicalizeEvents(logs);
}

void checkCase(const Case& spec) {
    const auto reference = run(spec, 1, Placement::Weighted, false, false, false);
    require(run(spec, 1, Placement::Weighted, false, true, false) == reference,
            "segmented sequential trace differs");
    if (spec.shape == Shape::Pair && spec.capacity == 1 && spec.rate == 4 && spec.delay == 1) {
        const auto sentAt = [&](uint64_t cycle) {
            for (const auto& event : reference) {
                if (event.component == 0 && event.cycle == cycle &&
                    event.kind == ModelEventKind::SendResult)
                    return event.value1;
            }
            throw std::runtime_error("missing admission event");
        };
        require(sentAt(3) == 0 && sentAt(4) == 1,
                "ordinary FIFO did not return cycle-3 drain credit at cycle 4");
    }
    for (auto placement :
         {Placement::Weighted, Placement::SA, Placement::Topology, Placement::Precomputed}) {
        for (size_t workers : {2, 4, 8}) {
            for (bool dynamic : {false, true}) {
                require(run(spec, workers, placement, dynamic, false, false) == reference,
                        "parallel cycle-visible trace differs");
                require(run(spec, workers, placement, dynamic, true, dynamic) == reference,
                        "segmented/migrated cycle-visible trace differs");
            }
        }
    }
}

void checkLocalUnboundedBurst() {
    // Even inside an atomic cluster, a dynamic SPSC ring must hold a whole
    // producer tick before the same-cycle consumer runs. Exercise growth past
    // the default 4096 slots without introducing architectural backpressure.
    for (bool dynamic : {false, true}) {
        TickSimulationConfig config;
        config.num_threads = 4;
        config.enable_dynamic_rebalance = dynamic;
        config.initial_partition_sync_cost_ns = 0;
        TickSimulation sim(config);
        auto* source = sim.createUnit<Node>("burst", 0, SIZE_MAX, 5000, 5000);
        auto* target = sim.createUnit<Node>("drain", 1, SIZE_MAX, 1, 0);
        sim.connect(source->out, target->in, 0);
        for (size_t i = 0; i < 8; ++i)
            sim.createUnit<Independent>("independent" + std::to_string(i));
        sim.initialize();
        require(sim.useParallelExecution(), "local burst disabled parallel execution");
        require(sim.run(3) == 3, "local burst stopped early");
        require(source->sent == 15000 && target->received == 15000 && source->blocked == 0,
                "cluster-local transport lost physical burst headroom");
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const std::string mode = argc > 1 ? argv[1] : "pair";
        if (mode == "pair") {
            checkLocalUnboundedBurst();
            for (const auto& spec : std::array{Case{1, 1, 1, 1, false}, Case{1, 4, 1, 1, true},
                                               Case{4, 4, 1, 4, true}, Case{1, 4, 0, 4, true},
                                               Case{8, 4, 1, 4, false}, Case{1, 1, 2, 1, true}}) {
                std::cout << "capacity=" << spec.capacity << " rate=" << spec.rate
                          << " delay=" << spec.delay << std::endl;
                checkCase(spec);
            }
        } else if (mode == "fanin") {
            checkCase({4, 4, 1, 4, true, Shape::FanIn});
            checkCase({4, 4, 1, 4, true, Shape::FanIn, true});
            checkCase({4, 4, 1, 4, true, Shape::FanIn, false, true});
        } else if (mode == "feedback") {
            checkCase({1, 1, 1, 1, true, Shape::Feedback});
            checkCase({4, 4, 1, 4, true, Shape::Feedback});
        } else if (mode == "transitive") {
            checkCase({1, 4, 1, 4, true, Shape::Transitive});
        } else {
            throw std::invalid_argument("unknown test mode");
        }
        std::cout << "Cluster headroom " << mode << ": PASSED\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
