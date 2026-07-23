// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

// test_rebalance_calibration.cpp
//
// Verifies that epoch-free dynamic rebalance fires and produces meaningful
// unit costs. Topology: 5 units with deliberately imbalanced tick costs;
// rebalance_check_interval_cycles is low enough to migrate within the budget.

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "sender/core/DynamicWaitPolicy.hpp"
#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/Port.hpp"
#include "sender/schedule/EpochFreeTopologyCost.hpp"

using namespace chronon::sender;

namespace chronon::sender {

struct DynamicMigrationTestAccess {
    struct UnitSamplingStats {
        uint64_t active_samples = 0;
        uint64_t active_total_ns = 0;
        uint64_t inactive_samples = 0;
        uint64_t inactive_total_ns = 0;
        uint64_t active_ticks = 0;
        uint64_t observed_cycles = 0;
        double normalized_cost = 0.0;
        bool ready = false;
    };

    static UnitSamplingStats unitSamplingStats(const TickSimulation& sim, const Unit* unit) {
        const auto it = std::find(sim.unit_ptrs_.begin(), sim.unit_ptrs_.end(), unit);
        assert(it != sim.unit_ptrs_.end());
        const size_t index = static_cast<size_t>(it - sim.unit_ptrs_.begin());
        const auto estimate = sim.dynamicUnitRuntimeCost_(index, 1.0);
        return {
            sim.dynamic_unit_active_sample_count_[index].load(std::memory_order_relaxed),
            sim.dynamic_unit_active_sample_time_ns_[index].load(std::memory_order_relaxed),
            sim.dynamic_unit_inactive_sample_count_[index].load(std::memory_order_relaxed),
            sim.dynamic_unit_inactive_sample_time_ns_[index].load(std::memory_order_relaxed),
            sim.dynamic_unit_observed_active_ticks_[index].load(std::memory_order_relaxed),
            sim.dynamic_unit_observed_cycles_[index].load(std::memory_order_relaxed),
            estimate.cost,
            estimate.ready,
        };
    }

    static bool sameCluster(const TickSimulation& sim, const Unit* lhs, const Unit* rhs) {
        const auto lhs_it = std::find(sim.unit_ptrs_.begin(), sim.unit_ptrs_.end(), lhs);
        const auto rhs_it = std::find(sim.unit_ptrs_.begin(), sim.unit_ptrs_.end(), rhs);
        if (lhs_it == sim.unit_ptrs_.end() || rhs_it == sim.unit_ptrs_.end()) return false;
        const size_t lhs_index = static_cast<size_t>(lhs_it - sim.unit_ptrs_.begin());
        const size_t rhs_index = static_cast<size_t>(rhs_it - sim.unit_ptrs_.begin());
        return lhs_index < sim.unit_to_cluster_.size() && rhs_index < sim.unit_to_cluster_.size() &&
               sim.unit_to_cluster_[lhs_index] == sim.unit_to_cluster_[rhs_index];
    }

    static bool sourceOnlyCommit(TickSimulation& sim) {
        sim.initDynamicMigrationRuntime_();
        if (!sim.cluster_runtime_owner_ || sim.dynamic_runtime_cluster_count_ == 0 ||
            sim.dynamic_runtime_thread_count_ < 2) {
            return false;
        }

        constexpr uint64_t fence = 11;
        const size_t cluster = 0;
        const size_t source = sim.cluster_runtime_owner_[cluster].load(std::memory_order_relaxed);
        const size_t target = (source + 1) % sim.dynamic_runtime_thread_count_;
        const uint64_t generation =
            sim.cluster_assignment_generation_.load(std::memory_order_relaxed);

        sim.thread_progress_array_[cluster].completed_cycle.store(fence - 1,
                                                                  std::memory_order_relaxed);
        sim.migration_request_.cluster.store(cluster, std::memory_order_relaxed);
        sim.migration_request_.source_thread.store(source, std::memory_order_relaxed);
        sim.migration_request_.target_thread.store(target, std::memory_order_relaxed);
        sim.migration_request_.fence_cycle.store(fence, std::memory_order_relaxed);
        sim.cluster_migration_pending_[cluster].store(1, std::memory_order_relaxed);
        sim.migration_request_.state.store(
            static_cast<uint8_t>(TickSimulation::MigrationRequestState::Quiescing),
            std::memory_order_release);

        sim.serviceEpochFreeMigration_(target);
        const bool target_rejected =
            sim.cluster_runtime_owner_[cluster].load(std::memory_order_relaxed) == source &&
            sim.cluster_assignment_generation_.load(std::memory_order_relaxed) == generation;

        sim.serviceEpochFreeMigration_(source);
        const bool fence_enforced =
            sim.cluster_runtime_owner_[cluster].load(std::memory_order_relaxed) == source;

        sim.thread_progress_array_[cluster].completed_cycle.store(fence, std::memory_order_release);
        sim.serviceEpochFreeMigration_(source);
        const bool source_committed =
            sim.cluster_runtime_owner_[cluster].load(std::memory_order_acquire) == target &&
            sim.cluster_assignment_generation_.load(std::memory_order_acquire) == generation + 1 &&
            sim.cluster_migration_pending_[cluster].load(std::memory_order_acquire) == 0 &&
            sim.migration_request_.state.load(std::memory_order_acquire) ==
                static_cast<uint8_t>(TickSimulation::MigrationRequestState::None);
        return target_rejected && fence_enforced && source_committed;
    }
};

}  // namespace chronon::sender

namespace {

bool nearlyEqual(double lhs, double rhs) {
    return std::abs(lhs - rhs) <= std::max(1e-9, std::abs(rhs) * 1e-12);
}

int run_dynamic_wait_policy_test() {
    using chronon::sender::detail::dynamicWaitThreadYieldSpinMask;
    using chronon::sender::detail::shouldYieldDynamicWaitThread;

    const uint64_t floor_mask = dynamicWaitThreadYieldSpinMask(true);
    const uint64_t dependency_mask = dynamicWaitThreadYieldSpinMask(false);
    if (floor_mask != 255 || dependency_mask != 4095) {
        std::cerr << "FAIL: dynamic wait policy selected the wrong yield cadence\n";
        return 1;
    }
    if (shouldYieldDynamicWaitThread(254, floor_mask) ||
        !shouldYieldDynamicWaitThread(255, floor_mask) ||
        shouldYieldDynamicWaitThread(256, floor_mask) ||
        !shouldYieldDynamicWaitThread(511, floor_mask)) {
        std::cerr << "FAIL: lookahead-floor waits must yield every 256 spins\n";
        return 1;
    }
    if (shouldYieldDynamicWaitThread(255, dependency_mask) ||
        shouldYieldDynamicWaitThread(4094, dependency_mask) ||
        !shouldYieldDynamicWaitThread(4095, dependency_mask) ||
        !shouldYieldDynamicWaitThread(8191, dependency_mask)) {
        std::cerr << "FAIL: dependency waits must yield every 4096 spins\n";
        return 1;
    }
    return 0;
}

int run_runtime_planner_input_test() {
    using chronon::sender::epoch_free_cost::RuntimeDependency;

    if (chronon::sender::detail::shouldSampleDynamicTick(
            254, chronon::sender::detail::kNoDynamicTickSample) ||
        !chronon::sender::detail::shouldSampleDynamicTick(
            255, chronon::sender::detail::kNoDynamicTickSample) ||
        chronon::sender::detail::shouldSampleDynamicTick(510, 255) ||
        !chronon::sender::detail::shouldSampleDynamicTick(511, 255)) {
        std::cerr << "FAIL: dynamic tick sampling must warm up and separate accepted samples\n";
        return 1;
    }
    const uint64_t first_window =
        chronon::sender::TickSimulationConfig{}.rebalance_check_interval_cycles;
    size_t warm_samples = 0;
    uint64_t last_sample = chronon::sender::detail::kNoDynamicTickSample;
    for (uint64_t cycle = 0; cycle < first_window; ++cycle) {
        if (chronon::sender::detail::shouldSampleDynamicTick(cycle, last_sample)) {
            ++warm_samples;
            last_sample = cycle;
        }
    }
    if (warm_samples != first_window / chronon::sender::detail::kDynamicTickSampleInterval) {
        std::cerr << "FAIL: default rebalance window must contain only complete warm samples\n";
        return 1;
    }

    if (chronon::sender::epoch_free_cost::runtimeSyncCostNs(0.0) != 1.0 ||
        chronon::sender::epoch_free_cost::runtimeSyncCostNs(3.5) != 3.5) {
        std::cerr << "FAIL: runtime sync cost must use measured platform cost only\n";
        return 1;
    }
    if (!chronon::sender::epoch_free_cost::isUnhiddenDependencyWait(42, 42) ||
        chronon::sender::epoch_free_cost::isUnhiddenDependencyWait(43, 42)) {
        std::cerr << "FAIL: only global-frontier dependency waits are unhidden\n";
        return 1;
    }

    const std::array dependencies = {
        RuntimeDependency{1, 0, 4}, RuntimeDependency{1, 0, 2}, RuntimeDependency{1, 0, 3, 3},
        RuntimeDependency{2, 1, 1}, RuntimeDependency{2, 2, 0}, RuntimeDependency{9, 0, 1},
    };
    const auto adjacency = chronon::sender::epoch_free_cost::buildRuntimeAdjacency(3, dependencies);
    if (adjacency.size() != 3 || adjacency[0].size() != 1 || adjacency[0][0].neighbor != 1 ||
        adjacency[0][0].num_connections != 5 || adjacency[0][0].min_delay != 2 ||
        adjacency[1].size() != 1 || adjacency[1][0].neighbor != 2 ||
        adjacency[1][0].min_delay != 1 || !adjacency[2].empty()) {
        std::cerr << "FAIL: runtime adjacency must deduplicate scheduling relationships\n";
        return 1;
    }

    PartitionInput move_input;
    move_input.num_units = 3;
    move_input.num_threads = 2;
    move_input.unit_cost_ns = {1000.0, 400.0, 1.0};
    move_input.sync_cost_ns = 1.0;
    move_input.adjacency.resize(3);
    move_input.adjacency[0].push_back({1, 1, 0});
    const std::vector<size_t> assignment{0, 0, 1};
    if (!chronon::sender::epoch_free_cost::moveWouldSplitZeroDelay(move_input, assignment, 1, 1)) {
        std::cerr << "FAIL: fixture must expose a zero-delay runtime coupling\n";
        return 1;
    }
    const chronon::sender::epoch_free_cost::RuntimeWaits no_waits;
    const auto scored = chronon::sender::epoch_free_cost::scoreMove(
        move_input, assignment, 1, 1, no_waits, /*min_gain_fraction=*/0.0,
        /*churn_penalty=*/0.0);
    if (!scored.valid || scored.active_gain <= 0.0) {
        std::cerr << "FAIL: runtime scoring rejected a beneficial move across a zero-delay "
                     "headroom constraint\n";
        return 1;
    }

    return 0;
}

class HeavyUnit : public TickableUnit {
public:
    explicit HeavyUnit(const std::string& name, int iterations = 5000)
        : TickableUnit(name), iterations_(iterations) {}
    OutPort<uint64_t> out{this, "out", 1};

    void tick() override {
        uint64_t v = localCycle();
        for (int i = 0; i < iterations_; ++i) {
            v = v * 6364136223846793005ULL + 1442695040888963407ULL;
        }
        sink_ += v;
        (void)out.send(v);
    }

    uint64_t sink_ = 0;

private:
    int iterations_ = 5000;
};

class LightUnit : public TickableUnit {
public:
    explicit LightUnit(const std::string& name) : TickableUnit(name) {}
    InPort<uint64_t> in{this, "in"};
    OutPort<uint64_t> out{this, "out"};

    void tick() override {
        if (auto val = in.tryReceive(localCycle())) {
            (void)out.send(*val);
        }
    }
};

class Sink : public TickableUnit {
public:
    Sink() : TickableUnit("sink") {}
    InPort<uint64_t> in{this, "in"};

    void tick() override {
        while (in.tryReceive(localCycle())) {
        }
    }
};

class TightProducer : public TickableUnit {
public:
    TightProducer() : TickableUnit("tight_producer") {}
    OutPort<uint64_t> out{this, "out", 1};

    void tick() override { (void)out.send(localCycle()); }
};

class TightConsumer : public TickableUnit {
public:
    TightConsumer() : TickableUnit("tight_consumer") {}
    InPort<uint64_t> in{this, "in"};

    void tick() override {
        if (auto v = in.tryReceive(localCycle())) {
            checksum_ ^= *v + localCycle();
        }
    }

    uint64_t checksum() const { return checksum_; }

private:
    uint64_t checksum_ = 0;
};

class SleepingProducer : public TickableUnit {
public:
    SleepingProducer() : TickableUnit("sleeping_producer") {}
    OutPort<uint64_t> out{this, "out", 1};

    void tick() override {
        uint64_t value = localCycle();
        for (size_t i = 0; i < 5000; ++i) {
            value = value * 6364136223846793005ULL + 1442695040888963407ULL;
        }
        checksum_ ^= value;
        (void)out.send(value);
        sleepUntil(localCycle() + 1024);
    }

private:
    uint64_t checksum_ = 0;
};

TickSimulationConfig samplingTestConfig() {
    TickSimulationConfig cfg;
    cfg.num_threads = 3;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.enable_epoch_free_lookahead = true;
    cfg.enable_dynamic_rebalance = true;
    cfg.rebalance_check_interval_cycles = 8192;
    cfg.initial_partition_sync_cost_ns = 0.0;
    return cfg;
}

int run_periodic_sampling_detection_test() {
    TickSimulation sim(samplingTestConfig());
    auto* periodic = sim.createUnit<HeavyUnit>("periodic", 5000);
    auto* ordinary0 = sim.createUnit<HeavyUnit>("ordinary0", 1);
    auto* ordinary1 = sim.createUnit<HeavyUnit>("ordinary1", 1);
    auto* ordinary2 = sim.createUnit<HeavyUnit>("ordinary2", 1);
    auto* sink = sim.createUnit<Sink>();
    periodic->setTickInterval(256);
    sim.connect(periodic->out, sink->in, 0);
    sim.connect(ordinary0->out, sink->in, 1);
    sim.connect(ordinary1->out, sink->in, 1);
    sim.connect(ordinary2->out, sink->in, 1);
    sim.initialize();
    sim.run(1025);

    const auto periodic_stats = DynamicMigrationTestAccess::unitSamplingStats(sim, periodic);
    const auto sink_stats = DynamicMigrationTestAccess::unitSamplingStats(sim, sink);
    const double periodic_active_mean =
        periodic_stats.active_samples == 0 ? 0.0
                                           : static_cast<double>(periodic_stats.active_total_ns) /
                                                 static_cast<double>(periodic_stats.active_samples);
    const double periodic_inactive_mean =
        periodic_stats.inactive_samples == 0
            ? 0.0
            : static_cast<double>(periodic_stats.inactive_total_ns) /
                  static_cast<double>(periodic_stats.inactive_samples);
    const double periodic_expected_cost =
        periodic_active_mean / 256.0 + periodic_inactive_mean * 255.0 / 256.0;
    if (!DynamicMigrationTestAccess::sameCluster(sim, periodic, sink) ||
        periodic_stats.active_samples != 4 || periodic_stats.inactive_samples < 4 ||
        periodic_stats.active_ticks != 4 || periodic_stats.observed_cycles != 1024 ||
        !periodic_stats.ready || periodic_active_mean <= 0.0 ||
        !nearlyEqual(periodic_stats.normalized_cost, periodic_expected_cost) || !sink_stats.ready ||
        sink_stats.active_ticks != sink_stats.observed_cycles) {
        std::cerr << "FAIL: periodic sampling did not preserve its 1/256 activation rate "
                     "inside a mixed cluster"
                  << " (same_cluster="
                  << DynamicMigrationTestAccess::sameCluster(sim, periodic, sink)
                  << ", active_samples=" << periodic_stats.active_samples
                  << ", inactive_samples=" << periodic_stats.inactive_samples
                  << ", active_ticks=" << periodic_stats.active_ticks
                  << ", cycles=" << periodic_stats.observed_cycles
                  << ", normalized=" << periodic_stats.normalized_cost
                  << ", active_mean=" << periodic_active_mean << ", sink_ready=" << sink_stats.ready
                  << ", sink_active_ticks=" << sink_stats.active_ticks
                  << ", sink_cycles=" << sink_stats.observed_cycles << ")\n";
        return 1;
    }
    return 0;
}

int run_multi_periodic_cluster_sampling_test() {
    TickSimulation sim(samplingTestConfig());
    auto* periodic2 = sim.createUnit<LightUnit>("periodic2");
    auto* periodic3 = sim.createUnit<LightUnit>("periodic3");
    sim.createUnit<HeavyUnit>("multi_ordinary0", 1);
    sim.createUnit<HeavyUnit>("multi_ordinary1", 1);
    sim.createUnit<HeavyUnit>("multi_ordinary2", 1);
    periodic2->setTickInterval(2);
    periodic3->setTickInterval(3);
    sim.connect(periodic2->out, periodic3->in, 0);
    sim.initialize();
    sim.run(1033);

    const auto periodic2_stats = DynamicMigrationTestAccess::unitSamplingStats(sim, periodic2);
    const auto periodic3_stats = DynamicMigrationTestAccess::unitSamplingStats(sim, periodic3);
    const bool interval2_rate = periodic2_stats.observed_cycles != 0 &&
                                periodic2_stats.active_ticks * 2 == periodic2_stats.observed_cycles;
    const bool interval3_rate =
        periodic3_stats.observed_cycles != 0 &&
        periodic3_stats.active_ticks * 3 >= periodic3_stats.observed_cycles &&
        periodic3_stats.active_ticks * 3 - periodic3_stats.observed_cycles <= 2;
    if (!DynamicMigrationTestAccess::sameCluster(sim, periodic2, periodic3) ||
        periodic2_stats.active_samples != 4 || periodic3_stats.active_samples != 4 ||
        !periodic2_stats.ready || !periodic3_stats.ready || !interval2_rate || !interval3_rate) {
        std::cerr << "FAIL: mixed periodic cluster did not retain each member's activation rate"
                  << " (same_cluster="
                  << DynamicMigrationTestAccess::sameCluster(sim, periodic2, periodic3)
                  << ", p2_samples=" << periodic2_stats.active_samples
                  << ", p2_active=" << periodic2_stats.active_ticks
                  << ", p2_cycles=" << periodic2_stats.observed_cycles
                  << ", p2_ready=" << periodic2_stats.ready
                  << ", p3_samples=" << periodic3_stats.active_samples
                  << ", p3_active=" << periodic3_stats.active_ticks
                  << ", p3_cycles=" << periodic3_stats.observed_cycles
                  << ", p3_ready=" << periodic3_stats.ready << ")\n";
        return 1;
    }
    return 0;
}

int run_activity_scheduled_cluster_sampling_test() {
    TickSimulation sim(samplingTestConfig());
    auto* sleeping = sim.createUnit<SleepingProducer>();
    auto* ordinary = sim.createUnit<LightUnit>("sleeping_consumer");
    sim.createUnit<HeavyUnit>("sleep_ordinary0", 1);
    sim.createUnit<HeavyUnit>("sleep_ordinary1", 1);
    sim.createUnit<HeavyUnit>("sleep_ordinary2", 1);
    sim.connect(sleeping->out, ordinary->in, 0);
    sim.initialize();
    sim.run(4097);

    const auto sleeping_stats = DynamicMigrationTestAccess::unitSamplingStats(sim, sleeping);
    const auto ordinary_stats = DynamicMigrationTestAccess::unitSamplingStats(sim, ordinary);
    const double sleeping_active_mean =
        sleeping_stats.active_samples == 0 ? 0.0
                                           : static_cast<double>(sleeping_stats.active_total_ns) /
                                                 static_cast<double>(sleeping_stats.active_samples);
    const double sleeping_inactive_mean =
        sleeping_stats.inactive_samples == 0
            ? 0.0
            : static_cast<double>(sleeping_stats.inactive_total_ns) /
                  static_cast<double>(sleeping_stats.inactive_samples);
    const double sleeping_expected_cost =
        sleeping_active_mean / 1024.0 + sleeping_inactive_mean * 1023.0 / 1024.0;
    if (!DynamicMigrationTestAccess::sameCluster(sim, sleeping, ordinary) ||
        sleeping_stats.active_samples != 4 || sleeping_stats.active_ticks != 4 ||
        sleeping_stats.observed_cycles != 4096 || !sleeping_stats.ready ||
        sleeping_active_mean <= 0.0 ||
        !nearlyEqual(sleeping_stats.normalized_cost, sleeping_expected_cost) ||
        !ordinary_stats.ready || ordinary_stats.active_ticks != ordinary_stats.observed_cycles) {
        std::cerr << "FAIL: activity-scheduled sampling did not preserve wakeup rate "
                     "inside a mixed cluster"
                  << " (same_cluster="
                  << DynamicMigrationTestAccess::sameCluster(sim, sleeping, ordinary)
                  << ", active_samples=" << sleeping_stats.active_samples
                  << ", inactive_samples=" << sleeping_stats.inactive_samples
                  << ", active_ticks=" << sleeping_stats.active_ticks
                  << ", cycles=" << sleeping_stats.observed_cycles
                  << ", ready=" << sleeping_stats.ready
                  << ", normalized=" << sleeping_stats.normalized_cost
                  << ", expected=" << sleeping_expected_cost
                  << ", ordinary_ready=" << ordinary_stats.ready
                  << ", ordinary_active=" << ordinary_stats.active_ticks
                  << ", ordinary_cycles=" << ordinary_stats.observed_cycles << ")\n";
        return 1;
    }
    return 0;
}

#ifdef CHRONON_SANITIZER_BUILD
constexpr int kPrimaryHeavy0Iterations = 6000;
constexpr int kPrimaryHeavy3Iterations = 4000;
constexpr int kGuardHeavy0Iterations = 6000;
constexpr int kGuardHeavy2Iterations = 4000;
constexpr uint64_t kRunUntilCycles = 8192;
constexpr uint64_t kLongRunCycles = 8192;
constexpr uint64_t kChunkedCooldownCycles = 4096;
constexpr uint64_t kChunkBoundaryCycles = 1024;
constexpr uint64_t kGuardCycles = 2048;
#else
constexpr int kPrimaryHeavy0Iterations = 12000;
constexpr int kPrimaryHeavy3Iterations = 8000;
constexpr int kGuardHeavy0Iterations = 12000;
constexpr int kGuardHeavy2Iterations = 8000;
constexpr uint64_t kRunUntilCycles = 16384;
constexpr uint64_t kLongRunCycles = 65536;
constexpr uint64_t kChunkedCooldownCycles = 8192;
constexpr uint64_t kChunkBoundaryCycles = 2048;
constexpr uint64_t kGuardCycles = 4096;
#endif

}  // namespace

int run_rebalance_calibration(bool use_run_until_termination, uint64_t chunk_cycles = 0,
                              uint64_t check_interval = 64, uint64_t total_cycles = 16384) {
    std::cout << (use_run_until_termination ? "Testing runUntilTermination rebalance"
                                            : "Testing run() rebalance")
              << " (epoch-free)... ";

    TickSimulationConfig cfg;
    cfg.num_threads = 3;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.enable_epoch_free_lookahead = true;
    cfg.epoch_size = 64;
    cfg.enable_dynamic_rebalance = true;
    cfg.rebalance_check_interval_cycles = check_interval;
    cfg.rebalance_imbalance_threshold = 1.05;
    cfg.rebalance_min_gain = 0.0;
    cfg.rebalance_cooldown_cycles = 0;
    // This test isolates runtime sampling and migration. Disable the initial
    // locality heuristic so the fan-in topology still starts in parallel mode.
    cfg.initial_partition_sync_cost_ns = 0.0;

    TickSimulation sim(cfg);

    // 5 units: 4 heavy + 1 sink. With 3 threads and uniform cost LPT,
    // one thread starts with fewer units than the others. Real tick costs are
    // still imbalanced (heavy >> sink), so sampled costs should replace the
    // initial 1.0 placeholders on the first successful rebalance.
    // Uniform initial costs place unit indices 0 and 3 on the same thread.
    // Make those two runtime-heavy so dynamic rebalance has a deterministic
    // migration that lowers max active cost instead of relying on timing noise
    // between otherwise identical units.
    auto* h0 = sim.createUnit<HeavyUnit>("heavy0", kPrimaryHeavy0Iterations);
    auto* h1 = sim.createUnit<HeavyUnit>("heavy1", 300);
    auto* h2 = sim.createUnit<HeavyUnit>("heavy2", 300);
    auto* h3 = sim.createUnit<HeavyUnit>("heavy3", kPrimaryHeavy3Iterations);
    auto* sink = sim.createUnit<Sink>();

    sim.connect(h0->out, sink->in, 1);
    sim.connect(h1->out, sink->in, 1);
    sim.connect(h2->out, sink->in, 1);
    sim.connect(h3->out, sink->in, 1);

    sim.initialize();

    if (use_run_until_termination) {
        sim.runUntilTermination(total_cycles);
    } else if (chunk_cycles > 0) {
        for (uint64_t done = 0; done < total_cycles; done += chunk_cycles) {
            sim.run(std::min<uint64_t>(chunk_cycles, total_cycles - done));
        }
    } else {
        sim.run(total_cycles);
    }

    const auto& post = sim.getPlatformMetrics();
    std::cout << "\nPost-run atomic_roundtrip_ns: " << post.atomic_roundtrip_ns << "\n";
    std::cout << "Rebalance count: " << sim.rebalanceCount() << "\n";
    std::cout << "Epoch-free runs: " << sim.epochFreeRunCount() << "\n";

    const auto& costs = sim.getUnitCosts();
    std::cout << "Unit costs:";
    for (size_t i = 0; i < costs.size(); ++i) {
        std::cout << " " << costs[i];
    }
    std::cout << "\n";

    if (sim.rebalanceCount() == 0) {
        std::cerr << "FAIL: no rebalance occurred (imbalance may not have been detected)\n";
        return 1;
    }

    if (sim.epochFreeRunCount() == 0) {
        std::cerr << "FAIL: dynamic rebalance did not use epoch-free lookahead\n";
        return 1;
    }

    if (costs.size() != 5) {
        std::cerr << "FAIL: expected 5 unit costs, got " << costs.size() << "\n";
        return 1;
    }

    bool any_measured_cost = false;
    for (double cost : costs) {
        if (cost <= 0.0) {
            std::cerr << "FAIL: non-positive unit cost after rebalance: " << cost << "\n";
            return 1;
        }
        if (cost != 1.0) {
            any_measured_cost = true;
        }
    }

    if (!any_measured_cost) {
        std::cerr << "FAIL: unit costs remained at initial uniform placeholder values\n";
        return 1;
    }

    std::cout << "\n=== Rebalance calibration: PASSED ===\n";
    return 0;
}

int run_tight_cluster_migration_guard() {
    std::cout << "Testing delay=0 cluster migration guard... ";

    TickSimulationConfig cfg;
    cfg.num_threads = 3;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.epoch_size = 64;
    cfg.enable_dynamic_rebalance = true;
    cfg.rebalance_check_interval_cycles = 64;
    cfg.rebalance_imbalance_threshold = 1.05;
    cfg.rebalance_min_gain = 0.0;
    cfg.initial_partition_sync_cost_ns = 0.0;

    TickSimulation sim(cfg);
    auto* tight_src = sim.createUnit<TightProducer>();
    auto* tight_dst = sim.createUnit<TightConsumer>();
    // With the tight producer/consumer forming one delay=0 cluster, uniform
    // initial cluster costs place guard_heavy0 and guard_heavy2 together.
    // Make that pair runtime-heavy so the guard exercises a real migration
    // while still verifying the tight cluster remains atomic.
    auto* h0 = sim.createUnit<HeavyUnit>("guard_heavy0", kGuardHeavy0Iterations);
    auto* h1 = sim.createUnit<HeavyUnit>("guard_heavy1", 300);
    auto* h2 = sim.createUnit<HeavyUnit>("guard_heavy2", kGuardHeavy2Iterations);
    auto* h3 = sim.createUnit<HeavyUnit>("guard_heavy3", 300);
    auto* sink = sim.createUnit<Sink>();

    sim.connect(tight_src->out, tight_dst->in, 0);
    sim.connect(h0->out, sink->in, 1);
    sim.connect(h1->out, sink->in, 1);
    sim.connect(h2->out, sink->in, 1);
    sim.connect(h3->out, sink->in, 1);

    sim.initialize();
    sim.run(kGuardCycles);

    if (sim.assignedThread(tight_src) != sim.assignedThread(tight_dst)) {
        std::cerr << "FAIL: delay=0 tight cluster was split by migration\n";
        return 1;
    }
    if (sim.rebalanceCount() == 0 || sim.epochFreeRunCount() == 0) {
        std::cerr << "FAIL: guard did not exercise epoch-free dynamic migration\n";
        return 1;
    }

    std::cout << "PASSED (checksum=" << tight_dst->checksum()
              << ", rebalances=" << sim.rebalanceCount() << ")\n";
    return 0;
}

int run_source_only_commit_guard() {
    std::cout << "Testing source-only sweep-boundary commit... ";

    TickSimulationConfig cfg;
    cfg.num_threads = 2;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.enable_epoch_free_lookahead = true;
    cfg.enable_dynamic_rebalance = true;
    cfg.initial_partition_sync_cost_ns = 0.0;

    TickSimulation sim(cfg);
    auto* p0 = sim.createUnit<TightProducer>();
    auto* p1 = sim.createUnit<TightProducer>();
    auto* c0 = sim.createUnit<TightConsumer>();
    auto* c1 = sim.createUnit<TightConsumer>();
    sim.connect(p0->out, c0->in, 1);
    sim.connect(p1->out, c1->in, 1);
    sim.initialize();

    if (!DynamicMigrationTestAccess::sourceOnlyCommit(sim)) {
        std::cerr << "FAIL: target committed, fence was bypassed, or source commit failed\n";
        return 1;
    }
    std::cout << "PASSED\n";
    return 0;
}

int main() {
    std::cout << "=== Rebalance Calibration Test ===\n";

    if (run_dynamic_wait_policy_test() != 0) {
        return 1;
    }

    if (run_runtime_planner_input_test() != 0) {
        return 1;
    }

    if (run_periodic_sampling_detection_test() != 0) {
        return 1;
    }

    if (run_multi_periodic_cluster_sampling_test() != 0) {
        return 1;
    }

    if (run_activity_scheduled_cluster_sampling_test() != 0) {
        return 1;
    }

    if (run_source_only_commit_guard() != 0) {
        return 1;
    }

    if (run_rebalance_calibration(true, 0, 64, kRunUntilCycles) != 0) {
        return 1;
    }
    if (run_rebalance_calibration(false, 0, 64, kLongRunCycles) != 0) {
        return 1;
    }
    if (run_rebalance_calibration(false, 1024, 2048, kChunkedCooldownCycles) != 0) {
        return 1;
    }
    if (run_rebalance_calibration(false, 64, 64, kChunkBoundaryCycles) != 0) {
        return 1;
    }
    if (run_tight_cluster_migration_guard() != 0) {
        return 1;
    }

    return 0;
}
