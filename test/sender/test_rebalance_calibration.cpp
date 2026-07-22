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
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/Port.hpp"
#include "sender/schedule/EpochFreeTopologyCost.hpp"

using namespace chronon::sender;

namespace chronon::sender {

struct DynamicMigrationTestAccess {
    struct SamplingStats {
        detail::DynamicTickSamplingSchedule schedule;
        uint64_t samples = 0;
        uint64_t total_ns = 0;
    };

    static std::vector<SamplingStats> samplingStats(const TickSimulation& sim) {
        std::vector<SamplingStats> out;
        out.reserve(sim.dynamic_cluster_tick_sample_schedule_.size());
        for (size_t c = 0; c < sim.dynamic_cluster_tick_sample_schedule_.size(); ++c) {
            out.push_back({sim.dynamic_cluster_tick_sample_schedule_[c],
                           sim.cluster_sample_count_[c].load(std::memory_order_relaxed),
                           sim.cluster_sample_time_ns_[c].load(std::memory_order_relaxed)});
        }
        return out;
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

int run_runtime_planner_input_test() {
    using chronon::sender::epoch_free_cost::RuntimeDependency;

    const auto ordinary = chronon::sender::detail::dynamicTickSamplingSchedule(1);
    const auto periodic_256 = chronon::sender::detail::dynamicTickSamplingSchedule(256);
    if (chronon::sender::detail::shouldSampleDynamicTick(0, periodic_256) ||
        !chronon::sender::detail::shouldSampleDynamicTick(255, ordinary) ||
        chronon::sender::detail::shouldSampleDynamicTick(256, ordinary) ||
        chronon::sender::detail::shouldSampleDynamicTick(255, periodic_256) ||
        !chronon::sender::detail::shouldSampleDynamicTick(256, periodic_256)) {
        std::cerr << "FAIL: dynamic tick sampling must use warm window ends for ordinary "
                     "clusters and non-cold boundaries for periodic clusters\n";
        return 1;
    }
    const uint64_t first_window =
        chronon::sender::TickSimulationConfig{}.rebalance_check_interval_cycles;
    size_t warm_samples = 0;
    for (uint64_t cycle = 0; cycle < first_window; ++cycle) {
        warm_samples += chronon::sender::detail::shouldSampleDynamicTick(cycle, ordinary) ? 1 : 0;
    }
    if (warm_samples != first_window / chronon::sender::detail::kDynamicTickSampleInterval) {
        std::cerr << "FAIL: default rebalance window must contain only complete warm samples\n";
        return 1;
    }

    for (const uint32_t tick_interval : {2U, 3U, 255U, 256U, 257U, 1000U}) {
        const auto schedule = chronon::sender::detail::dynamicTickSamplingSchedule(tick_interval);
        if (schedule.phase != 0 || schedule.period < tick_interval ||
            schedule.period % tick_interval != 0 ||
            !chronon::sender::detail::shouldSampleDynamicTick(schedule.period, schedule)) {
            std::cerr << "FAIL: periodic dynamic sample is not an active tick for interval "
                      << tick_interval << '\n';
            return 1;
        }
        for (uint64_t sample = schedule.period, count = 0; count < 4;
             sample += schedule.period, ++count) {
            if (sample % tick_interval != 0 ||
                !chronon::sender::detail::shouldSampleDynamicTick(sample, schedule)) {
                std::cerr << "FAIL: first four dynamic samples alias inactive interval "
                          << tick_interval << '\n';
                return 1;
            }
        }
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

int run_periodic_sampling_detection_test() {
    TickSimulationConfig cfg;
    cfg.num_threads = 3;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.enable_epoch_free_lookahead = true;
    cfg.enable_dynamic_rebalance = true;
    cfg.rebalance_check_interval_cycles = 4096;
    cfg.initial_partition_sync_cost_ns = 0.0;

    TickSimulation sim(cfg);
    auto* periodic = sim.createUnit<HeavyUnit>("periodic", 5000);
    auto* ordinary0 = sim.createUnit<HeavyUnit>("ordinary0", 1);
    auto* ordinary1 = sim.createUnit<HeavyUnit>("ordinary1", 1);
    auto* ordinary2 = sim.createUnit<HeavyUnit>("ordinary2", 1);
    auto* sink = sim.createUnit<Sink>();
    periodic->setTickInterval(256);
    sim.connect(periodic->out, sink->in, 1);
    sim.connect(ordinary0->out, sink->in, 1);
    sim.connect(ordinary1->out, sink->in, 1);
    sim.connect(ordinary2->out, sink->in, 1);
    sim.initialize();
    sim.run(1025);

    const auto stats = DynamicMigrationTestAccess::samplingStats(sim);
    const size_t periodic_clusters = static_cast<size_t>(std::count_if(
        stats.begin(), stats.end(), [](const auto& entry) { return entry.schedule.phase == 0; }));
    const bool sampled_four_active_ticks =
        std::any_of(stats.begin(), stats.end(), [](const auto& entry) {
            return entry.schedule.phase == 0 && entry.schedule.period == 256 &&
                   entry.samples == 4 && entry.total_ns > 0;
        });
    if (periodic_clusters != 1 || !sampled_four_active_ticks) {
        std::cerr << "FAIL: periodic tick interval did not select boundary-aligned sampling "
                  << "(clusters=" << periodic_clusters << ")\n";
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

    if (run_runtime_planner_input_test() != 0) {
        return 1;
    }

    if (run_periodic_sampling_detection_test() != 0) {
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
