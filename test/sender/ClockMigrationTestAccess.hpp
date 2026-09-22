// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <cassert>
#include <cmath>

#include "sender/core/TickSimulationClockRuntime.hpp"

namespace chronon::sender {

// This seam publishes the same request consumed by the production source-owner
// protocol. It never moves an actor or drains workers on behalf of the test.
struct DynamicMigrationTestAccess {
    static size_t cluster(const TickSimulation& sim, const Unit* unit) {
        const auto it = std::find(sim.unit_ptrs_.begin(), sim.unit_ptrs_.end(), unit);
        assert(it != sim.unit_ptrs_.end());
        return sim.unit_to_cluster_.at(it - sim.unit_ptrs_.begin());
    }
    static size_t bridge(const TickSimulation& sim, size_t index) {
        assert(index < sim.clock_parallel_->bridges.size());
        return sim.clusters_.numClusters() + index;
    }
    static CdcComponent*& bridgeCircuit(TickSimulation& sim, size_t index) {
        return sim.clock_parallel_->bridges.at(index)->lanes.at(0).circuit;
    }
    static void assertBridgeBeginServiceExcluded(const TickSimulation& sim, size_t index) {
        const auto& state = *sim.clock_parallel_->bridges.at(index);
        if (state.sample) assert(state.sample_ns == 0);
    }
    static void assertBridgeServiceExcluded(const TickSimulation& sim, size_t index) {
        const auto actor = bridge(sim, index);
        assert(sim.cluster_active_sample_count_[actor].load() >= 4);
        assert(sim.cluster_sample_time_ns_[actor].load() == 0);
    }
    static size_t owner(const TickSimulation& sim, size_t actor) {
        return sim.cluster_runtime_owner_[actor].load(std::memory_order_acquire);
    }
    static bool request(TickSimulation& sim, size_t actor, uint64_t fence) {
        using State = TickSimulation::MigrationRequestState;
        uint8_t expected = static_cast<uint8_t>(State::None);
        if (!sim.migration_request_.state.compare_exchange_strong(
                expected, static_cast<uint8_t>(State::Requested), std::memory_order_acq_rel))
            return false;
        const size_t source = owner(sim, actor);
        sim.last_rebalance_detail_ = "test hot clock migration";
        sim.migration_request_.cluster.store(actor, std::memory_order_relaxed);
        sim.migration_request_.source_thread.store(source, std::memory_order_relaxed);
        sim.migration_request_.target_thread.store((source + 1) % sim.thread_units_.size(),
                                                   std::memory_order_relaxed);
        sim.migration_request_.fence_cycle.store(fence, std::memory_order_relaxed);
        sim.cluster_migration_pending_[actor].store(1, std::memory_order_relaxed);
        sim.migration_request_.state.store(static_cast<uint8_t>(State::Quiescing),
                                           std::memory_order_release);
        return true;
    }
    static bool request(TickSimulation& sim, size_t actor) {
        return request(sim, actor, sim.dynamicActorProgress_(actor));
    }
    static void assertIdle(const TickSimulation& sim) {
        assert(!sim.epoch_free_dynamic_runtime_active_.load());
        assert(sim.migration_request_.state.load() ==
               static_cast<uint8_t>(TickSimulation::MigrationRequestState::None));
        for (size_t a = 0; a < sim.dynamic_runtime_cluster_count_; ++a)
            assert(!sim.cluster_migration_pending_[a].load());
    }

    static void verifySafePoints(TickSimulation& sim) {
        const size_t actor = bridge(sim, 0);
        auto& state = *sim.clock_parallel_->bridges.front();
        const size_t source = owner(sim, actor);
        const size_t target = (source + 1) % sim.thread_units_.size();
        const uint64_t generation = sim.cluster_assignment_generation_.load();
        state.completed.store(7);
        state.edge_count = 1;
        sim.clock_parallel_->rebalance_cycle.store(9000);
        assert(request(sim, actor, 8));
        sim.serviceEpochFreeMigration_(source);
        assert(owner(sim, actor) == source);  // Local progress is below the fence.
        state.completed.store(8);
        sim.serviceEpochFreeMigration_(source);
        assert(owner(sim, actor) == source);  // begin/commit still in flight.
        state.edge_count = 0;
        sim.serviceEpochFreeMigration_(target);
        assert(owner(sim, actor) == source);  // Only the losing owner can commit.
        sim.serviceEpochFreeMigration_(source);
        assert(owner(sim, actor) == target);
        assert(sim.cluster_assignment_generation_.load() == generation + 1);
        assert(sim.dynamic_cluster_last_migration_cycle_[actor] == 9000);
        assert(sim.next_dynamic_rebalance_check_cycle_.load() ==
               9000 + std::max(sim.config_.rebalance_check_interval_cycles,
                               sim.config_.rebalance_cooldown_cycles));
        assertIdle(sim);
        state.completed.store(0);
        sim.finishClockMigrationRun_();
    }

    static void verifyRates(TickSimulation& sim) {
        const auto& runtime = *sim.clock_parallel_;
        const auto expected_rate = [&](const ClockDomain& clock) {
            return (clock.id() == 1 ? 2'000'000'000.0 : 100'000'000.0) / sim.tickFrequencyHz();
        };
        for (size_t c = 0; c < sim.clusters_.numClusters(); ++c) {
            for (const auto u : sim.clusters_.clusters[c]) {
                sim.dynamic_unit_active_sample_time_ns_[u].store(400);
                sim.dynamic_unit_active_sample_count_[u].store(4);
                sim.dynamic_unit_observed_cycles_[u].store(1024);
                sim.dynamic_unit_observed_active_ticks_[u].store(1024);
            }
            const double rate = expected_rate(*runtime.clusters[c].clock);
            assert(std::abs(runtime.actor_rates[c] - rate) < 1e-9);
            assert(runtime.clusters[c].sample_interval ==
                   static_cast<uint64_t>(std::ceil(detail::kDynamicTickSampleInterval * rate)));
            const double expected = 100.0 * sim.clusters_.clusters[c].size() * rate;
            const auto cost = sim.dynamicClockActorCost_(c);
            assert(cost.ready && std::abs(cost.cost - expected) < 1e-9);
            assert(sim.dynamicClusterRuntimeCost_(c).cost ==
                   100.0 * sim.clusters_.clusters[c].size());
        }
        const size_t b = bridge(sim, 0);
        sim.cluster_sample_time_ns_[b].store(800);
        sim.cluster_sample_count_[b].store(8);  // Four coincident-edge transactions.
        sim.cluster_active_sample_count_[b].store(4);
        const auto cost = sim.dynamicClockActorCost_(b);
        const double bridge_rate = 2'100'000'000.0 / sim.tickFrequencyHz();
        assert(cost.ready && std::abs(cost.cost - 100.0 * bridge_rate) < 1e-9);
        assert(sim.dynamic_rebalance_adjacency_.size() == sim.dynamic_runtime_cluster_count_);
        assert(sim.dynamic_rebalance_adjacency_[b].size() == 2);
        for (const auto& edge : sim.dynamic_rebalance_adjacency_[b])
            assert(std::abs(edge.activity_rate -
                            expected_rate(*runtime.clusters[edge.neighbor].clock)) < 1e-9);
        assert(sim.clockRebalanceCycle_(SimTime(3, 7)) ==
               uint64_t(clock_detail::Wide(3) * sim.tickFrequencyHz() / 7));
        assert(sim.clockRebalanceCycle_(SimTime(UINT64_MAX)) == UINT64_MAX);
    }

    static void verifyPlacementAndWaits(TickSimulation& sim) {
        const size_t count = sim.clusters_.numClusters();
        PartitionInput input;
        input.num_units = count;
        input.num_threads = sim.thread_units_.size();
        input.sync_cost_ns = sim.config_.initial_partition_sync_cost_ns;
        input.unit_cost_ns.assign(count, 1.0);
        input.adjacency.resize(count);
        std::unordered_map<Unit*, size_t> indices;
        for (size_t u = 0; u < sim.unit_ptrs_.size(); ++u) indices.emplace(sim.unit_ptrs_[u], u);
        sim.addClockPartitionActors_(input, indices);
        assert(input.num_units == count + sim.cdc_.size());
        for (size_t c = 0; c < count; ++c) {
            const auto& state = sim.clock_parallel_->clusters[c];
            const double expected =
                (state.clock->id() == 1 ? 2'000'000'000.0 : 100'000'000.0) / sim.tickFrequencyHz();
            assert(std::abs(input.unit_cost_ns[c] - expected) < 1e-9);
            assert(state.domain->completions.size() == 6);  // Three units and three FIFO endpoints.
        }
        for (size_t b = count; b < input.num_units; ++b)
            assert(std::abs(input.unit_cost_ns[b] - 2'100'000'000.0 / sim.tickFrequencyHz()) <
                   1e-9);
        std::vector<double> per_thread(input.num_threads);
        std::vector<size_t> placement(count, 0);
        placement.insert(placement.end(), sim.clock_bridge_owners_.begin(),
                         sim.clock_bridge_owners_.end());
        for (size_t c = 0; c < count; ++c) placement[c] = sim.cluster_to_thread_[c];
        partition_utils::computeThreadTimes(input, placement, per_thread);
        assert(*std::min_element(per_thread.begin(), per_thread.end()) > 0);

        auto& runtime = *sim.clock_parallel_;
        const size_t b = bridge(sim, 0);
        const size_t c = runtime.bridges[0]->endpoints[0].cluster;
        BlockedClusterInfo dependency;
        dependency.cluster = b;
        dependency.pred_cluster = c;
        sim.recordClockWaitSample_(0, dependency, SimTime{}, 100);
        assert(sim.dynamic_thread_dep_wait_ns_[0].load() == 100);
        assert(sim.dynamic_cluster_blocked_wait_ns_[b].load() == 100);
        assert(sim.dynamic_cluster_blocker_wait_ns_[c].load() == 100);
        // Same local cycle, but the phased slow edge is later than fast edge 0.
        sim.recordClockWaitSample_(0, dependency, SimTime::picoseconds(137), 50);
        assert(sim.dynamic_thread_dep_wait_ns_[0].load() == 100);
        runtime.domains.at(1).retired.store(1);
        sim.recordClockWaitSample_(0, dependency, SimTime::picoseconds(137), 50);
        assert(sim.dynamic_thread_dep_wait_ns_[0].load() == 150);
        runtime.domains.at(1).retired.store(0);
        dependency.pred_cluster = SIZE_MAX;
        sim.recordClockWaitSample_(0, dependency, SimTime::nanoseconds(1), 25);
        assert(sim.dynamic_thread_floor_wait_ns_[0].load() == 25);
    }

    static void placeAllOnWorkerZero(TickSimulation& sim) {
        for (size_t a = 0; a < sim.dynamic_runtime_cluster_count_; ++a)
            sim.cluster_runtime_owner_[a].store(0);
        sim.cluster_assignment_generation_.fetch_add(1);
        sim.finishClockMigrationRun_();
    }

    static void verifyBudget(TickSimulation& sim) {
        auto& runtime = *sim.clock_parallel_;
        runtime.migration_max_batches = UINT64_MAX;
        runtime.migration_completed_batches.store(UINT64_MAX - 3);
        runtime.migration_window = 16;
        assert(runtime.migrationHorizon(0) == 0);
        runtime.migration_completed_batches.store(0);
        runtime.migration_window = 0;
        runtime.migration_limit = 100;
        assert(runtime.migrationHorizon(99) == 1);
        assert(runtime.migrationHorizon(101) == 0);
        runtime.migration_limit = UINT64_MAX;
    }

    static void assertCostsReady(TickSimulation& sim, bool ready) {
        for (size_t c = 0; c < sim.clusters_.numClusters(); ++c)
            assert(sim.dynamicClockActorCost_(c).ready == ready);
    }

    static size_t planBridge(TickSimulation& sim) {
        placeAllOnWorkerZero(sim);
        for (size_t u = 0; u < sim.unit_ptrs_.size(); ++u) {
            sim.dynamic_unit_active_sample_time_ns_[u].store(4);
            sim.dynamic_unit_active_sample_count_[u].store(4);
            sim.dynamic_unit_observed_cycles_[u].store(1024);
            sim.dynamic_unit_observed_active_ticks_[u].store(1024);
        }
        for (size_t b = sim.clusters_.numClusters(); b < sim.dynamic_runtime_cluster_count_; ++b) {
            sim.cluster_sample_time_ns_[b].store(400'000);
            sim.cluster_sample_count_[b].store(4);
            sim.cluster_active_sample_count_[b].store(4);
        }
        sim.clock_parallel_->migration_max_batches = UINT64_MAX;
        sim.clock_parallel_->migration_benefit.startRun(0, detail::MigrationBenefit::now());
        assert(!sim.maybeRequestEpochFreeMigration_(100'000));  // First window is not confidence.
        for (size_t u = 0; u < sim.unit_ptrs_.size(); ++u) {
            sim.dynamic_unit_active_sample_time_ns_[u].store(8);
            sim.dynamic_unit_active_sample_count_[u].store(8);
            sim.dynamic_unit_observed_cycles_[u].store(2048);
            sim.dynamic_unit_observed_active_ticks_[u].store(2048);
        }
        for (size_t b = sim.clusters_.numClusters(); b < sim.dynamic_runtime_cluster_count_; ++b) {
            sim.cluster_sample_time_ns_[b].store(800'000);
            sim.cluster_sample_count_[b].store(8);
            sim.cluster_active_sample_count_[b].store(8);
        }
        // The expensive bridge is ready, but one resident source actor still
        // has unknown fresh cost. It must not be treated as a free background.
        const size_t unknown = sim.unit_ptrs_.size() - 1;
        sim.dynamic_unit_active_sample_count_[unknown].store(4);
        assert(!sim.maybeRequestEpochFreeMigration_(200'000));
        sim.dynamic_unit_active_sample_count_[unknown].store(8);
        assert(sim.maybeRequestEpochFreeMigration_(300'000));
        const size_t actor = sim.migration_request_.cluster.load();
        assert(actor >= sim.clusters_.numClusters());
        return actor;
    }
};

}  // namespace chronon::sender
