// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include "TickSimulationClockRuntime.hpp"

namespace chronon::sender {

uint64_t TickSimulation::clockRebalanceCycle_(SimTime time) const noexcept {
    const auto cycles =
        clock_detail::Wide(time.numerator()) * config_.tick_frequency_hz / time.denominator();
    return static_cast<uint64_t>(std::min(cycles, clock_detail::Wide(UINT64_MAX)));
}

uint64_t TickSimulation::dynamicMigrationCycle_() const {
    return clock_parallel_ ? clock_parallel_->rebalance_cycle.load(std::memory_order_acquire)
                           : current_cycle_;
}

uint64_t TickSimulation::dynamicActorProgress_(size_t actor) const {
    if (actor < clusters_.numClusters())
        return thread_progress_array_[actor].completed_cycle.load(std::memory_order_acquire);
    return clock_parallel_->bridges.at(actor - clusters_.numClusters())
        ->completed.load(std::memory_order_acquire);
}

bool TickSimulation::clockActorCanMigrate_(size_t actor) const {
    // Called only by the losing owner after its sweep, so edge_count and the
    // bridge's circuit/trace state need no additional atomics or lock.
    return actor < clusters_.numClusters() ||
           clock_parallel_->bridges.at(actor - clusters_.numClusters())->edge_count == 0;
}

void TickSimulation::initializeClockMigration_() {
    auto& runtime = *clock_parallel_;
    const size_t clusters = clusters_.numClusters();
    initDynamicMigrationRuntime_();
    for (size_t worker = 0; worker < runtime.worker_bridges.size(); ++worker)
        for (const size_t b : runtime.worker_bridges[worker])
            cluster_runtime_owner_[clusters + b].store(worker, std::memory_order_relaxed);

    // Work and synchronization costs must have the same physical-time basis.
    // Floating point is confined to placement heuristics, never edge ordering.
    const auto rate = [&](const ClockDomain& clock) {
        return static_cast<double>(clock.period().denominator()) /
               static_cast<double>(clock.period().numerator()) /
               static_cast<double>(config_.tick_frequency_hz);
    };
    runtime.actor_rates.resize(dynamic_runtime_cluster_count_);
    dynamic_rebalance_adjacency_.resize(dynamic_runtime_cluster_count_);
    for (size_t c = 0; c < clusters; ++c) {
        runtime.actor_rates[c] = rate(*runtime.clusters[c].clock);
        for (auto& edge : dynamic_rebalance_adjacency_[c])
            edge.activity_rate = runtime.actor_rates[c];
    }
    for (size_t b = 0; b < runtime.bridges.size(); ++b) {
        const size_t actor = clusters + b;
        for (const auto& endpoint : runtime.bridges[b]->endpoints) {
            const double endpoint_rate = rate(*endpoint.clock);
            runtime.actor_rates[actor] += endpoint_rate;
            // Endpoint-specific begin/commit handshakes, not a zero-delay edge
            // merging two hardware domains into one scheduling cluster.
            dynamic_rebalance_adjacency_[actor].push_back({endpoint.cluster, 1, 1, endpoint_rate});
            dynamic_rebalance_adjacency_[endpoint.cluster].push_back({actor, 1, 1, endpoint_rate});
        }
    }
    const uint64_t start = clockRebalanceCycle_(clock_calendar_->nextTime());
    runtime.rebalance_cycle.store(start, std::memory_order_relaxed);
    const uint64_t interval = std::max<uint64_t>(1, config_.rebalance_check_interval_cycles);
    next_dynamic_rebalance_check_cycle_.store(start + std::min(interval, UINT64_MAX - start),
                                              std::memory_order_relaxed);
}

TickSimulation::DynamicRuntimeCostEstimate TickSimulation::dynamicClockActorCost_(size_t actor) {
    DynamicRuntimeCostEstimate estimate;
    if (actor < clusters_.numClusters()) {
        estimate = dynamicClusterRuntimeCost_(actor);
    } else {
        // A sampled transaction charges both participating endpoints if their
        // edges coincide. Summed endpoint rates then count its work only once.
        const uint64_t samples = cluster_sample_count_[actor].load(std::memory_order_relaxed);
        estimate.samples = cluster_active_sample_count_[actor].load(std::memory_order_relaxed);
        estimate.ready = estimate.samples >= 4 && samples != 0;
        estimate.cost =
            samples ? std::max(0.001, static_cast<double>(cluster_sample_time_ns_[actor].load(
                                          std::memory_order_relaxed)) /
                                          static_cast<double>(samples))
                    : 1.0;
    }
    estimate.cost *= clock_parallel_->actor_rates.at(actor);
    return estimate;
}

void TickSimulation::finishClockMigrationRun_() {
    if (!config_.enable_dynamic_rebalance) return;
    // Workers have joined. A fence beyond this call's admission/stop boundary
    // must not leave a pending request blocking settlement or the next run.
    const size_t actor = migration_request_.cluster.load(std::memory_order_relaxed);
    if (actor < dynamic_runtime_cluster_count_)
        cluster_migration_pending_[actor].store(0, std::memory_order_relaxed);
    clearDynamicMigrationRequest_();
    rebuildThreadUnitsFromClusterOwners_();
    auto& lists = clock_parallel_->worker_bridges;
    for (auto& list : lists) list.clear();
    for (size_t b = 0; b < clock_parallel_->bridges.size(); ++b) {
        const size_t owner =
            cluster_runtime_owner_[clusters_.numClusters() + b].load(std::memory_order_acquire);
        lists.at(owner).push_back(b);
    }
}

}  // namespace chronon::sender
