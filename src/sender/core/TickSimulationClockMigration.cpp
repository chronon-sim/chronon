// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include "TickSimulationClockRuntime.hpp"

namespace chronon::sender {

namespace {
double edgeRate(const ClockDomain& clock, uint64_t reference_hz) {
    return static_cast<double>(clock.period().denominator()) /
           static_cast<double>(clock.period().numerator()) / reference_hz;
}
}  // namespace

void TickSimulation::addClockPartitionActors_(
    PartitionInput& input, const std::unordered_map<Unit*, size_t>& unit_indices) const {
    const auto rate = [&](const ClockDomain& clock) {
        return edgeRate(clock, config_.tick_frequency_hz);
    };
    const size_t clusters = clusters_.numClusters();
    input.num_units += cdc_.size();
    input.unit_cost_ns.resize(input.num_units);
    input.adjacency.resize(input.num_units);
    for (size_t c = 0; c < clusters; ++c) {
        const auto& clock = unit_ptrs_[clusters_.clusters[c].front()]->clockDomain();
        input.unit_cost_ns[c] *= rate(clock);
        for (auto& edge : input.adjacency[c]) edge.activity_rate = rate(clock);
    }
    for (size_t b = 0; b < cdc_.size(); ++b) {
        const size_t actor = clusters + b;
        for (auto* unit : {cdc_[b]->writeOwner(), cdc_[b]->readOwner()}) {
            const size_t endpoint = unit_to_cluster_[unit_indices.at(unit)];
            const double frequency = rate(unit->clockDomain());
            // No speculative simulation warmup: use the existing uniform-cost
            // prior for each endpoint until live bridge samples become ready.
            input.unit_cost_ns[actor] += frequency;
            input.adjacency[actor].push_back({endpoint, 1, 1, frequency});
            input.adjacency[endpoint].push_back({actor, 1, 1, frequency});
        }
    }
}

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

void TickSimulation::recordClockWaitSample_(size_t worker, const BlockedClusterInfo& blocker,
                                            SimTime edge_time, uint64_t elapsed_ns) {
    if (blocker.pred_cluster != SIZE_MAX) {
        // Credit only a dependency at the exact physical retirement frontier.
        // Stale per-domain publications can suppress a sample, never turn a
        // hidden runahead wait into critical-path loss. Local cycle numbers
        // (or rounded nanoseconds) are not comparable across clock domains.
        bool at_frontier = false;
        for (const auto& [id, domain] : clock_parallel_->domains) {
            (void)id;
            const auto next = domain.clock->edge(domain.retired.load(std::memory_order_acquire));
            if (next < edge_time) return;
            at_frontier |= next == edge_time;
        }
        if (!at_frontier) return;
    }
    recordDynamicWaitSample_(worker, blocker, elapsed_ns);
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
        return edgeRate(clock, config_.tick_frequency_hz);
    };
    runtime.actor_rates.resize(dynamic_runtime_cluster_count_);
    dynamic_rebalance_adjacency_.resize(dynamic_runtime_cluster_count_);
    for (size_t c = 0; c < clusters; ++c) {
        auto& state = runtime.clusters[c];
        runtime.actor_rates[c] = rate(*state.clock);
        // Reuse the sparse unit sampler on a common physical-time cadence.
        // Slow domains must not need 1024 local edges before their first ready
        // estimate. Round up to a whole local edge; keep four-sample confidence
        // and exact local activity-window accounting unchanged.
        const auto numerator = clock_detail::Wide(detail::kDynamicTickSampleInterval) *
                               state.clock->period().denominator();
        const auto denominator =
            clock_detail::Wide(state.clock->period().numerator()) * config_.tick_frequency_hz;
        state.sample_interval = static_cast<uint64_t>(
            std::clamp(numerator / denominator + (numerator % denominator != 0),
                       clock_detail::Wide(1), clock_detail::Wide(UINT64_MAX)));
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
