// SPDX-License-Identifier: MPL-2.0
#include "TickSimulationClockRuntime.hpp"
#include "sender/schedule/PreparedTopologyCost.hpp"
#include "sender/schedule/SmallPlacementCost.hpp"

namespace chronon::sender {
namespace {
constexpr uint64_t kNoMigrationCycle = std::numeric_limits<uint64_t>::max();
uint64_t saturatingCycleAdd(uint64_t base, uint64_t delta) noexcept {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    return delta > max - base ? max : base + delta;
}
}  // namespace

// One exclusive planner may run on any pool thread. Retained capacities are
// bounded by topology/worker dimensions and released on progress replacement
// or simulation destruction; each planning pass replaces every sample.
struct TickSimulation::PlanningScratch {
    PartitionInput input;
    epoch_free_cost::PreparedTopologyCost prepared;
    epoch_free_cost::ObjectiveSummary fallback;
    std::vector<double> thread_cost;
    std::vector<size_t> thread_active_clusters, assignment, source_threads;
    std::vector<uint8_t> cluster_cost_ready, thread_cost_ready;
    std::vector<uint64_t> thread_floor_wait, thread_dep_wait, thread_no_ready_wait,
        cluster_blocked_wait, cluster_blocker_wait;
};

bool TickSimulation::maybeRequestEpochFreeMigration_(uint64_t cycle) {
    if (!config_.enable_dynamic_rebalance || !cluster_runtime_owner_ ||
        dynamic_runtime_cluster_count_ == 0 || config_.num_threads < 2) {
        return false;
    }

    const uint64_t interval = std::max<uint64_t>(1, config_.rebalance_check_interval_cycles);
    const uint64_t floor_cycle = lookahead_floor_.load(std::memory_order_acquire);
    const uint64_t gate_cycle = std::max(floor_cycle, cycle);
    uint64_t next = next_dynamic_rebalance_check_cycle_.load(std::memory_order_relaxed);
    while (gate_cycle >= next) {
        if (next_dynamic_rebalance_check_cycle_.compare_exchange_weak(
                next, saturatingCycleAdd(gate_cycle, interval), std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            break;
        }
    }
    if (gate_cycle < next) {
        return false;
    }

    // Planning is comparatively expensive. Keep the request state at None so
    // peer workers retain their stable-sweep fast path until a complete,
    // generation-validated migration is ready to publish.
    if (dynamic_planner_busy_.test_and_set(std::memory_order_acquire)) {
        return false;
    }
    struct PlannerGuard {
        std::atomic_flag& busy;
        ~PlannerGuard() { busy.clear(std::memory_order_release); }
    } planner_guard{dynamic_planner_busy_};

    const uint8_t none = static_cast<uint8_t>(MigrationRequestState::None);
    if (migration_request_.state.load(std::memory_order_acquire) != none) return false;
    const uint64_t planning_generation =
        cluster_assignment_generation_.load(std::memory_order_acquire);

    auto* benefit = clock_mode_ ? &clock_parallel_->migration_benefit : nullptr;
    const uint64_t planning_started = benefit ? detail::MigrationBenefit::now() : 0;
    const uint64_t benefit_interval = std::max(interval, 4 * detail::kDynamicTickSampleInterval);
    bool published = false, assessed = false;
    struct BenefitGuard {
        detail::MigrationBenefit* benefit;
        std::atomic<uint64_t>& next;
        uint64_t start, cycle, interval;
        bool& published;
        bool& assessed;
        ~BenefitGuard() {
            if (!benefit) return;
            const uint64_t elapsed = detail::MigrationBenefit::now() - start;
            benefit->planning_ns = std::max(double(elapsed), benefit->planning_ns * 0.875);
            ++benefit->planning_calls;
            benefit->planning_total_ns += elapsed;
            if (!published && !benefit->pending) {
                if (assessed) benefit->defer();
                // Compare the next candidate against recent physical progress,
                // not a lifetime rate that could hide a workload phase change.
                benefit->window_cycle = cycle;
                benefit->window_ns = start;
            }
            const uint64_t check =
                detail::MigrationBenefit::add(cycle, benefit->interval(interval));
            auto old = next.load(std::memory_order_relaxed);
            while (old < check &&
                   !next.compare_exchange_weak(old, check, std::memory_order_relaxed)) {
            }
        }
    } benefit_guard{benefit,          next_dynamic_rebalance_check_cycle_,
                    planning_started, gate_cycle,
                    benefit_interval, published,
                    assessed};
    if (benefit) {
        benefit->handoff_ns =
            std::max(benefit->handoff_ns,
                     double(clock_parallel_->migration_handoff_ns.load(std::memory_order_relaxed)));
        if (!benefit->feedback(cycle, planning_started, planning_generation, benefit_interval))
            return false;
        if (clock_parallel_->migrationHorizon(cycle) < benefit_interval) return false;
        benefit->confidence.resize(dynamic_runtime_cluster_count_);
    }

    const size_t num_threads = thread_units_.size();
    const size_t num_clusters = dynamic_runtime_cluster_count_;
    auto& planning = schedulerScratch_().planning;
    if (!planning) planning = std::make_shared<PlanningScratch>();
    auto& scratch = *planning;
    auto& input = scratch.input;
    auto& cluster_cost = input.unit_cost_ns;
    cluster_cost.assign(num_clusters, 0.0);
    auto& thread_cost = scratch.thread_cost;
    thread_cost.assign(num_threads, 0);
    auto& thread_active_clusters = scratch.thread_active_clusters;
    thread_active_clusters.assign(num_threads, 0);
    auto& thread_floor_wait = scratch.thread_floor_wait;
    thread_floor_wait.assign(num_threads, 0);
    auto& thread_dep_wait = scratch.thread_dep_wait;
    thread_dep_wait.assign(num_threads, 0);
    auto& thread_no_ready_wait = scratch.thread_no_ready_wait;
    thread_no_ready_wait.assign(num_threads, 0);
    auto& cluster_blocked_wait = scratch.cluster_blocked_wait;
    cluster_blocked_wait.assign(num_clusters, 0);
    auto& cluster_blocker_wait = scratch.cluster_blocker_wait;
    cluster_blocker_wait.assign(num_clusters, 0);
    auto& assignment = scratch.assignment;
    assignment.assign(num_clusters, 0);
    auto& cluster_cost_ready = scratch.cluster_cost_ready;
    cluster_cost_ready.assign(num_clusters, 0);
    auto& thread_cost_ready = scratch.thread_cost_ready;
    thread_cost_ready.assign(num_threads, 1);

    for (size_t c = 0; c < num_clusters; ++c) {
        const auto estimate =
            clock_mode_ ? dynamicClockActorCost_(c, true) : dynamicClusterRuntimeCost_(c);
        cluster_cost[c] = estimate.cost;
        cluster_cost_ready[c] = benefit ? benefit->confidence[c].observe(
                                              estimate.cost, estimate.samples, estimate.ready)
                                        : estimate.ready;

        size_t owner = cluster_runtime_owner_[c].load(std::memory_order_acquire);
        if (owner >= num_threads) owner = 0;
        assignment[c] = owner;
        if (owner < num_threads) {
            thread_cost[owner] += cluster_cost[c];
            thread_cost_ready[owner] &= cluster_cost_ready[c];
            if (cluster_cost[c] > 0.0) {
                ++thread_active_clusters[owner];
            }
        }
    }
    for (size_t t = 0; t < num_threads && t < dynamic_runtime_thread_count_; ++t) {
        thread_floor_wait[t] = dynamic_thread_floor_wait_ns_[t].load(std::memory_order_relaxed);
        thread_dep_wait[t] = dynamic_thread_dep_wait_ns_[t].load(std::memory_order_relaxed);
        thread_no_ready_wait[t] =
            dynamic_thread_no_ready_wait_ns_[t].load(std::memory_order_relaxed);
    }
    for (size_t c = 0; c < num_clusters; ++c) {
        cluster_blocked_wait[c] =
            dynamic_cluster_blocked_wait_ns_[c].load(std::memory_order_relaxed);
        cluster_blocker_wait[c] =
            dynamic_cluster_blocker_wait_ns_[c].load(std::memory_order_relaxed);
    }

    // A fully sampled balanced workload is assessed even if it has no source.
    // Otherwise only a complete source/target pair below can justify backoff;
    // an empty worker alone says nothing about an unknown-cost source.
    assessed = !benefit || std::all_of(thread_cost_ready.begin(), thread_cost_ready.end(),
                                       [](uint8_t ready) { return ready != 0; });
    double total_cost = 0.0;
    for (double cost : thread_cost) total_cost += cost;
    if (total_cost <= 0.0) {
        return false;
    }

    const double avg = total_cost / static_cast<double>(num_threads);
    if (avg <= 0.0) {
        return false;
    }

    auto& source_threads = scratch.source_threads;
    source_threads.clear();
    uint64_t total_dep_wait = 0;
    for (uint64_t wait : thread_dep_wait) total_dep_wait += wait;
    const double avg_dep_wait =
        num_threads > 0 ? static_cast<double>(total_dep_wait) / static_cast<double>(num_threads)
                        : 0.0;
    for (size_t t = 0; t < num_threads; ++t) {
        const bool active_overloaded =
            (thread_cost[t] / avg) > config_.rebalance_imbalance_threshold;
        const bool dep_overloaded = avg_dep_wait > 0.0 &&
                                    static_cast<double>(thread_dep_wait[t]) > avg_dep_wait * 1.25 &&
                                    thread_cost[t] > avg * 0.90;
        if (active_overloaded || dep_overloaded) {
            source_threads.push_back(t);
        }
    }
    if (source_threads.empty()) {
        return false;
    }

    input.num_units = num_clusters;
    input.num_threads = num_threads;
    input.sync_cost_ns = epoch_free_cost::runtimeSyncCostNs(platform_metrics_.atomic_roundtrip_ns);
    input.critical_path_weight = config_.sa_critical_path_weight;
    input.adjacency = dynamic_rebalance_adjacency_;

    epoch_free_cost::RuntimeWaits waits{&thread_floor_wait, &thread_dep_wait, &thread_no_ready_wait,
                                        &cluster_blocked_wait, &cluster_blocker_wait};
    auto& prepared = scratch.prepared;
    prepared.prepare(input, assignment, waits, planning_generation);
    const uint64_t history_cooldown = std::max(config_.rebalance_cooldown_cycles,
                                               detail::MigrationBenefit::multiply(interval, 2));
    const uint64_t pingpong_cooldown =
        std::max(history_cooldown, detail::MigrationBenefit::multiply(interval, 4));
    auto last_migration_cycle = [&](size_t c) -> uint64_t {
        return c < dynamic_cluster_last_migration_cycle_.size()
                   ? dynamic_cluster_last_migration_cycle_[c]
                   : kNoMigrationCycle;
    };
    auto in_history_cooldown = [&](uint64_t last_cycle) {
        return last_cycle != kNoMigrationCycle &&
               cycle < saturatingCycleAdd(last_cycle, history_cooldown);
    };
    auto in_pingpong_cooldown = [&](size_t c, size_t candidate_source, size_t candidate_target,
                                    uint64_t last_cycle) {
        return last_cycle != kNoMigrationCycle && c < dynamic_cluster_last_source_thread_.size() &&
               c < dynamic_cluster_last_target_thread_.size() &&
               dynamic_cluster_last_source_thread_[c] == candidate_target &&
               dynamic_cluster_last_target_thread_[c] == candidate_source &&
               cycle < saturatingCycleAdd(last_cycle, pingpong_cooldown);
    };
    const auto profitable = [&](size_t actor, const epoch_free_cost::MoveBreakdown& move) {
        if (!benefit) return true;
        const double horizon =
            std::min(clock_parallel_->migrationHorizon(cycle),
                     double(detail::MigrationBenefit::multiply(benefit_interval, 8)));
        return benefit->profitable(
            move.active_gain, move.topology_delta,
            cluster_cost[actor] / clock_parallel_->actor_rates[actor], move.old_max_active, horizon,
            detail::MigrationBenefit::now() - planning_started, benefit->timer_ns, num_clusters,
            benefit->rate(cycle, planning_started), config_.rebalance_min_gain);
    };
    size_t source = SIZE_MAX;
    size_t cluster = SIZE_MAX;
    size_t target = SIZE_MAX;
    double best_cluster_cost = 0.0;
    epoch_free_cost::MoveBreakdown best_breakdown;
    for (size_t candidate_source : source_threads) {
        for (size_t c = 0; c < num_clusters; ++c) {
            if (cluster_runtime_owner_[c].load(std::memory_order_acquire) != candidate_source) {
                continue;
            }
            if (cluster_migration_pending_[c].load(std::memory_order_acquire) != 0) continue;
            if (!cluster_cost_ready[c]) continue;
            if (cluster_cost[c] <= 0.0) continue;
            const uint64_t last_cycle = last_migration_cycle(c);
            if (in_history_cooldown(last_cycle)) continue;

            for (size_t candidate_target = 0; candidate_target < num_threads; ++candidate_target) {
                if (candidate_target == candidate_source) continue;
                if (in_pingpong_cooldown(c, candidate_source, candidate_target, last_cycle))
                    continue;
                if (benefit &&
                    (!thread_cost_ready[candidate_source] || !thread_cost_ready[candidate_target]))
                    continue;
                assessed = true;

                const double churn =
                    last_cycle == kNoMigrationCycle ? 0.0 : std::max(0.001, cluster_cost[c] * 0.05);
                auto breakdown =
                    prepared.scoreMove(c, candidate_target, config_.rebalance_min_gain, churn);
                if (!breakdown.valid) continue;
                if (breakdown.score >= best_breakdown.score - prepared.roundoff())
                    breakdown =
                        prepared.scoreFull(c, candidate_target, config_.rebalance_min_gain, churn);
                if (!breakdown.valid || !profitable(c, breakdown)) continue;
                if (breakdown.score > best_breakdown.score ||
                    (breakdown.score == best_breakdown.score && c < cluster)) {
                    best_breakdown = breakdown;
                    best_cluster_cost = cluster_cost[c];
                    source = candidate_source;
                    cluster = c;
                    target = candidate_target;
                }
            }
        }
    }
    if (source == SIZE_MAX && !benefit) {
        size_t fallback_source = source_threads.front();
        for (size_t t : source_threads) {
            if (thread_cost[t] > thread_cost[fallback_source]) fallback_source = t;
        }
        size_t fallback_target = SIZE_MAX;
        for (size_t t = 0; t < num_threads; ++t) {
            if (t == fallback_source) continue;
            if (fallback_target == SIZE_MAX || thread_cost[t] < thread_cost[fallback_target]) {
                fallback_target = t;
            }
        }
        if (fallback_target != SIZE_MAX) {
            for (size_t c = 0; c < num_clusters; ++c) {
                if (cluster_runtime_owner_[c].load(std::memory_order_acquire) != fallback_source) {
                    continue;
                }
                if (cluster_migration_pending_[c].load(std::memory_order_acquire) != 0) continue;
                if (!cluster_cost_ready[c]) continue;
                const uint64_t last_cycle = last_migration_cycle(c);
                if (in_history_cooldown(last_cycle)) continue;
                if (in_pingpong_cooldown(c, fallback_source, fallback_target, last_cycle)) continue;
                const double target_after = thread_cost[fallback_target] + cluster_cost[c];
                if (target_after > thread_cost[fallback_source] * 1.02) continue;
                if (cluster == SIZE_MAX || cluster_cost[c] > best_cluster_cost) {
                    source = fallback_source;
                    target = fallback_target;
                    cluster = c;
                    best_cluster_cost = cluster_cost[c];
                }
            }
        }
        if (cluster != SIZE_MAX) {
            const auto& old_summary = prepared.baseline();
            auto& new_summary = scratch.fallback;
            prepared.evaluateFull(new_summary, cluster, target);
            best_breakdown.objective_gain = old_summary.objective - new_summary.objective;
            best_breakdown.active_gain = old_summary.max_active - new_summary.max_active;
            best_breakdown.topology_delta =
                (old_summary.cross_pressure + old_summary.max_incoming_pressure) -
                (new_summary.cross_pressure + new_summary.max_incoming_pressure);
            best_breakdown.score = std::max(0.0, best_breakdown.active_gain);
            best_breakdown.old_max_active = old_summary.max_active;
            best_breakdown.new_max_active = new_summary.max_active;
            best_breakdown.target_active_after = thread_cost[target] + best_cluster_cost;
            best_breakdown.active_budget = std::max(avg * 1.15, old_summary.max_active * 0.72);
            best_breakdown.target_heavy_before = old_summary.heavy_count[target];
            best_breakdown.target_heavy_after = new_summary.heavy_count[target];
            const double min_active_gain = std::max(
                best_cluster_cost * 0.05, old_summary.max_active * config_.rebalance_min_gain);
            const double min_score =
                std::max(0.01, config_.rebalance_min_gain * old_summary.objective * 0.25);
            best_breakdown.valid =
                best_breakdown.active_gain > min_active_gain && best_breakdown.score >= min_score;
        }
    }
    if (source == SIZE_MAX || cluster == SIZE_MAX || target == SIZE_MAX || !best_breakdown.valid ||
        best_cluster_cost <= 0.0) {
        return false;
    }

    const uint64_t progress = dynamicActorProgress_(cluster);
    const uint64_t fence = saturatingCycleAdd(progress, 1);

    std::string names;
    if (cluster < clusters_.clusters.size()) {
        names = buildUnitNameList_(clusters_.clusters[cluster]);
    } else if (clock_mode_) {
        names = "CDC bridge " + std::to_string(cluster - clusters_.numClusters());
    }
    std::string rebalance_detail =
        "rebalance=" + std::to_string(rebalance_count_ + 1) + " C" + std::to_string(cluster) + "(" +
        names + ") T" + std::to_string(source) + "->T" + std::to_string(target) +
        " fence=" + std::to_string(fence) + " src_active=" + std::to_string(thread_cost[source]) +
        " dst_active=" + std::to_string(thread_cost[target]) +
        " score=" + std::to_string(best_breakdown.score) +
        " obj_gain=" + std::to_string(best_breakdown.objective_gain) +
        " active_gain=" + std::to_string(best_breakdown.active_gain) +
        " topology_delta=" + std::to_string(best_breakdown.topology_delta) +
        " dep_bonus=" + std::to_string(best_breakdown.measured_dep_bonus) +
        " floor_bonus=" + std::to_string(best_breakdown.floor_slack_bonus) +
        " dep_penalty=" + std::to_string(best_breakdown.target_dep_penalty) +
        " stack_penalty=" + std::to_string(best_breakdown.active_stack_penalty) +
        " churn_penalty=" + std::to_string(best_breakdown.churn_penalty) +
        " new_max_active=" + std::to_string(best_breakdown.new_max_active) +
        " target_active_after=" + std::to_string(best_breakdown.target_active_after) +
        " active_budget=" + std::to_string(best_breakdown.active_budget) +
        " target_heavy_before=" + std::to_string(best_breakdown.target_heavy_before) +
        " target_heavy_after=" + std::to_string(best_breakdown.target_heavy_after) +
        " dst_clusters=" + std::to_string(thread_active_clusters[target]) +
        " dst_floor_wait_ns=" + std::to_string(thread_floor_wait[target]) +
        " dst_dep_wait_ns=" + std::to_string(thread_dep_wait[target]) +
        " dst_no_ready_wait_ns=" + std::to_string(thread_no_ready_wait[target]) +
        " cluster_blocked_wait_ns=" + std::to_string(cluster_blocked_wait[cluster]) +
        " cluster_blocker_wait_ns=" + std::to_string(cluster_blocker_wait[cluster]);

    // The planner ran without disturbing peer workers. Reject a stale plan,
    // then reserve the request slot and publish only the complete handoff.
    if (cluster_assignment_generation_.load(std::memory_order_acquire) != prepared.generation() ||
        cluster_runtime_owner_[cluster].load(std::memory_order_acquire) != source ||
        cluster_migration_pending_[cluster].load(std::memory_order_acquire) != 0) {
        return false;
    }
    uint8_t expected = none;
    if (!migration_request_.state.compare_exchange_strong(
            expected, static_cast<uint8_t>(MigrationRequestState::Requested),
            std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return false;
    }
    if (cluster_assignment_generation_.load(std::memory_order_acquire) != prepared.generation() ||
        cluster_runtime_owner_[cluster].load(std::memory_order_acquire) != source ||
        cluster_migration_pending_[cluster].load(std::memory_order_acquire) != 0) {
        clearDynamicMigrationRequest_();
        return false;
    }
    last_rebalance_detail_ = std::move(rebalance_detail);

    if (observe_ctx_) {
        observe::log_info<"Dynamic rebalance requested: cluster {} [{}] T{} -> T{} fence={}">(
            observe_ctx_, cluster, names.c_str(), source, target, fence);
    }
    recordDynamicSchedulerMarker_("Chronon epoch-free rebalance requested", cycle,
                                  last_rebalance_detail_);

    if (benefit) {
        benefit->before_ns_per_cycle = benefit->rate(cycle, planning_started);
        benefit->window_cycle = benefit->pending_cycle = cycle;
        benefit->window_ns = planning_started;
        benefit->pending_generation = planning_generation;
        benefit->pending = true;
        published = true;
        clock_parallel_->migration_requested_ns.store(detail::MigrationBenefit::now(),
                                                      std::memory_order_relaxed);
    }
    migration_request_.cluster.store(cluster, std::memory_order_relaxed);
    migration_request_.source_thread.store(source, std::memory_order_relaxed);
    migration_request_.target_thread.store(target, std::memory_order_relaxed);
    migration_request_.fence_cycle.store(fence, std::memory_order_release);
    cluster_migration_pending_[cluster].store(1, std::memory_order_release);
    migration_request_.state.store(static_cast<uint8_t>(MigrationRequestState::Quiescing),
                                   std::memory_order_release);

    return true;
}

}  // namespace chronon::sender

namespace chronon::sender::epoch_free_cost {
namespace {
[[gnu::noinline]] std::vector<size_t> improveSmallPlacement(const PartitionInput& input,
                                                            std::vector<size_t> assignment,
                                                            size_t num_threads) {
    const SmallPlacementCost prepared(input, num_threads);
    auto best = prepared.evaluate(assignment);
    for (size_t pass = 0; pass < 3; ++pass) {
        size_t best_unit = SIZE_MAX, best_target = SIZE_MAX;
        auto best_candidate = best;
        for (size_t u = 0; u < input.num_units; ++u) {
            const size_t from = assignment[u];
            for (size_t target = 0; target < num_threads; ++target) {
                if (target == from || moveWouldSplitZeroDelay(input, assignment, u, target))
                    continue;
                assignment[u] = target;
                const auto candidate = prepared.evaluate(assignment);
                assignment[u] = from;
                if (candidate.active_threads < best.active_threads) continue;
                if (candidate.max_active > best.max_active + 0.01 &&
                    candidate.max_incoming_pressure >= best.max_incoming_pressure - 0.01 &&
                    candidate.cross_pressure >= best.cross_pressure - 0.01)
                    continue;
                if (candidate.objective < best_candidate.objective - 0.01) {
                    best_candidate = candidate;
                    best_unit = u;
                    best_target = target;
                }
            }
        }
        if (best_unit == SIZE_MAX) break;
        assignment[best_unit] = best_target;
        best = best_candidate;
    }
    return assignment;
}

[[gnu::noinline]] std::vector<size_t> improveLargePlacement(const PartitionInput& input,
                                                            std::vector<size_t> assignment,
                                                            size_t num_threads) {
    if (input.num_units <= 1 || num_threads <= 1 || input.sync_cost_ns <= 0.0) return assignment;
    PreparedTopologyCost prepared;
    prepared.prepare(input, assignment);
    ObjectiveSummary candidate;
    for (size_t pass = 0; pass < 3; ++pass) {
        const auto& best = prepared.baseline();
        double best_objective = best.objective;
        size_t best_unit = SIZE_MAX, best_target = SIZE_MAX;
        for (size_t u = 0; u < input.num_units; ++u) {
            for (size_t target = 0; target < num_threads; ++target) {
                if (target == assignment[u] || prepared.splitsZeroDelay(u, target)) continue;
                prepared.evaluateMove(candidate, u, target);
                const double error = prepared.roundoff();
                if (std::abs(candidate.max_active - best.max_active - 0.01) <= error ||
                    std::abs(candidate.max_incoming_pressure - best.max_incoming_pressure + 0.01) <=
                        error ||
                    std::abs(candidate.cross_pressure - best.cross_pressure + 0.01) <= error ||
                    std::abs(candidate.objective - best_objective + 0.01) <= error)
                    prepared.evaluateFull(candidate, u, target);
                if (candidate.active_threads < best.active_threads) continue;
                if (candidate.max_active > best.max_active + 0.01 &&
                    candidate.max_incoming_pressure >= best.max_incoming_pressure - 0.01 &&
                    candidate.cross_pressure >= best.cross_pressure - 0.01)
                    continue;
                if (candidate.objective < best_objective - 0.01) {
                    // Keep the incumbent exact, so subsequent near ties use the
                    // same deterministic comparisons as the original search.
                    prepared.evaluateFull(candidate, u, target);
                    if (candidate.objective >= best_objective - 0.01) continue;
                    best_objective = candidate.objective;
                    best_unit = u;
                    best_target = target;
                }
            }
        }
        if (best_unit == SIZE_MAX) break;
        assignment[best_unit] = best_target;
        prepared.resetAssignment();
    }
    return assignment;
}

}  // namespace

std::vector<size_t> improvePreparedPlacement(const PartitionInput& input,
                                             std::vector<size_t> assignment, size_t num_threads) {
    if (input.num_units <= 1 || num_threads <= 1 || input.sync_cost_ns <= 0.0) return assignment;
    if (input.num_units <= SmallPlacementCost::kCapacity &&
        num_threads <= SmallPlacementCost::kCapacity)
        return improveSmallPlacement(input, std::move(assignment), num_threads);
    return improveLargePlacement(input, std::move(assignment), num_threads);
}

}  // namespace chronon::sender::epoch_free_cost
