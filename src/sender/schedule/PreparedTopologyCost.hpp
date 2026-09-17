// SPDX-License-Identifier: MPL-2.0
#pragma once

#include "EpochFreeTopologyCost.hpp"

namespace chronon::sender::epoch_free_cost {

// A planning-pass snapshot. The input, assignment and wait samples must remain
// alive and unchanged until the next prepare()/resetAssignment(). The runtime
// planner owns that storage under its exclusive planning guard and validates
// generation() again before publishing a migration.
class PreparedTopologyCost {
public:
    void prepare(const PartitionInput& input, const std::vector<size_t>& assignment,
                 const RuntimeWaits& waits = {}, uint64_t generation = 0) {
        input_ = &input;
        assignment_ = &assignment;
        waits_ = waits;
        generation_ = generation;
        avg_dep_wait_ = averageDependencyWait(waits);
        edges_.clear();
        incoming_.resize(input.num_units);
        outgoing_.resize(input.num_units);
        for (auto& edges : incoming_) edges.clear();
        for (auto& edges : outgoing_) edges.clear();
        incident_.assign(input.num_units, 0.0);
        effective_.resize(input.num_units);
        heavy_.clear();
        pairs_.clear();
        for (size_t u = 0; u < input.num_units; ++u) {
            if (u >= input.adjacency.size()) continue;
            for (const auto& edge : input.adjacency[u]) {
                const double pressure = edgePressure(input, edge);
                incident_[u] += pressure;
                if (edge.neighbor >= input.num_units) continue;
                incident_[edge.neighbor] += pressure;
                outgoing_[u].push_back(edges_.size());
                incoming_[edge.neighbor].push_back(edges_.size());
                edges_.push_back({u, edge.neighbor, pressure, edge.min_delay == 0});
            }
        }
        double total_effective = 0.0;
        for (size_t u = 0; u < input.num_units; ++u) {
            effective_[u] = cost(u) + 0.02 * std::sqrt(incident_[u]);
            total_effective += effective_[u];
        }
        const double average = input.num_units ? total_effective / input.num_units : 0.0;
        for (size_t u = 0; u < input.num_units; ++u)
            if (effective_[u] > average * 1.25) heavy_.push_back(u);
        std::sort(heavy_.begin(), heavy_.end(), [&](size_t a, size_t b) {
            return effective_[a] != effective_[b] ? effective_[a] > effective_[b] : a < b;
        });
        if (heavy_.size() > input.num_threads * 2) heavy_.resize(input.num_threads * 2);
        for (size_t i = 0; i < heavy_.size(); ++i) {
            for (size_t j = i + 1; j < heavy_.size(); ++j) {
                const size_t a = heavy_[i], b = heavy_[j];
                const double protection =
                    1.0 + directPressureBetween(input, a, b) / std::max(1.0, average);
                pairs_.push_back(
                    {a, b, 0.20 * std::min(effective_[a], effective_[b]) / protection});
            }
        }
        resetAssignment();
    }

    uint64_t generation() const noexcept { return generation_; }
    const ObjectiveSummary& baseline() const noexcept { return baseline_; }

    void resetAssignment() {
        evaluateFull(baseline_);
        total_active_ = 0.0;
        safe_incremental_ = input_->num_threads > 0;
        positive_count_.assign(input_->num_threads, 0);
        for (size_t u = 0; u < input_->num_units; ++u) {
            const size_t owner = (*assignment_)[u];
            if (owner >= input_->num_threads || cost(u) < 0.0 || !std::isfinite(cost(u))) {
                safe_incremental_ = false;
                continue;
            }
            total_active_ += cost(u);
            if (cost(u) > 0.0) ++positive_count_[owner];
        }
        double all_pressure = 0.0, all_pair_weights = 0.0;
        for (const auto& edge : edges_) {
            if (edge.pressure < 0.0 || !std::isfinite(edge.pressure)) safe_incremental_ = false;
            all_pressure += std::abs(edge.pressure);
        }
        for (const auto& pair : pairs_) all_pair_weights += std::abs(pair.weight);
        // Conservative error envelope for nonnegative accumulation and worker
        // reductions. Near any decision boundary use the ordered full path.
        roundoff_ =
            128.0 * std::numeric_limits<double>::epsilon() *
            (input_->num_units + edges_.size() + pairs_.size() + input_->num_threads + 1) *
            std::max({1.0, total_active_ + all_pressure + all_pair_weights, baseline_.objective});
        if (!std::isfinite(roundoff_)) safe_incremental_ = false;
    }
    double roundoff() const noexcept { return roundoff_; }

    size_t retainedBytes() const noexcept {
        const auto bytes = [](const auto& values) { return values.capacity() * sizeof(values[0]); };
        size_t result = sizeof(*this) + bytes(edges_) + bytes(incoming_) + bytes(outgoing_) +
                        bytes(incident_) + bytes(effective_) + bytes(heavy_) + bytes(pairs_) +
                        bytes(positive_count_);
        for (const auto& edges : incoming_) result += bytes(edges);
        for (const auto& edges : outgoing_) result += bytes(edges);
        for (const auto* summary : {&baseline_, &candidate_})
            result += bytes(summary->active) + bytes(summary->incoming_pressure) +
                      bytes(summary->heavy_count);
        return result;
    }

    void evaluateMove(ObjectiveSummary& out, size_t moved, size_t target) const {
        const size_t source = (*assignment_)[moved];
        if (!safe_incremental_ || target >= input_->num_threads || target == source) {
            evaluateFull(out, moved, target);
            return;
        }
        out = baseline_;  // Worker-sized storage only; capacities are retained.
        out.active[source] -= cost(moved);
        out.active[target] += cost(moved);
        const size_t remaining = positive_count_[source] - (cost(moved) > 0.0 ? 1 : 0);
        if (remaining == 0)
            out.active[source] = 0.0;
        else if (out.active[source] <= roundoff_) {
            evaluateFull(out, moved, target);
            return;
        }
        const auto update = [&](const Edge& edge) {
            if (edge.from == edge.to) return;
            const size_t old_from = (*assignment_)[edge.from], old_to = (*assignment_)[edge.to];
            const size_t new_from = edge.from == moved ? target : old_from;
            const size_t new_to = edge.to == moved ? target : old_to;
            if (old_from != old_to) {
                out.cross_pressure -= edge.pressure;
                out.incoming_pressure[old_to] -= edge.pressure;
            }
            if (new_from != new_to) {
                out.cross_pressure += edge.pressure;
                out.incoming_pressure[new_to] += edge.pressure;
            }
        };
        for (size_t index : outgoing_[moved]) update(edges_[index]);
        for (size_t index : incoming_[moved])
            if (edges_[index].from != moved) update(edges_[index]);
        if (std::find(heavy_.begin(), heavy_.end(), moved) != heavy_.end()) {
            --out.heavy_count[source];
            ++out.heavy_count[target];
            for (const auto& pair : pairs_) {
                if (pair.a != moved && pair.b != moved) continue;
                const size_t other = (*assignment_)[pair.a == moved ? pair.b : pair.a];
                if (other == source) out.heavy_colocation_penalty -= pair.weight;
                if (other == target) out.heavy_colocation_penalty += pair.weight;
            }
        }
        finish(out, total_active_);
    }

    MoveBreakdown scoreMove(size_t cluster, size_t target, double min_gain, double churn) {
        if (cluster >= input_->num_units || target >= input_->num_threads ||
            (*assignment_)[cluster] == target)
            return {};
        evaluateMove(candidate_, cluster, target);
        bool uncertain = false;
        const auto result =
            scoreCandidate(cluster, target, min_gain, churn, candidate_, &uncertain);
        return uncertain ? scoreFull(cluster, target, min_gain, churn) : result;
    }

    // Preserve the full oracle's summation order while caching the graph-only
    // terms. A candidate overrides one owner without copying the assignment.
    void evaluateFull(ObjectiveSummary& out, size_t moved = SIZE_MAX,
                      size_t target = SIZE_MAX) const {
        reset(out);
        const size_t threads = input_->num_threads;
        if (input_->num_units == 0 || threads == 0) return;
        const auto owner = [&](size_t u) { return u == moved ? target : (*assignment_)[u]; };
        double total = 0.0;
        for (size_t u = 0; u < input_->num_units; ++u) {
            const size_t t = owner(u);
            if (t >= threads) continue;
            out.active[t] += cost(u);
            total += cost(u);
        }
        for (const auto& edge : edges_) {
            const size_t to = owner(edge.to);
            if (owner(edge.from) == to) continue;
            out.cross_pressure += edge.pressure;
            if (to < threads) out.incoming_pressure[to] += edge.pressure;
        }
        for (const size_t u : heavy_)
            if (owner(u) < threads) ++out.heavy_count[owner(u)];
        for (const auto& pair : pairs_)
            if (owner(pair.a) == owner(pair.b)) out.heavy_colocation_penalty += pair.weight;
        finish(out, total);
    }

    bool splitsZeroDelay(size_t unit, size_t target) const {
        for (size_t index : outgoing_[unit]) {
            const auto& edge = edges_[index];
            if (edge.zero_delay && (*assignment_)[edge.to] != target) return true;
        }
        for (size_t index : incoming_[unit]) {
            const auto& edge = edges_[index];
            if (edge.zero_delay && (*assignment_)[edge.from] != target) return true;
        }
        return false;
    }

    MoveBreakdown scoreFull(size_t cluster, size_t target, double min_gain, double churn) {
        if (cluster >= input_->num_units || target >= input_->num_threads ||
            (*assignment_)[cluster] == target)
            return {};
        evaluateFull(candidate_, cluster, target);
        return scoreCandidate(cluster, target, min_gain, churn, candidate_);
    }

private:
    struct Edge {
        size_t from, to;
        double pressure;
        bool zero_delay;
    };
    struct HeavyPair {
        size_t a, b;
        double weight;
    };
    double cost(size_t u) const {
        return u < input_->unit_cost_ns.size() ? input_->unit_cost_ns[u] : 1.0;
    }
    void reset(ObjectiveSummary& out) const {
        out.objective = out.max_active = out.cross_pressure = out.max_incoming_pressure = 0.0;
        out.heavy_colocation_penalty = out.idle_thread_penalty = 0.0;
        out.active_threads = 0;
        const size_t threads = input_->num_units ? input_->num_threads : 0;
        out.active.assign(threads, 0.0);
        out.incoming_pressure.assign(threads, 0.0);
        out.heavy_count.assign(threads, 0);
    }
    void finish(ObjectiveSummary& out, double total) const {
        const size_t threads = input_->num_threads;
        out.max_active = *std::max_element(out.active.begin(), out.active.end());
        out.max_incoming_pressure =
            *std::max_element(out.incoming_pressure.begin(), out.incoming_pressure.end());
        const double average = total / static_cast<double>(threads);
        double balance = 0.0;
        out.active_threads = 0;
        for (double active : out.active) {
            if (active > 0.0) ++out.active_threads;
            const double diff = active - average;
            balance += diff * diff / std::max(1.0, average);
        }
        out.idle_thread_penalty = out.active_threads < threads
                                      ? 0.25 * total *
                                            static_cast<double>(threads - out.active_threads) /
                                            static_cast<double>(threads)
                                      : 0.0;
        out.objective = out.max_active + 0.05 * balance + 0.35 * out.max_incoming_pressure +
                        0.12 * out.cross_pressure + out.heavy_colocation_penalty +
                        out.idle_thread_penalty;
    }
    double localCross(size_t cluster, size_t thread) const {
        double pressure = 0.0;
        for (size_t i : outgoing_[cluster]) {
            const auto& edge = edges_[i];
            if (edge.to != cluster && (*assignment_)[edge.to] != thread) pressure += edge.pressure;
        }
        for (size_t i : incoming_[cluster]) {
            const auto& edge = edges_[i];
            if (edge.from != cluster && (*assignment_)[edge.from] != thread)
                pressure += edge.pressure;
        }
        return pressure;
    }
    MoveBreakdown scoreCandidate(size_t cluster, size_t target, double min_gain, double churn,
                                 const ObjectiveSummary& candidate,
                                 bool* uncertain = nullptr) const {
        const size_t source = (*assignment_)[cluster];
        return scoreSummaries(
            cost(cluster), cluster, source, target, input_->num_threads, waits_, min_gain, churn,
            baseline_, candidate, localCross(cluster, source) - localCross(cluster, target),
            avg_dep_wait_, roundoff_ * std::max(1.0, std::abs(min_gain)), uncertain);
    }

    const PartitionInput* input_ = nullptr;
    const std::vector<size_t>* assignment_ = nullptr;
    RuntimeWaits waits_;
    uint64_t generation_ = 0;
    double avg_dep_wait_ = 0.0;
    double total_active_ = 0.0, roundoff_ = 0.0;
    bool safe_incremental_ = false;
    std::vector<size_t> positive_count_;
    std::vector<Edge> edges_;
    std::vector<std::vector<size_t>> incoming_, outgoing_;
    std::vector<double> incident_, effective_;
    std::vector<size_t> heavy_;
    std::vector<HeavyPair> pairs_;
    ObjectiveSummary baseline_, candidate_;
};

inline std::vector<size_t> improvePreparedPlacement(const PartitionInput& input,
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

}  // namespace chronon::sender::epoch_free_cost
