// SPDX-License-Identifier: MPL-2.0
#include <cassert>
#include <iostream>
#include <random>

#include "sender/schedule/PreparedTopologyCost.hpp"

using namespace chronon::sender;
using namespace chronon::sender::epoch_free_cost;

static void equal(const ObjectiveSummary& a, const ObjectiveSummary& b) {
    assert(a.objective == b.objective);
    assert(a.max_active == b.max_active);
    assert(a.cross_pressure == b.cross_pressure);
    assert(a.max_incoming_pressure == b.max_incoming_pressure);
    assert(a.heavy_colocation_penalty == b.heavy_colocation_penalty);
    assert(a.idle_thread_penalty == b.idle_thread_penalty);
    assert(a.active_threads == b.active_threads);
    assert(a.active == b.active);
    assert(a.incoming_pressure == b.incoming_pressure);
    assert(a.heavy_count == b.heavy_count);
}

static void equal(const MoveBreakdown& a, const MoveBreakdown& b) {
    assert(a.valid == b.valid);
    assert(a.score == b.score);
    assert(a.objective_gain == b.objective_gain);
    assert(a.active_gain == b.active_gain);
    assert(a.topology_delta == b.topology_delta);
    assert(a.measured_dep_bonus == b.measured_dep_bonus);
    assert(a.floor_slack_bonus == b.floor_slack_bonus);
    assert(a.target_dep_penalty == b.target_dep_penalty);
    assert(a.active_stack_penalty == b.active_stack_penalty);
    assert(a.churn_penalty == b.churn_penalty);
    assert(a.old_max_active == b.old_max_active);
    assert(a.new_max_active == b.new_max_active);
    assert(a.old_target_dep_pressure == b.old_target_dep_pressure);
    assert(a.new_target_dep_pressure == b.new_target_dep_pressure);
    assert(a.target_active_after == b.target_active_after);
    assert(a.active_budget == b.active_budget);
    assert(a.target_heavy_before == b.target_heavy_before);
    assert(a.target_heavy_after == b.target_heavy_after);
}

static void close(double a, double b, double error) { assert(a == b || std::abs(a - b) <= error); }
static void close(const ObjectiveSummary& a, const ObjectiveSummary& b, double error) {
    close(a.objective, b.objective, error);
    close(a.max_active, b.max_active, error);
    close(a.cross_pressure, b.cross_pressure, error);
    close(a.max_incoming_pressure, b.max_incoming_pressure, error);
    close(a.heavy_colocation_penalty, b.heavy_colocation_penalty, error);
    close(a.idle_thread_penalty, b.idle_thread_penalty, error);
    assert(a.active_threads == b.active_threads);
    assert(a.heavy_count == b.heavy_count);
    for (size_t t = 0; t < a.active.size(); ++t) {
        close(a.active[t], b.active[t], error);
        close(a.incoming_pressure[t], b.incoming_pressure[t], error);
    }
}

static void newlyExposedPressure() {
    PartitionInput input{};
    input.num_units = 5;
    input.num_threads = 2;
    input.sync_cost_ns = 1.0;
    input.unit_cost_ns.assign(5, 1e9);
    input.adjacency.resize(5);
    for (size_t u = 0; u < 4; ++u) input.adjacency[u].push_back({4, 1, 1, 100.0});
    input.adjacency[4].push_back({0, 1, 1, std::ldexp(1.0, 60)});
    const std::vector<size_t> assignment(5, 0);
    PreparedTopologyCost prepared;
    prepared.prepare(input, assignment);
    assert(prepared.baseline().cross_pressure == 0.0);
    ObjectiveSummary full, incremental;
    prepared.evaluateFull(full, 4, 1);
    prepared.evaluateMove(incremental, 4, 1);
    // The incoming small edges precede the large outgoing edge in the full
    // sum, but follow it in the incremental sum. Baseline cross pressure alone
    // would give an error envelope many orders of magnitude too small.
    assert(full.cross_pressure != incremental.cross_pressure);
    close(full, incremental, prepared.roundoff());
    const auto score = prepared.scoreFull(4, 1, 0.01, 0.0);
    const double threshold = score.score + input.unit_cost_ns[4] * 0.25;
    for (double penalty :
         {std::nextafter(threshold, -INFINITY), threshold, std::nextafter(threshold, INFINITY)})
        equal(prepared.scoreFull(4, 1, 0.01, penalty), prepared.scoreMove(4, 1, 0.01, penalty));
}

int main() {
    newlyExposedPressure();
    std::mt19937_64 random(143);
    PreparedTopologyCost prepared;
    PartitionInput empty{};
    empty.num_threads = 2;
    const std::vector<size_t> no_assignment;
    prepared.prepare(empty, no_assignment);
    equal(summarize(empty, no_assignment, 2), prepared.baseline());
    for (size_t sample = 0; sample < 300; ++sample) {
        PartitionInput input;
        input.num_units = 1 + random() % 40;
        input.num_threads = 1 + random() % 8;
        input.sync_cost_ns = sample % 5 ? 0.25 + (random() % 100) / 7.0 : 0.0;
        input.adjacency.resize(input.num_units);
        std::vector<size_t> assignment(input.num_units);
        for (size_t u = 0; u < input.num_units; ++u) {
            assignment[u] = random() % input.num_threads;
            input.unit_cost_ns.push_back(sample % 7 ? (random() % 10000) / 13.0 : 1.0);
            if (sample % 11 == 0) input.unit_cost_ns.back() = 0;
            if (sample % 13 == 0 && u == 0) input.unit_cost_ns.back() = 1e8;
            for (size_t j = 0, n = random() % 6; j < n; ++j)
                input.adjacency[u].push_back({random() % input.num_units, 1 + random() % 5,
                                              static_cast<uint32_t>(random() % 5),
                                              (random() % 100) / 37.0});
        }
        if (sample % 19 == 0)
            for (auto& cost : input.unit_cost_ns) cost *= (sample % 2 ? 1e-12 : 1e12);
        if (sample % 17 == 0) input.unit_cost_ns.resize(input.num_units / 2);
        std::vector<uint64_t> floor(input.num_threads), dep(input.num_threads),
            ready(input.num_threads);
        std::vector<uint64_t> blocked(input.num_units), blocker(input.num_units);
        for (auto* values : {&floor, &dep, &ready, &blocked, &blocker})
            for (auto& value : *values) value = sample % 3 ? random() % 10000 : 0;
        RuntimeWaits waits{&floor, &dep, &ready, &blocked, &blocker};
        prepared.prepare(input, assignment, waits, sample);
        assert(prepared.generation() == sample);
        equal(summarize(input, assignment, input.num_threads), prepared.baseline());
        MoveBreakdown best_full, best_fast;
        size_t full_unit = SIZE_MAX, fast_unit = SIZE_MAX, full_target = SIZE_MAX,
               fast_target = SIZE_MAX;
        for (size_t u = 0; u < input.num_units; ++u) {
            for (size_t target = 0; target < input.num_threads; ++target) {
                auto candidate = assignment;
                candidate[u] = target;
                ObjectiveSummary summary;
                prepared.evaluateFull(summary, u, target);
                equal(summarize(input, candidate, input.num_threads), summary);
                ObjectiveSummary incremental;
                prepared.evaluateMove(incremental, u, target);
                close(summary, incremental, prepared.roundoff());
                assert(prepared.splitsZeroDelay(u, target) ==
                       moveWouldSplitZeroDelay(input, assignment, u, target));
                const double gain = sample % 4 ? 0.05 : 0.0;
                const double churn = sample % 9 ? 0.0 : 10.0;
                equal(scoreMove(input, assignment, u, target, waits, gain, churn),
                      prepared.scoreFull(u, target, gain, churn));
                const auto full = prepared.scoreFull(u, target, gain, churn);
                auto delta = prepared.scoreMove(u, target, gain, churn);
                assert(full.valid == delta.valid);
                close(full.score, delta.score, prepared.roundoff());
                if (full.valid && (full.score > best_full.score ||
                                   (full.score == best_full.score && u < full_unit))) {
                    best_full = full;
                    full_unit = u;
                    full_target = target;
                }
                if (delta.valid && delta.score >= best_fast.score - prepared.roundoff())
                    delta = prepared.scoreFull(u, target, gain, churn);
                if (delta.valid && (delta.score > best_fast.score ||
                                    (delta.score == best_fast.score && u < fast_unit))) {
                    best_fast = delta;
                    fast_unit = u;
                    fast_target = target;
                }
                if (std::isfinite(full.score)) {
                    const double threshold =
                        full.score + churn -
                        std::max(0.01, gain * prepared.baseline().objective * 0.25);
                    for (double penalty : {std::nextafter(threshold, -INFINITY), threshold,
                                           std::nextafter(threshold, INFINITY)}) {
                        const auto exact = prepared.scoreFull(u, target, gain, penalty);
                        const auto fast = prepared.scoreMove(u, target, gain, penalty);
                        assert(exact.valid == fast.valid);
                        close(exact.score, fast.score, prepared.roundoff());
                    }
                }
            }
        }
        assert(full_unit == fast_unit && full_target == fast_target);
        equal(best_full, best_fast);
        assert(improveInitialPlacement(input, assignment, input.num_threads) ==
               improvePreparedPlacement(input, assignment, input.num_threads));
        // Rebinding changes both sampled costs and owners, even if dimensions
        // stay the same. Nothing from the preceding generation may survive.
        assignment[0] = (assignment[0] + 1) % input.num_threads;
        input.sync_cost_ns += 0.5;
        prepared.prepare(input, assignment, {}, sample + 1);
        equal(summarize(input, assignment, input.num_threads), prepared.baseline());
    }
    std::cout << "Prepared topology scoring matches the full oracle\n";
}
