// SPDX-License-Identifier: MPL-2.0
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>

#include "ClockAllocationCount.hpp"
#include "sender/schedule/PreparedTopologyCost.hpp"

int main(int argc, char** argv) {
    if (argc != 6) {
        std::cerr << "usage: placement_scoring_benchmark oracle|prepared|incremental UNITS THREADS "
                     "SKEW PASSES\n";
        return 2;
    }
    using namespace chronon::sender;
    using namespace chronon::sender::epoch_free_cost;
    using chronon::benchmark::clockAllocations;
    const std::string mode = argv[1];
    const size_t units = std::stoull(argv[2]), threads = std::stoull(argv[3]);
    const size_t skew = std::stoull(argv[4]), passes = std::stoull(argv[5]);
    if (!units || units > 100000 || threads < 2 || threads > 64 || !passes) return 2;
    if (mode != "oracle" && mode != "prepared" && mode != "incremental") return 2;
    std::mt19937_64 random(143);
    PartitionInput input;
    input.num_units = units;
    input.num_threads = threads;
    input.sync_cost_ns = 8.0;
    input.adjacency.resize(units);
    input.unit_cost_ns.resize(units);
    std::vector<size_t> assignment(units);
    for (size_t u = 0; u < units; ++u) {
        input.unit_cost_ns[u] = (10.0 + (random() % 100) / 7.0) * (u % 7 ? 1 : skew);
        assignment[u] = random() % threads;
        for (size_t e = 0; e < std::min(units, size_t{4}); ++e)
            input.adjacency[u].push_back(
                {random() % units, 1 + random() % 3, static_cast<uint32_t>(random() % 5), 1.0});
    }
    std::vector<uint64_t> floor(threads, 400), dep(threads, 100), ready(threads, 300);
    std::vector<uint64_t> blocked(units, 100), blocker(units, 200);
    RuntimeWaits waits{&floor, &dep, &ready, &blocked, &blocker};
    PreparedTopologyCost prepared;
    uint64_t prepare_ns = 0, score_ns = 0, prepare_allocations = 0, score_allocations = 0;
    uint64_t valid = 0, selection = 0;
    for (size_t pass = 0; pass < passes; ++pass) {
        auto allocations = clockAllocations();
        auto begin = std::chrono::steady_clock::now();
        if (mode != "oracle") prepared.prepare(input, assignment, waits, pass);
        auto end = std::chrono::steady_clock::now();
        prepare_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
        prepare_allocations += clockAllocations() - allocations;
        allocations = clockAllocations();
        begin = std::chrono::steady_clock::now();
        MoveBreakdown best;
        size_t chosen = SIZE_MAX, worker = SIZE_MAX;
        for (size_t u = 0; u < units; ++u) {
            for (size_t t = 0; t < threads; ++t) {
                if (t == assignment[u]) continue;
                auto result = mode == "oracle" ? scoreMove(input, assignment, u, t, waits, 0.01, 0)
                              : mode == "prepared" ? prepared.scoreFull(u, t, 0.01, 0)
                                                   : prepared.scoreMove(u, t, 0.01, 0);
                if (!result.valid) continue;
                ++valid;
                if (mode == "incremental" && result.score >= best.score - prepared.roundoff())
                    result = prepared.scoreFull(u, t, 0.01, 0);
                if (result.score > best.score || (result.score == best.score && u < chosen)) {
                    best = result;
                    chosen = u;
                    worker = t;
                }
            }
        }
        end = std::chrono::steady_clock::now();
        score_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
        score_allocations += clockAllocations() - allocations;
        selection = selection * 31 + chosen * threads + worker;
    }
    std::cout << "prepare_ns,score_ns,prepare_allocations,score_allocations,retained_bytes,valid,"
                 "selection\n"
              << prepare_ns << ',' << score_ns << ',' << prepare_allocations << ','
              << score_allocations << ',' << (mode == "oracle" ? 0 : prepared.retainedBytes())
              << ',' << valid << ',' << selection << '\n';
}
