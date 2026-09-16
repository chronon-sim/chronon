// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include "TickSimulation.hpp"

namespace chronon::sender {

// A bridge is a scheduled actor, not a dependency between whole clock domains.
// begin grants endpoint edges; their completion hands pending commands back to
// commit. Only a fully committed bridge may transfer ownership.
struct TickSimulation::ClockParallelRuntime {
    struct alignas(64) Cluster {
        const ClockDomain* clock = nullptr;
        std::atomic<uint64_t> allowed{0};
        std::vector<const std::atomic<uint64_t>*> bridges;
    };
    struct alignas(64) Endpoint {
        size_t cluster = 0;
        const ClockDomain* clock = nullptr;
        uint64_t next = 0;  // Bridge-owner private.
        std::atomic<uint64_t> prepared{0};
        std::atomic<uint64_t> completed{0};
    };
    struct Bridge {
        CdcComponent* circuit = nullptr;
        std::array<Endpoint, 2> endpoints;
        std::array<ClockEdge, 2> edges;
        std::array<bool, 2> participating{};
        std::array<observe::ClockTraceStream*, 2> trace{};
        size_t edge_count = 0;
        std::atomic<uint64_t> completed{0};  // Committed merged-edge transactions.
        bool sample = false;
        uint64_t sample_ns = 0;  // begin + commit CPU work, excluding dependency waits.
    };
    struct Batch {
        SimTime time;
        std::vector<ClockEdge> edges;
    };

    std::unique_ptr<Cluster[]> clusters;
    std::vector<std::unique_ptr<Bridge>> bridges;
    std::vector<std::vector<size_t>> worker_bridges;
    std::deque<Batch> pending;
    // Migration heuristics use reference-clock cycles of retired physical time,
    // never incomparable actor-local cycles or calendar batch counts.
    std::atomic<uint64_t> rebalance_cycle{0};
    std::vector<double> actor_rates;
};

}  // namespace chronon::sender
