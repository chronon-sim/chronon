// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include "TickSimulation.hpp"

namespace chronon::sender {

// A bridge is a scheduled actor, not a dependency between whole clock domains.
// begin grants endpoint edges; their completion hands pending commands back to
// commit. Only a fully committed bridge may transfer ownership.
struct TickSimulation::ClockParallelRuntime {
    struct Domain {
        const ClockDomain* clock = nullptr;
        ClockRuntime* serial = nullptr;
        uint64_t completion_sweep = 0, acquired_completed = 0;  // Coordinator-private.
        std::atomic<uint64_t> allowed{0};
        std::atomic<uint64_t> retired{0};
        std::vector<const std::atomic<uint64_t>*> completions;
    };
    struct Dependency {
        size_t actor;
        const std::atomic<uint64_t>* progress;
    };
    struct alignas(64) Cluster {
        const ClockDomain* clock = nullptr;
        Domain* domain = nullptr;
        uint64_t sample_interval = detail::kDynamicTickSampleInterval;
        std::vector<Dependency> bridges;
    };
    struct alignas(64) Endpoint {
        size_t cluster = 0;
        const ClockDomain* clock = nullptr;
        uint64_t next = 0;  // Bridge-owner private.
        SimTime next_time;  // Cached successor, not recomputed on unsuccessful readiness polls.
        std::atomic<uint64_t> prepared{0};
        std::atomic<uint64_t> completed{0};
    };
    struct Lane {
        CdcComponent* circuit = nullptr;
        std::array<observe::ClockTraceStream*, 2> trace{};
    };
    struct Bridge {
        std::vector<Lane> lanes;
        std::array<Endpoint, 2> endpoints;
        std::array<ClockEdge, 2> edges;
        std::array<bool, 2> participating{};
        size_t edge_count = 0;
        std::atomic<uint64_t> completed{0};  // Committed merged-edge transactions.
        bool sample = false;
        uint64_t sample_ns = 0;  // begin + commit execution time, excluding dependency waits.
    };
    struct BatchEdge {
        Domain* domain;
        uint64_t cycle;
    };
    struct Batch {
        SimTime time;
        std::vector<BatchEdge> edges;
    };

    std::unique_ptr<Cluster[]> clusters;
    std::unordered_map<ClockDomainId, Domain> domains;
    std::vector<std::unique_ptr<Bridge>> bridges;
    std::vector<std::vector<size_t>> worker_bridges;
    // Bounded reusable ring: only the coordinator accesses slots. Pop does not
    // free edge storage. Dense references avoid hardware-ID lookup at retirement.
    std::vector<Domain*> indexed_domains;
    std::vector<Batch> pending;
    size_t pending_head = 0, pending_size = 0;
    uint64_t coordinator_sweep = 0;
    Batch& pendingAt(size_t index) { return pending[(pending_head + index) % pending.size()]; }
    // Migration heuristics use reference-clock cycles of retired physical time,
    // never incomparable actor-local cycles or calendar batch counts.
    std::atomic<uint64_t> rebalance_cycle{0};
    std::vector<double> actor_rates;
};

}  // namespace chronon::sender
