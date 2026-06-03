// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file synthetic_workload.hpp
///
/// Synthetic, tunable workload unit and topology generator for measuring the
/// *chronon framework itself* — scheduler partitioning, cross-thread progress
/// atomics, lookahead-floor scans, MPSC arbitration, epoch sync — decoupled
/// from the heavy real units (e.g. nucleus LSU/IFU, ~10us/tick) that otherwise
/// dominate wall time and hide framework overhead.
///
/// A `SyntheticUnit`'s per-tick cost is tunable along four axes:
///   - arithmetic intensity  : `arith_ops` integer ALU ops / tick (+ optional `fp_ops`)
///   - memory footprint       : `footprint_bytes` working set, `accesses_per_tick` lines walked
///   - number of units        : how many units the topology creates
///   - topology               : chain / islands / fan-out tree / 2D mesh / random DAG / feedback
///
/// MEMORY FOOTPRINT — pick realistic SRAM sizes. A real microarchitectural unit
/// holds its tables/caches/TLBs/queues in SRAM, typically tens of KB to a few MB
/// per unit, and that working set is meant to stay L1/L2/L3-resident. So the
/// per-tick memory cost comes from the scattered line touches (`accesses_per_tick`)
/// *within* a resident buffer, not from DRAM misses. Sensible ranges per unit:
///   ~4-64 KB   small tables / queues / TLBs (stays in L1/L2)
///   ~256 KB-2 MB  L2-class caches, large predictor tables (L2/L3-resident)
/// Only push `footprint_bytes` so the *aggregate* set exceeds the LLC if you
/// deliberately want to model a DRAM-bound unit — that is the exception, not the norm.
///
/// DETERMINISM CONTRACT (same parameters -> consistent results):
///   The integer accumulator `acc_` is a pure function of a unit's seed/params
///   and the per-cycle multiset of input messages. Messages are delivered at a
///   fixed *simulated* cycle (delay is a property of the edge, not the thread
///   schedule) and the framework drains MPSC inputs in deterministic conn_id
///   order. Therefore the XOR of every unit's `checksum()` is identical across
///   thread counts, lookahead on/off, and rebalance on/off — the benchmark uses
///   this to self-verify the framework's determinism guarantees. All state is
///   integer (bit-exact, cross-platform); the optional FP accumulator follows a
///   fixed per-unit op sequence (thread-count invariant within one build, but
///   its value is build-specific, so the portable checksum is the integer one).
///
/// DELAY SEMANTICS (how topology maps onto threads):
///   delay == 0  : tight coupling. The scheduler co-locates the endpoints in one
///                 cluster on one thread (intra-cluster sequential). Exercises
///                 ordering, but offers little parallelism.
///   delay >= 1  : the partitioner may split the endpoints across threads;
///                 lookahead depth is bounded by the minimum edge delay. This is
///                 the regime that actually stresses the cross-thread scheduler.

#pragma once

#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include "chronon/Chronon.hpp"

namespace synthetic {

using namespace chronon;

// LCG constants (Knuth / PCG multiplier). Used both for per-tick work and for
// deterministic seeding. A full-period LCG => the memory walk visits lines in a
// scattered, prefetcher-defeating order.
inline constexpr uint64_t kLcgMul = 6364136223846793005ULL;
inline constexpr uint64_t kLcgAdd = 1442695040888963407ULL;
inline constexpr uint64_t kGolden = 0x9E3779B97F4A7C15ULL;

// ============================================================================
// SyntheticUnit
// ============================================================================

struct SyntheticParams : public ParameterSet {
    Param<uint32_t> arith_ops{this, "arith_ops", 8, "Integer ALU ops per tick"};
    Param<uint32_t> fp_ops{this, "fp_ops", 0, "Floating-point FMA ops per tick (0 = off)"};
    Param<uint64_t> footprint_bytes{this, "footprint_bytes", 4096,
                                    "Per-unit SRAM working-set bytes (realistic: ~4KB-2MB)"};
    Param<uint32_t> accesses_per_tick{this, "accesses_per_tick", 0,
                                      "Cache lines touched per tick (scattered RMW walk)"};
    Param<uint64_t> seed{this, "seed", 0, "Per-unit seed (the topology builder overrides per id)"};
};

/// A single synthetic workload unit with one fan-in InPort and one fan-out
/// OutPort. Because OutPort::send() broadcasts to every connection and InPort
/// accepts many producers (MPSC), one port pair expresses arbitrary topology.
class SyntheticUnit : public AutoRegisteredUnit<SyntheticUnit> {
public:
    using ParameterSet = SyntheticParams;
    static constexpr const char* unit_type_name = "SyntheticUnit";
    static constexpr const char* unit_description = "Tunable synthetic workload unit";

    InPort<uint64_t> in{this, "in"};
    OutPort<uint64_t> out{this, "out"};

    // Factory ctor (const ParameterSet*) delegates to the direct ctor below.
    CHRONON_UNIT_CONSTRUCTOR(SyntheticUnit, ParameterSet, params->seed, params->arith_ops,
                             params->fp_ops, params->footprint_bytes, params->accesses_per_tick)
    (uint64_t seed = 0, uint32_t arith_ops = 8, uint32_t fp_ops = 0,
     uint64_t footprint_bytes = 4096, uint32_t accesses_per_tick = 0, std::string name = {})
        : AutoRegisteredUnit(name.empty() ? ("syn_" + std::to_string(seed)) : std::move(name)),
          arith_ops_(arith_ops),
          fp_ops_(fp_ops),
          accesses_per_tick_(accesses_per_tick),
          acc_(0x1234567ULL + seed * 2654435761ULL),
          walk_state_(seed | 1ULL),  // odd => LCG full period
          // Seed-derived FP start in [1.0, 2.0) so each unit's FP accumulator is
          // distinct (otherwise an FP loop independent of unit state would make
          // every unit identical and the XOR-folded FP checksum cancel to 0).
          fp_acc_(1.0 + static_cast<double>(seed & 0xFFFFULL) / 65536.0) {
        // Working set: footprint rounded to whole 64-byte cache lines (>= 1).
        n_lines_ = std::max<uint64_t>(1, footprint_bytes / 64);
        buffer_.resize(n_lines_ * kLanesPerLine);
        // Deterministic, seed-derived fill so the RMW reads run-independent data.
        uint64_t f = seed * kGolden + 0xABCDEFULL;
        for (auto& w : buffer_) {
            f = f * kLcgMul + kLcgAdd;
            w = f;
        }
    }

    void tick() override {
        // (1) Consume all ready inputs. MPSC staging was drained in conn_id
        //     order by executeTick() before tick(); XOR is order-independent.
        for (uint64_t v : in.receiveAllBuffered(localCycle())) {
            acc_ ^= v;
        }

        // (2) Memory walk: scattered read-modify-write across the unit's SRAM-
        //     sized buffer. The store-back keeps it non-elidable; cost scales with
        //     accesses_per_tick and (via cache residency) with footprint_bytes.
        uint64_t w = walk_state_;
        for (uint32_t i = 0; i < accesses_per_tick_; ++i) {
            w = w * kLcgMul + kLcgAdd;
            uint64_t line = (w >> 16) % n_lines_;
            uint64_t idx = line * kLanesPerLine + ((w >> 3) & (kLanesPerLine - 1));
            uint64_t r = buffer_[idx];
            r = r * kLcgMul + (acc_ ^ w);
            buffer_[idx] = r;
            acc_ += r;
        }
        walk_state_ = w;

        // (3) Integer arithmetic: loop-carried LCG (the "arithmetic intensity").
        uint64_t a = acc_;
        for (uint32_t i = 0; i < arith_ops_; ++i) {
            a = a * kLcgMul + kLcgAdd;
        }
        acc_ = a;

        // (4) Optional floating-point arithmetic (kept out of the integer checksum).
        if (fp_ops_ != 0) {
            double f = fp_acc_;
            for (uint32_t i = 0; i < fp_ops_; ++i) {
                f = std::fma(f, 1.0000000001, 1e-9);
            }
            fp_acc_ = f;
        }

        // (5) Emit. send() broadcasts the same value to all connections; on a
        //     unit with no out-edges it is a no-op.
        if (out.canSend()) {
            (void)out.send(acc_);
        }
        ++ticks_;
    }

    /// Portable, bit-exact checksum source (the determinism guarantee).
    uint64_t checksum() const { return acc_; }
    /// FP checksum (thread-count invariant within a build; value is build-specific).
    uint64_t fpChecksum() const { return std::bit_cast<uint64_t>(fp_acc_); }
    uint64_t ticks() const { return ticks_; }

private:
    static constexpr uint64_t kLanesPerLine = 8;  // 8 * sizeof(uint64_t) = 64 bytes

    std::vector<uint64_t> buffer_;
    uint64_t n_lines_ = 1;
    uint32_t arith_ops_;
    uint32_t fp_ops_;
    uint32_t accesses_per_tick_;
    uint64_t acc_;
    uint64_t walk_state_;
    double fp_acc_ = 1.0;
    uint64_t ticks_ = 0;
};

// ============================================================================
// Topology generator
// ============================================================================

struct WorkloadKnobs {
    uint32_t arith_ops = 8;
    uint32_t fp_ops = 0;
    uint64_t footprint_bytes = 4096;
    uint32_t accesses_per_tick = 0;
};

struct TopologySpec {
    enum class Kind { Chain, Islands, FanoutTree, Mesh2D, RandomDag, Feedback };
    Kind kind = Kind::Islands;
    uint32_t num_units = 64;
    uint32_t fanout = 2;  ///< tree branching / island cluster size / DAG max out-degree
    uint32_t grid_w = 0;  ///< mesh width  (0 => derived from sqrt(num_units))
    uint32_t grid_h = 0;  ///< mesh height (0 => derived)
    uint32_t delay = 1;   ///< edge delay (0 = tight coupling; >=1 = partitionable)
    uint64_t seed = 0xC0FFEEULL;
};

struct Edge {
    uint32_t src;
    uint32_t dst;
    uint32_t delay;
};

inline const char* topologyKindName(TopologySpec::Kind k) {
    switch (k) {
        case TopologySpec::Kind::Chain:
            return "chain";
        case TopologySpec::Kind::Islands:
            return "islands";
        case TopologySpec::Kind::FanoutTree:
            return "fanout";
        case TopologySpec::Kind::Mesh2D:
            return "mesh";
        case TopologySpec::Kind::RandomDag:
            return "dag";
        case TopologySpec::Kind::Feedback:
            return "feedback";
    }
    return "?";
}

inline bool parseTopologyKind(const std::string& s, TopologySpec::Kind& out) {
    if (s == "chain") {
        out = TopologySpec::Kind::Chain;
        return true;
    }
    if (s == "islands") {
        out = TopologySpec::Kind::Islands;
        return true;
    }
    if (s == "fanout" || s == "tree") {
        out = TopologySpec::Kind::FanoutTree;
        return true;
    }
    if (s == "mesh" || s == "mesh2d" || s == "grid") {
        out = TopologySpec::Kind::Mesh2D;
        return true;
    }
    if (s == "dag" || s == "random" || s == "randomdag") {
        out = TopologySpec::Kind::RandomDag;
        return true;
    }
    if (s == "feedback" || s == "ring") {
        out = TopologySpec::Kind::Feedback;
        return true;
    }
    return false;
}

/// Build a deterministic, canonically sorted edge list for a topology. Sorting
/// by (src, dst) makes conn_id assignment reproducible run-to-run.
inline std::vector<Edge> buildEdges(const TopologySpec& spec) {
    const uint32_t N = spec.num_units;
    const uint32_t d = spec.delay;
    std::vector<Edge> edges;
    if (N == 0) return edges;

    auto add = [&](uint32_t s, uint32_t t, uint32_t delay) {
        if (s < N && t < N && s != t) edges.push_back(Edge{s, t, delay});
    };

    switch (spec.kind) {
        case TopologySpec::Kind::Chain: {
            for (uint32_t i = 0; i + 1 < N; ++i) add(i, i + 1, d);
            break;
        }
        case TopologySpec::Kind::Islands: {
            const uint32_t cluster = std::max<uint32_t>(1, spec.fanout);
            for (uint32_t base = 0; base < N; base += cluster) {
                uint32_t end = std::min(base + cluster, N);
                for (uint32_t i = base; i + 1 < end; ++i) add(i, i + 1, d);
            }
            break;
        }
        case TopologySpec::Kind::FanoutTree: {
            const uint32_t b = std::max<uint32_t>(1, spec.fanout);
            for (uint32_t p = 0; p < N; ++p) {
                for (uint32_t c = 1; c <= b; ++c) add(p, p * b + c, d);
            }
            break;
        }
        case TopologySpec::Kind::Mesh2D: {
            uint32_t W = spec.grid_w;
            uint32_t H = spec.grid_h;
            if (W == 0) {
                W = static_cast<uint32_t>(std::lround(std::sqrt(static_cast<double>(N))));
                W = std::max<uint32_t>(1, W);
            }
            if (H == 0) H = (N + W - 1) / W;
            for (uint32_t y = 0; y < H; ++y) {
                for (uint32_t x = 0; x < W; ++x) {
                    uint32_t id = y * W + x;
                    if (id >= N) continue;
                    if (x + 1 < W) add(id, id + 1, d);  // east
                    if (y + 1 < H) add(id, id + W, d);  // south
                }
            }
            break;
        }
        case TopologySpec::Kind::RandomDag: {
            const uint32_t maxdeg = std::max<uint32_t>(1, spec.fanout);
            uint64_t rng = spec.seed | 1ULL;
            for (uint32_t i = 0; i + 1 < N; ++i) {
                rng = rng * kLcgMul + kLcgAdd;
                uint32_t deg = 1 + static_cast<uint32_t>((rng >> 17) % maxdeg);
                uint32_t span = N - 1 - i;  // candidate forward targets
                deg = std::min(deg, span);
                uint32_t last = i;  // pick strictly increasing targets
                for (uint32_t k = 0; k < deg; ++k) {
                    rng = rng * kLcgMul + kLcgAdd;
                    uint32_t remaining = N - 1 - last;
                    if (remaining == 0) break;
                    uint32_t step = 1 + static_cast<uint32_t>((rng >> 19) % remaining);
                    last += step;
                    add(i, last, d);
                }
            }
            break;
        }
        case TopologySpec::Kind::Feedback: {
            for (uint32_t i = 0; i + 1 < N; ++i) add(i, i + 1, d);
            // Back-edges MUST have delay >= 1: a delay=0 cycle would collapse the
            // SCC onto one thread / stall the safe boundary.
            const uint32_t bd = std::max<uint32_t>(1, d);
            if (N >= 2) add(N - 1, 0, bd);
            if (N >= 4) add(N - 1, N / 2, bd);
            break;
        }
    }

    std::sort(edges.begin(), edges.end(), [](const Edge& a, const Edge& b) {
        return a.src != b.src ? a.src < b.src : a.dst < b.dst;
    });
    return edges;
}

/// Instantiate units and wire them per `spec`. Uses the direct connect(port&,
/// port&) path (no PortDirectory / TreeNode needed). Returns the units in id order.
inline std::vector<SyntheticUnit*> createTopology(sender::TickSimulation& sim,
                                                  const TopologySpec& spec,
                                                  const WorkloadKnobs& knobs) {
    std::vector<SyntheticUnit*> units;
    units.reserve(spec.num_units);
    for (uint32_t i = 0; i < spec.num_units; ++i) {
        uint64_t seed_i = spec.seed ^ (static_cast<uint64_t>(i) * kGolden);
        units.push_back(sim.createUnit<SyntheticUnit>(
            seed_i, knobs.arith_ops, knobs.fp_ops, knobs.footprint_bytes, knobs.accesses_per_tick));
    }
    for (const Edge& e : buildEdges(spec)) {
        sim.connect(units[e.src]->out, units[e.dst]->in, e.delay);
    }
    return units;
}

/// XOR-fold the integer checksum across all units (the headline consistency value).
inline uint64_t aggregateChecksum(const std::vector<SyntheticUnit*>& units) {
    uint64_t chk = 0;
    for (auto* u : units) chk ^= u->checksum();
    return chk;
}

/// XOR-fold the FP checksum across all units.
inline uint64_t aggregateFpChecksum(const std::vector<SyntheticUnit*>& units) {
    uint64_t chk = 0;
    for (auto* u : units) chk ^= u->fpChecksum();
    return chk;
}

}  // namespace synthetic
