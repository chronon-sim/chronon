// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file boom_topology.hpp
///
/// A BOOMv3-flavored (RISC-V BOOM, github.com/riscv-boom/riscv-boom) synthetic
/// system: N identical out-of-order cores sharing one L2 cache. Built entirely
/// from `SyntheticUnit`s, each sized to the SRAM working set and per-tick
/// compute of the structure it stands in for — so it stresses chronon's
/// scheduler with a *realistic* dependency graph (deep frontend, feedback loops,
/// a shared last-level cache) instead of an abstract chain/mesh, while staying
/// far cheaper than a real RTL/uarch model.
///
/// Per-core pipeline (BOOMv3-like), forward edges unless noted:
///
///   bpd ─3─▶ fetch ─4─▶ decode ─▶ rename ─▶ dispatch ─▶ issue ─┬─▶ exe_int ─┐
///    ▲          ▲                    ▲                          ├─▶ exe_fp  ─┤
///    │          │                    │                          └─▶ lsu ─────┤
///    │ (redirect/update)    (commit frees)                                   │
///    └──────────┴────────────────────────── rob ◀───────────────────────────┘
///                                             ▲   (wakeup: exe/lsu ─▶ issue)
///   fetch ─▶ L2 (I$ miss),  lsu ─▶ L2 (D$ miss),  L2 ─8─▶ fetch/lsu (refill)
///
/// Multi-cycle stages are modeled by the latency (delay) on their forward edge:
///   - bpd  (BTB/BIM/TAGE/RAS)  : 3-cycle  -> bpd→fetch delay=3
///   - fetch(I-TLB/I$/predecode): 4-cycle  -> fetch→decode delay=4
///   - L2 access latency        : 8-cycle  -> L2→core delay=8
/// All other forward + feedback edges are delay=1 (loose cycles; lookahead-able).
/// Every SyntheticUnit is deterministic, so the system checksum is invariant to
/// worker count (see synthetic_workload.hpp).
///
/// NOTE: this topology has *heterogeneous* edge delays (1/3/4/8). Heterogeneous
/// delays once made lookahead diverge from barrier/sequential (the MPSC arbiter
/// drained all of an InPort's connections up to a single min-over-producers
/// bound, holding back low-delay messages). That is fixed: the arbiter now
/// drains each connection up to its own producer's completed cycle, so lookahead
/// is cycle-accurate here and the system checksum is worker-count invariant. The
/// `test_mpsc_mixed_delay_determinism` regression test guards it.

#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "synthetic_workload.hpp"

namespace synthetic {

using sender::TickSimulation;

// Per-stage workload sizing. footprint_bytes ~ the structure's SRAM; accesses ~
// cache lines / table entries walked per tick; arith ~ per-tick compute (e.g.
// TAGE hash folding, wakeup-select). Sized in the spirit of a BOOMv3 "large"
// config — order-of-magnitude realistic, not cycle-exact.
struct BoomStage {
    const char* role;
    uint32_t arith_ops;
    uint32_t fp_ops;
    uint64_t footprint_bytes;
    uint32_t accesses_per_tick;
};

// Stage order is also the per-core seed/index order.
enum BoomStageId {
    BPD = 0,
    FETCH,
    DECODE,
    RENAME,
    DISPATCH,
    ISSUE,
    EXE_INT,
    EXE_FP,
    LSU,
    ROB,
    kStagesPerCore
};

inline constexpr std::array<BoomStage, kStagesPerCore> kBoomStages = {{
    // role        arith  fp    footprint(B)    accesses
    {"bpd", 400, 0, 40u * 1024, 8},     // BTB + BIM + TAGE (6 tbl) + RAS, 3-cycle
    {"fetch", 150, 0, 32u * 1024, 4},   // I-TLB + 32KB I$ + predecode, 4-cycle
    {"decode", 200, 0, 4u * 1024, 1},   // 8-wide decoders
    {"rename", 150, 0, 8u * 1024, 4},   // map table + freelist + busy table
    {"dispatch", 80, 0, 2u * 1024, 1},  // dispatch to issue queues
    {"issue", 300, 0, 8u * 1024, 4},    // IQs + wakeup/select matrix
    {"exe_int", 150, 0, 4u * 1024, 2},  // int ALUs + PRF read + bypass
    {"exe_fp", 50, 200, 4u * 1024, 1},  // FP/vector pipes
    {"lsu", 250, 0, 40u * 1024, 6},     // LDQ/STQ + D-TLB + 32KB L1 D$
    {"rob", 150, 0, 8u * 1024, 2},      // ROB + commit
}};

// Shared L2: 512 KiB, multi-bank tag+data lookups.
inline constexpr BoomStage kL2Stage = {"l2_512k", 120, 0, 512u * 1024, 4};

// Edge delays (cycles).
inline constexpr uint32_t kDelayBpd = 3;    // bpd 3-cycle pipeline
inline constexpr uint32_t kDelayFetch = 4;  // fetch 4-cycle pipeline
inline constexpr uint32_t kDelayL2 = 8;     // L2 access latency
inline constexpr uint32_t kDelay1 = 1;      // single-cycle forward / feedback

struct BoomCore {
    std::array<SyntheticUnit*, kStagesPerCore> u{};
    SyntheticUnit* operator[](BoomStageId id) const { return u[id]; }
};

/// Build `num_cores` identical cores + one shared L2 and return every unit (cores
/// first in core/stage order, L2 last). Per-unit seeds are deterministic.
inline std::vector<SyntheticUnit*> buildBoomSystem(TickSimulation& sim, uint32_t num_cores,
                                                   uint64_t seed = 0xB00Cu) {
    std::vector<SyntheticUnit*> all;
    std::vector<BoomCore> cores(num_cores);

    // L2 first so we have its handle while wiring cores (created/owned by sim).
    uint64_t l2_seed = seed ^ 0x12CAC4E0ULL;  // shared LLC seed
    SyntheticUnit* l2 = sim.createUnit<SyntheticUnit>(
        l2_seed, kL2Stage.arith_ops, kL2Stage.fp_ops, kL2Stage.footprint_bytes,
        kL2Stage.accesses_per_tick, std::string("L2"));

    for (uint32_t c = 0; c < num_cores; ++c) {
        BoomCore& core = cores[c];
        for (uint32_t s = 0; s < kStagesPerCore; ++s) {
            const BoomStage& spec = kBoomStages[s];
            uint64_t us = seed ^ (static_cast<uint64_t>(c + 1) * kGolden) ^
                          (static_cast<uint64_t>(s) * 0x100000001b3ULL);
            std::string nm = "c" + std::to_string(c) + "_" + spec.role;  // e.g. c0_fetch
            core.u[s] = sim.createUnit<SyntheticUnit>(
                us, spec.arith_ops, spec.fp_ops, spec.footprint_bytes, spec.accesses_per_tick, nm);
        }
    }

    auto E = [&](SyntheticUnit* a, SyntheticUnit* b, uint32_t d) { sim.connect(a->out, b->in, d); };

    for (uint32_t c = 0; c < num_cores; ++c) {
        const BoomCore& k = cores[c];
        // Frontend (multi-cycle stages carry their latency on the forward edge).
        E(k[BPD], k[FETCH], kDelayBpd);
        E(k[FETCH], k[DECODE], kDelayFetch);
        E(k[DECODE], k[RENAME], kDelay1);
        E(k[RENAME], k[DISPATCH], kDelay1);
        E(k[DISPATCH], k[ISSUE], kDelay1);
        // Issue -> execution units.
        E(k[ISSUE], k[EXE_INT], kDelay1);
        E(k[ISSUE], k[EXE_FP], kDelay1);
        E(k[ISSUE], k[LSU], kDelay1);
        // Results -> ROB.
        E(k[EXE_INT], k[ROB], kDelay1);
        E(k[EXE_FP], k[ROB], kDelay1);
        E(k[LSU], k[ROB], kDelay1);
        // Wakeup (broadcast back to issue).
        E(k[EXE_INT], k[ISSUE], kDelay1);
        E(k[EXE_FP], k[ISSUE], kDelay1);
        E(k[LSU], k[ISSUE], kDelay1);
        // Branch resolve / redirect / predictor update.
        E(k[EXE_INT], k[BPD], kDelay1);
        E(k[EXE_INT], k[FETCH], kDelay1);
        // Commit -> rename (free regs) and fetch (exception redirect).
        E(k[ROB], k[RENAME], kDelay1);
        E(k[ROB], k[FETCH], kDelay1);
        // Memory hierarchy: I$/D$ miss requests up, refills back (long latency).
        E(k[FETCH], l2, kDelay1);
        E(k[LSU], l2, kDelay1);
        E(l2, k[FETCH], kDelayL2);
        E(l2, k[LSU], kDelayL2);
    }

    for (uint32_t c = 0; c < num_cores; ++c)
        for (auto* p : cores[c].u) all.push_back(p);
    all.push_back(l2);
    return all;
}

}  // namespace synthetic
