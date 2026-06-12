// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file cpu_pipeline_types.hpp
///
/// Shared data structures, hash validation, trace categories, and parameter
/// sets for the CPU pipeline examples.

#pragma once

#include <cstdint>
#include <string>

#include "chronon/Chronon.hpp"

namespace cpu_pipeline {

using namespace chronon;

// ============================================================================
// Data Structures
// ============================================================================

struct Instruction {
    uint64_t pc;
    uint64_t id;
    uint8_t next_hash_hint = 0;  // High 8 bits of next packet's hash
};

enum class OpType { LOAD, STORE, ADD, MUL };

/// Low-cardinality timeline event name per op type, so EX-occupancy spans
/// aggregate by kind in trace_processor (SELECT dur FROM slice WHERE name='mul').
inline EventNameRef opEventName(OpType op_type) {
    switch (op_type) {
        case OpType::LOAD:
            return "load"_ev;
        case OpType::STORE:
            return "store"_ev;
        case OpType::ADD:
            return "add"_ev;
        case OpType::MUL:
            return "mul"_ev;
    }
    return "op"_ev;
}

struct DecodedOp {
    OpType op_type;
    uint8_t dest_reg;
    uint8_t src_reg1;
    uint8_t src_reg2;
    int16_t imm;
    uint64_t instr_id;
    uint64_t pc;                 // For flush correctness
    uint8_t dispatch_target;     // ALU assignment (0-3)
    uint8_t next_hash_hint = 0;  // High 8 bits of next packet's hash
};

struct Result {
    uint64_t value;
    uint8_t dest_reg;
    uint64_t instr_id;
    uint8_t next_hash_hint = 0;  // High 8 bits of next packet's hash
};

struct CacheRequest {
    uint64_t addr;
    bool is_write;
    uint64_t req_id;
};

struct CacheResponse {
    uint64_t addr;
    uint64_t data;
    uint64_t req_id;
    bool hit;
};

// Flush signal for branch misprediction recovery
struct FlushSignal {
    uint64_t flush_id;        // Unique flush event ID
    uint64_t redirect_pc;     // PC to redirect to after flush
    uint64_t flush_point_id;  // Instruction ID that caused the flush
};

// Branch prediction structures for FetchUnit internal pipelines
struct BranchPrediction {
    uint64_t pc;
    bool predicted_taken;
    uint64_t target_pc;
};

struct BPStage1Data {  // 1-cycle latency prediction input
    uint64_t pc;
};

struct BPStage2Data {  // 2-cycle latency prediction output
    uint64_t pc;
    BranchPrediction prediction;
};

// ============================================================================
// Hash-Based Packet Validation
// ============================================================================

namespace validation {

// FNV-1a style hash combining
inline uint64_t hashCombine(uint64_t h1, uint64_t h2) { return h1 ^ (h2 * 0x100000001b3ULL); }

inline uint64_t hashInstruction(const Instruction& i) { return hashCombine(i.pc, i.id); }

inline uint64_t hashDecodedOp(const DecodedOp& op) {
    uint64_t h = static_cast<uint64_t>(op.op_type);
    h = hashCombine(h, op.dest_reg);
    h = hashCombine(h, op.src_reg1);
    h = hashCombine(h, op.src_reg2);
    h = hashCombine(h, static_cast<uint64_t>(op.imm));
    h = hashCombine(h, op.instr_id);
    return h;
}

inline uint64_t hashResult(const Result& r) {
    return hashCombine(hashCombine(r.value, r.dest_reg), r.instr_id);
}

inline uint8_t high8(uint64_t hash) { return static_cast<uint8_t>(hash >> 56); }

}  // namespace validation

// ============================================================================
// Trace Categories - Auto-assigned bit positions (no manual bit management!)
// ============================================================================

namespace trace_cat {
inline const auto ICACHE_HIT = Category<"icache_hit", "I-Cache hit events">{};
inline const auto ICACHE_MISS = Category<"icache_miss", "I-Cache miss events">{};
inline const auto FETCH = Category<"fetch", "Instruction fetch events">{};
inline const auto DECODE = Category<"decode", "Decode events">{};
inline const auto EXECUTE = Category<"execute", "Execute events">{};
inline const auto COMMIT = Category<"commit", "Commit events">{};
inline const auto L2_ACCESS = Category<"l2_access", "L2 access events">{};
inline const auto FLUSH = Category<"flush", "Pipeline flush events">{};
inline const auto BRANCH_PRED = Category<"branch_pred", "Branch prediction events">{};
inline const auto DISPATCH = Category<"dispatch", "Dispatch events">{};
}  // namespace trace_cat

// ============================================================================
// Parameter Sets for YAML Configuration
// ============================================================================

struct FetchParams : public ParameterSet {
    Param<uint64_t> max_instructions{this, "max_instructions", 1000000,
                                     "Maximum instructions to fetch"};
    Param<uint32_t> icache_lines{this, "icache_lines", 50, "Initial I-Cache lines"};
};

struct DecodeParams : public ParameterSet {
    Param<uint32_t> decode_width{this, "decode_width", 4, "Max instructions per cycle"};
};

struct ExecuteParams : public ParameterSet {};

struct WritebackParams : public ParameterSet {
    Param<uint64_t> target_commits{this, "target_commits", 0,
                                   "Target commits before termination (0 = no limit)"};
};

struct L2CacheParams : public ParameterSet {
    Param<uint32_t> latency{this, "latency", 10, "L2 cache access latency"};
    Param<uint64_t> cache_lines{this, "cache_lines", 1000, "Number of L2 cache lines"};
};

struct ALUParams : public ParameterSet {
    Param<uint32_t> alu_id{this, "alu_id", 0, "ALU identifier (0-3)"};
    Param<double> flush_probability{this, "flush_probability", 0.001,
                                    "Branch mispredict probability"};
};

struct FlushEngineParams : public ParameterSet {};

}  // namespace cpu_pipeline
