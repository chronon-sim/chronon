// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file cpu_pipeline_backend.hpp
///
/// Back-end pipeline units: ALUUnit, FlushEngine, WritebackUnit, L2CacheUnit.

#pragma once

#include <array>
#include <optional>
#include <queue>
#include <random>
#include <set>

#include "cpu_pipeline_types.hpp"

namespace cpu_pipeline {

using namespace chronon;

/**
 * ALUUnit - Standalone ALU execution unit.
 *
 * Features:
 * - Configurable ALU ID (0-3)
 * - Internal pipeline register for flush handling
 * - Random flush generation for branch misprediction simulation
 * - Configurable misprediction probability
 */
class ALUUnit : public PhasedAutoRegisteredUnit<ALUUnit>, public ObservableUnit {
public:
    static constexpr const char* unit_type_name = "ALUUnit";
    static constexpr const char* unit_description = "Standalone ALU execution unit";
    using ParameterSet = ALUParams;

    InPort<DecodedOp> in_op{this, "in_op", 64};
    InPort<FlushSignal> in_flush{this, "in_flush", 8};
    OutPort<Result> out_result{this, "out_result"};
    OutPort<FlushSignal> out_flush_request{this, "out_flush_request"};

    Counter ops_{this, "ops", "Operations executed", "ops"};
    Counter executed_{this, "executed", "Operations completed", "ops"};
    Counter alu_ops_{this, "alu_ops", "ALU operations", "ops"};
    Counter load_ops_{this, "load_ops", "Load operations", "ops"};
    Counter store_ops_{this, "store_ops", "Store operations", "ops"};
    Counter branch_mispred_{this, "branch_mispred", "Branch mispredictions", "events"};

    CHRONON_UNIT_CONSTRUCTOR(ALUUnit, ParameterSet, params->alu_id, params->flush_probability)
    (uint32_t alu_id = 0, double flush_probability = 0.001)
        : PhasedAutoRegisteredUnit("alu" + std::to_string(alu_id)),
          alu_id_(alu_id),
          flush_probability_(flush_probability),
          rng_(std::random_device{}()) {}

    void initialize() override {
        info<"ALU{} initialized: flush_probability={}">(alu_id_, flush_probability_);
    }

    uint64_t getObserveCycle() const noexcept override { return localCycle(); }

    uint64_t getValidationPassCount() const { return validation_pass_count_; }
    uint64_t getValidationFailCount() const { return validation_fail_count_; }
    bool validationPassed() const { return validation_fail_count_ == 0; }

    uint64_t getOpsCount() const { return ops_.get(); }
    uint32_t getAluId() const { return alu_id_; }

    template <ValidPhase P>
    void tickPhase() {
        pipeline_reg_.template beginCycle<P>();

        // 1. Handle incoming flush - clear pipeline
        while (auto flush = in_flush.tryReceive(localCycle())) {
            trace<"ALU{} flush received: flush_id={} pipeline_cleared">(trace_cat::FLUSH, alu_id_,
                                                                        flush->flush_id);

            in_op.flush();
            out_result.cancelInFlight();

            pipeline_reg_.reset();
            first_packet_received_ = true;
            skip_next_validation_ = true;
            expected_hash_hint_ = 0;
            pending_result_.reset();
            (void)flush;
        }

        // 2. Process current pipeline stage
        bool can_accept_new = false;
        if (pipeline_reg_.template valid<P>()) {
            auto& op = pipeline_reg_.template read<P>();

            std::uniform_real_distribution<> dist(0.0, 1.0);
            if (dist(rng_) < flush_probability_) {
                if (out_flush_request.canSend()) {
                    if (out_flush_request.send(FlushSignal{.flush_id = op.instr_id,
                                                           .redirect_pc = op.pc + 4,
                                                           .flush_point_id = op.instr_id})) {
                        trace<
                            "Branch misprediction: instr_id={} redirect_pc=0x{:x} flush_id={} "
                            "alu_id={}">(trace_cat::FLUSH, op.instr_id, op.pc + 4, op.instr_id,
                                         alu_id_);
                        ++branch_mispred_;
                    }
                }
            }

            uint64_t result_value = executeOp(op);

            if (out_result.canSend()) {
                Result result{
                    .value = result_value, .dest_reg = op.dest_reg, .instr_id = op.instr_id};
                bool sent = sendWithValidation(result);
                trace<"Execute ALU{}: instr_id={} op_type={} result={} dest_reg=r{}">(
                    trace_cat::EXECUTE, alu_id_, op.instr_id, static_cast<int>(op.op_type),
                    result_value, op.dest_reg);
                if (sent) {
                    ++executed_;
                    ++ops_;
                }
                can_accept_new = true;
            } else {
                pipeline_reg_.template retain<P>();
                warn<"ALU{} result blocked: output port full, instr_id={} - stalling">(alu_id_,
                                                                                       op.instr_id);
                can_accept_new = false;
            }
        } else {
            can_accept_new = true;
        }

        // 3. Accept new operation only if pipeline can progress
        if (can_accept_new) {
            if (auto op = in_op.tryReceive(localCycle())) {
                validatePacket(*op, validation::hashDecodedOp);
                pipeline_reg_.template write<P>(*op);
            }
        }
    }

private:
    template <typename Packet, typename HashFn>
    void validatePacket(const Packet& pkt, HashFn hashFn) {
        if (skip_next_validation_) {
            skip_next_validation_ = false;
            return;
        }
        if (!first_packet_received_) {
            uint8_t actual_hint = validation::high8(hashFn(pkt));
            if (actual_hint == expected_hash_hint_) {
                ++validation_pass_count_;
            } else {
                ++validation_fail_count_;
                warn<"ALU{} Validation FAIL: expected=0x{:02x} actual=0x{:02x} instr_id={}">(
                    alu_id_, expected_hash_hint_, actual_hint, pkt.instr_id);
            }
        }
        first_packet_received_ = false;
        expected_hash_hint_ = pkt.next_hash_hint;
    }

    bool sendWithValidation(Result current) {
        bool sent = false;
        if (pending_result_) {
            pending_result_->next_hash_hint = validation::high8(validation::hashResult(current));
            sent = out_result.send(*pending_result_);
            if (!sent) {
                return false;
            }
        }
        pending_result_ = current;
        return sent;
    }

    uint64_t executeOp(const DecodedOp& op) {
        switch (op.op_type) {
            case OpType::ADD:
                ++alu_ops_;
                return op.src_reg1 + op.src_reg2;
            case OpType::MUL:
                ++alu_ops_;
                return op.src_reg1 * op.src_reg2;
            case OpType::LOAD:
                ++load_ops_;
                return op.imm;
            case OpType::STORE:
                ++store_ops_;
                return op.imm;
            default:
                return op.imm;
        }
    }

    uint32_t alu_id_;
    double flush_probability_;
    std::mt19937 rng_;
    SingleStageReg<DecodedOp> pipeline_reg_;

    bool first_packet_received_ = true;
    bool skip_next_validation_ = false;
    uint8_t expected_hash_hint_ = 0;
    uint64_t validation_pass_count_ = 0;
    uint64_t validation_fail_count_ = 0;

    std::optional<Result> pending_result_;
};

/**
 * FlushEngine - Centralized flush coordination unit.
 *
 * Receives flush requests from ALUs and broadcasts to all pipeline units.
 */
class FlushEngine : public AutoRegisteredUnit<FlushEngine>, public ObservableUnit {
public:
    static constexpr const char* unit_type_name = "FlushEngine";
    static constexpr const char* unit_description = "Pipeline flush controller";
    using ParameterSet = FlushEngineParams;

    InPort<FlushSignal> in_flush_request{this, "in_flush_request", 64};
    OutPort<FlushSignal> out_flush_broadcast{this, "out_flush_broadcast"};

    Counter flushes_{this, "flushes", "Pipeline flushes", "events"};

    CHRONON_UNIT_CONSTRUCTOR(FlushEngine, ParameterSet, )
    () : AutoRegisteredUnit("flush_engine") {}

    void initialize() override { info<"FlushEngine initialized">(); }

    uint64_t getObserveCycle() const noexcept override { return localCycle(); }

    void tick() override {
        while (auto req = in_flush_request.tryReceive(localCycle())) {
            if (out_flush_broadcast.canSend()) {
                if (out_flush_broadcast.send(*req)) {
                    trace<"Flush broadcast: flush_id={} redirect_pc=0x{:x} flush_point_id={}">(
                        trace_cat::FLUSH, req->flush_id, req->redirect_pc, req->flush_point_id);
                    ++flushes_;
                }
            } else {
                error<"Flush broadcast blocked: critical flush request dropped, flush_id={}">(
                    req->flush_id);
            }
        }
    }
};

/**
 * WritebackUnit - 4-wide result writeback unit.
 *
 * Commits execution results from all 4 ALUs.
 */
class WritebackUnit : public AutoRegisteredUnit<WritebackUnit>, public ObservableUnit {
public:
    static constexpr const char* unit_type_name = "WritebackUnit";
    static constexpr const char* unit_description = "4-wide result writeback unit";
    using ParameterSet = WritebackParams;

    InPort<Result> in_result_0{this, "in_result_0", 64};
    InPort<Result> in_result_1{this, "in_result_1", 64};
    InPort<Result> in_result_2{this, "in_result_2", 64};
    InPort<Result> in_result_3{this, "in_result_3", 64};
    InPort<FlushSignal> in_flush{this, "in_flush", 8};

    Counter committed_{this, "committed", "Results committed", "results"};

    CHRONON_UNIT_CONSTRUCTOR(WritebackUnit, ParameterSet, params->target_commits)
    (uint64_t target_commits = 0)
        : AutoRegisteredUnit("writeback"), target_commits_(target_commits) {}

    void initialize() override {
        if (target_commits_ > 0) {
            info<"WritebackUnit initialized: 4-wide commit, target_commits={}">(target_commits_);
        } else {
            info<"WritebackUnit initialized: 4-wide commit, no commit limit">();
        }
    }

    uint64_t getObserveCycle() const noexcept override { return localCycle(); }

    uint64_t getValidationPassCount() const { return validation_pass_count_; }
    uint64_t getValidationFailCount() const { return validation_fail_count_; }
    bool validationPassed() const { return validation_fail_count_ == 0; }

    void tick() override {
        // Handle flush signals
        while (auto flush = in_flush.tryReceive(localCycle())) {
            trace<"Writeback flush: flush_id={}">(trace_cat::FLUSH, flush->flush_id);

            in_result_0.flush();
            in_result_1.flush();
            in_result_2.flush();
            in_result_3.flush();

            for (size_t i = 0; i < 4; ++i) {
                first_packet_received_[i] = true;
                skip_next_validation_[i] = true;
                expected_hash_hint_[i] = 0;
            }
            (void)flush;
        }

        InPort<Result>* ports[] = {&in_result_0, &in_result_1, &in_result_2, &in_result_3};

        for (size_t i = 0; i < 4; ++i) {
            while (auto result = ports[i]->tryReceive(localCycle())) {
                validatePacket(*result, i);
                trace<"Commit: instr_id={} value=0x{:x} dest_reg=r{}">(
                    trace_cat::COMMIT, result->instr_id, result->value, result->dest_reg);
                ++committed_;
                ++total_commits_;

                if (target_commits_ > 0 && total_commits_ >= target_commits_) {
                    requestTermination(
                        TerminationReason::Completed, 0,
                        "Committed " + std::to_string(total_commits_) + " instructions");
                    return;
                }
            }
        }
    }

private:
    void validatePacket(const Result& pkt, size_t alu_id) {
        if (skip_next_validation_[alu_id]) {
            skip_next_validation_[alu_id] = false;
            return;
        }
        if (!first_packet_received_[alu_id]) {
            uint8_t actual_hint = validation::high8(validation::hashResult(pkt));
            if (actual_hint == expected_hash_hint_[alu_id]) {
                ++validation_pass_count_;
            } else {
                ++validation_fail_count_;
                warn<
                    "Writeback Validation FAIL from ALU{}: expected=0x{:02x} actual=0x{:02x} "
                    "instr_id={}">(alu_id, expected_hash_hint_[alu_id], actual_hint, pkt.instr_id);
            }
        }
        first_packet_received_[alu_id] = false;
        expected_hash_hint_[alu_id] = pkt.next_hash_hint;
    }

    uint64_t target_commits_;
    uint64_t total_commits_ = 0;

    std::array<bool, 4> first_packet_received_ = {true, true, true, true};
    std::array<bool, 4> skip_next_validation_ = {false, false, false, false};
    std::array<uint8_t, 4> expected_hash_hint_ = {0, 0, 0, 0};
    uint64_t validation_pass_count_ = 0;
    uint64_t validation_fail_count_ = 0;
};

/**
 * L2CacheUnit - L2 cache unit.
 *
 * Handles I-Cache miss requests with configurable latency.
 */
class L2CacheUnit : public AutoRegisteredUnit<L2CacheUnit>, public ObservableUnit {
public:
    static constexpr const char* unit_type_name = "L2CacheUnit";
    static constexpr const char* unit_description = "L2 cache unit";
    using ParameterSet = L2CacheParams;

    InPort<CacheRequest> in_req{this, "in_req", 64};
    OutPort<CacheResponse> out_resp{this, "out_resp"};

    Counter l2_hits_{this, "l2_hits", "L2 cache hits", "accesses"};

    CHRONON_UNIT_CONSTRUCTOR(L2CacheUnit, ParameterSet, params->latency, params->cache_lines)
    (uint32_t latency = 10, uint64_t cache_lines = 1000)
        : AutoRegisteredUnit("l2cache"), latency_(latency), cache_line_count_(cache_lines) {
        for (uint64_t line = 0; line < cache_lines; ++line) {
            l2_lines_.insert(line);
        }
    }

    void initialize() override {
        info<"L2CacheUnit initialized: latency={} cache_lines={}">(latency_, cache_line_count_);
    }

    uint64_t getObserveCycle() const noexcept override { return localCycle(); }

    void tick() override {
        while (auto req = in_req.tryReceive(localCycle())) {
            trace<"L2 request: addr=0x{:x} is_write={} req_id={} latency={}">(
                trace_cat::L2_ACCESS, req->addr, req->is_write, req->req_id, latency_);
            ++l2_hits_;
            pending_.push({*req, localCycle() + latency_});

            if (pending_.size() > 50) {
                warn<"L2 pending queue high: {} requests in flight">(pending_.size());
            }
        }

        while (!pending_.empty() && pending_.front().second <= localCycle()) {
            auto& [req, _] = pending_.front();
            trace<"L2 response: addr=0x{:x} req_id={} hit=true">(trace_cat::L2_ACCESS, req.addr,
                                                                 req.req_id);
            if (out_resp.send(CacheResponse{
                    .addr = req.addr, .data = req.addr, .req_id = req.req_id, .hit = true})) {
                pending_.pop();
            } else {
                break;
            }
        }
    }

private:
    uint32_t latency_;
    uint64_t cache_line_count_;
    std::set<uint64_t> l2_lines_;
    std::queue<std::pair<CacheRequest, uint64_t>> pending_;
};

}  // namespace cpu_pipeline
