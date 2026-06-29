// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file cpu_pipeline_frontend.hpp
///
/// Front-end pipeline units: FetchUnit and DecodeUnit.

#pragma once

#include <array>
#include <optional>
#include <set>
#include <unordered_map>

#include "cpu_pipeline_types.hpp"

namespace cpu_pipeline {

using namespace chronon;

/**
 * FetchUnit - Instruction fetch unit with I-Cache simulation and branch prediction.
 *
 * Features:
 * - Configurable max instructions and I-Cache size
 * - Simulates I-Cache hits/misses
 * - Sends cache miss requests to L2
 * - Dual internal branch prediction pipeline:
 *   - Stage 1: PC -> prediction input (1 cycle latency)
 *   - Stage 2: Prediction output (2 cycle latency, 2-branch lookahead)
 * - Flush handling with PC redirect
 */
class FetchUnit : public PhasedAutoRegisteredUnit<FetchUnit>, public ObservableUnit {
public:
    static constexpr const char* unit_type_name = "FetchUnit";
    static constexpr const char* unit_description =
        "Instruction fetch unit with I-Cache and branch prediction";
    using ParameterSet = FetchParams;
    static constexpr uint32_t kFetchWidth = 4;

    OutPort<Instruction> out_instr{this, "out_instr", kFetchWidth};
    OutPort<CacheRequest> out_icache_miss{this, "out_icache_miss"};
    InPort<CacheResponse> in_l2_resp{this, "in_l2_resp", 64};
    InPort<FlushSignal> in_flush{this, "in_flush", 8};

    // Per-instance counters
    Counter fetched_{this, "fetched", "Instructions fetched", "instrs"};
    Counter icache_hits_{this, "icache_hits", "I-Cache hits", "accesses"};
    Counter icache_misses_{this, "icache_misses", "I-Cache misses", "accesses"};
    Counter bp_predictions_{this, "bp_predictions", "Predictions made", "preds"};
    Counter flushes_{this, "flushes", "Pipeline flushes", "events"};

    // Instruction-lifetime pilot: every stage stamps its events with
    // flow(instr_id), so Perfetto links one instruction's journey
    // fetch → dispatch → ex → commit across units.
    TimelineLane fetch_lane_{this, "fetch"};

    CHRONON_UNIT_CONSTRUCTOR(FetchUnit, ParameterSet, params->max_instructions,
                             params->icache_lines)
    (uint64_t max_instr = 1000000, uint32_t icache_lines = 50)
        : PhasedAutoRegisteredUnit("fetch"),
          max_instructions_(max_instr),
          icache_line_count_(icache_lines) {
        for (uint64_t line = 0; line < icache_lines; ++line) {
            icache_lines_.insert(line);
        }
    }

    void initialize() override {
        info<"FetchUnit initialized: max_instructions={} icache_lines={}">(max_instructions_,
                                                                           icache_line_count_);
    }

    uint64_t getObserveCycle() const noexcept override { return localCycle(); }

    bool isCompleted() const override { return fetched_count_ >= max_instructions_; }

    template <ValidPhase P>
    void tickPhase() {
        bp_stage1_.template beginCycle<P>();
        bp_stage2_.template beginCycle<P>();

        // 1. Handle flush signals first (highest priority)
        while (auto flush = in_flush.tryReceive(localCycle())) {
            trace<"Flush received: redirect_pc=0x{:x} flush_id={}">(
                trace_cat::FLUSH, flush->redirect_pc, flush->flush_id);
            flushing_ = true;
            redirect_pc_ = flush->redirect_pc;

            out_instr.cancelInFlight();
            out_icache_miss.cancelInFlight();

            bp_stage1_.reset();
            bp_stage2_.reset();
            pending_send_.reset();
            ++flushes_;
        }

        // Apply flush redirect
        if (flushing_) {
            pc_ = redirect_pc_;
            instr_id_ = redirect_pc_;
            flushing_ = false;
            stalled_ = false;
            trace<"Flush applied: new_pc=0x{:x} pipelines_reset">(trace_cat::FLUSH, pc_);
        }

        // 2. Handle L2 responses
        while (auto resp = in_l2_resp.tryReceive(localCycle())) {
            icache_lines_.insert(resp->addr >> 6);
            stalled_ = false;
            trace<"L2 response received: addr=0x{:x} cache_line={} unstalling">(
                trace_cat::ICACHE_MISS, resp->addr, resp->addr >> 6);
            stall_cycles_ = 0;
        }

        // 3. Branch Prediction Pipeline (dual pipeline demonstration)
        bp_stage2_.template forEachValidConsume<P>([&](auto& pred) {
            trace<"Branch prediction consumed: pc=0x{:x} taken={} target=0x{:x}">(
                trace_cat::BRANCH_PRED, pred.prediction.pc, pred.prediction.predicted_taken,
                pred.prediction.target_pc);
            if (pred.prediction.predicted_taken) {
            }
            ++bp_predictions_;
        });

        bp_stage1_.template forEachValidConsume<P>([&](auto& s1) {
            auto pred = predictBranch(s1.pc);
            bp_stage2_.template write<P>(BPStage2Data{.pc = s1.pc, .prediction = pred});
        });

        bp_stage1_.template write<P>(BPStage1Data{.pc = pc_});

        // 4. Fetch logic - 4-wide fetch
        if (stalled_ || fetched_count_ >= max_instructions_) {
            if (stalled_) {
                stall_cycles_++;
                if (stall_cycles_ % 1000 == 0) {
                    warn<"FetchUnit stalled for {} cycles waiting for L2 response">(stall_cycles_);
                }
            }
            if (fetched_count_ >= max_instructions_) {
                flushPending();
            }
            return;
        }

        for (uint32_t i = 0; i < kFetchWidth && fetched_count_ < max_instructions_; ++i) {
            if (!out_instr.canSend()) {
                break;
            }

            uint64_t cache_line = pc_ >> 6;
            bool hit = icache_lines_.count(cache_line) > 0;

            if (hit) {
                Instruction instr{.pc = pc_, .id = instr_id_};
                sendWithValidation(instr);
                fetch_lane_.instant(0, trace_cat::FETCH, "fetch"_ev, flow(instr_id_),
                                    arg<"pc">(pc_));
                trace<"I-Cache HIT: pc=0x{:x} cache_line={} instr_id={}">(
                    trace_cat::ICACHE_HIT, pc_, cache_line, instr_id_);
                ++pc_;
                ++instr_id_;
                ++fetched_count_;
                ++icache_hits_;
            } else {
                if (out_icache_miss.canSend()) {
                    CacheRequest req{
                        .addr = cache_line << 6, .is_write = false, .req_id = instr_id_};
                    if (out_icache_miss.send(req)) {
                        trace<"I-Cache MISS: pc=0x{:x} cache_line={} stalling">(
                            trace_cat::ICACHE_MISS, pc_, cache_line);
                        stalled_ = true;
                        ++icache_misses_;
                    }
                }
                break;
            }
        }
    }

private:
    void sendWithValidation(Instruction current) {
        if (pending_send_) {
            pending_send_->next_hash_hint = validation::high8(validation::hashInstruction(current));
            if (out_instr.send(*pending_send_)) {
                ++fetched_;
                pending_send_ = current;
            } else {
                pending_send_ = current;
            }
        } else {
            pending_send_ = current;
        }
    }

    void flushPending() {
        if (pending_send_ && out_instr.canSend()) {
            pending_send_->next_hash_hint = 0;
            if (out_instr.send(*pending_send_)) {
                ++fetched_;
                pending_send_.reset();
            }
        }
    }

    BranchPrediction predictBranch(uint64_t pc) {
        bool predicted_taken = false;
        auto it = branch_history_.find(pc);
        if (it != branch_history_.end()) {
            predicted_taken = it->second;
        }

        trace<"Branch prediction: pc=0x{:x} taken={} target=0x{:x} from_history={}">(
            trace_cat::BRANCH_PRED, pc, predicted_taken, predicted_taken ? pc + 4 : pc + 1,
            it != branch_history_.end());

        return BranchPrediction{.pc = pc, .predicted_taken = predicted_taken, .target_pc = pc + 4};
    }

    uint64_t max_instructions_;
    uint32_t icache_line_count_;
    std::set<uint64_t> icache_lines_;
    uint64_t pc_ = 0;
    uint64_t instr_id_ = 0;
    uint64_t fetched_count_ = 0;
    bool stalled_ = false;

    bool flushing_ = false;
    uint64_t redirect_pc_ = 0;
    uint64_t stall_cycles_ = 0;

    std::optional<Instruction> pending_send_;

    SingleStageReg<BPStage1Data> bp_stage1_;
    SingleStageReg<BPStage2Data> bp_stage2_;

    std::unordered_map<uint64_t, bool> branch_history_;
};

/**
 * DecodeUnit - 4-wide instruction decode unit.
 *
 * Features:
 * - Configurable decode width (default 4 instructions per cycle)
 * - Batch receive with receiveAll()
 * - Round-robin dispatch to 4 ALUs
 * - Flush handling
 */
class DecodeUnit : public AutoRegisteredUnit<DecodeUnit>, public ObservableUnit {
public:
    static constexpr const char* unit_type_name = "DecodeUnit";
    static constexpr const char* unit_description = "4-wide instruction decode unit";
    using ParameterSet = DecodeParams;

    InPort<Instruction> in_instr{this, "in_instr", 128};
    InPort<FlushSignal> in_flush{this, "in_flush", 8};

    OutPort<DecodedOp> out_decoded_0{this, "out_decoded_0"};
    OutPort<DecodedOp> out_decoded_1{this, "out_decoded_1"};
    OutPort<DecodedOp> out_decoded_2{this, "out_decoded_2"};
    OutPort<DecodedOp> out_decoded_3{this, "out_decoded_3"};

    Counter decoded_{this, "decoded", "Instructions decoded", "instrs"};

    /// Dispatch instants carry the instruction flow plus the target ALU.
    TimelineLane dispatch_lane_{this, "dispatch"};

    CHRONON_UNIT_CONSTRUCTOR(DecodeUnit, ParameterSet, params->decode_width)
    (uint32_t decode_width = 4) : AutoRegisteredUnit("decode"), decode_width_(decode_width) {}

    void initialize() override { info<"DecodeUnit initialized: decode_width={}">(decode_width_); }

    uint64_t getObserveCycle() const noexcept override { return localCycle(); }

    uint64_t getValidationPassCount() const { return validation_pass_count_; }
    uint64_t getValidationFailCount() const { return validation_fail_count_; }
    bool validationPassed() const { return validation_fail_count_ == 0; }

    void tick() override {
        // Handle flush signals
        while (auto flush = in_flush.tryReceive(localCycle())) {
            trace<"Decode flush: flush_id={} - clearing {} buffered instructions">(
                trace_cat::FLUSH, flush->flush_id, decode_buffer_.size());

            in_instr.flush();

            out_decoded_0.cancelInFlight();
            out_decoded_1.cancelInFlight();
            out_decoded_2.cancelInFlight();
            out_decoded_3.cancelInFlight();

            decode_buffer_.clear();
            first_packet_received_ = true;
            skip_next_validation_ = true;
            expected_hash_hint_ = 0;
            for (auto& pending : pending_sends_) {
                pending.reset();
            }
        }

        // Decode and dispatch up to decode_width instructions per cycle
        OutPort<DecodedOp>* ports[] = {&out_decoded_0, &out_decoded_1, &out_decoded_2,
                                       &out_decoded_3};

        uint32_t dispatched = 0;

        // First, try to dispatch any buffered decoded ops from previous cycles
        while (!decode_buffer_.empty() && dispatched < decode_width_) {
            auto& op = decode_buffer_.front();

            if (ports[op.dispatch_target]->canSend()) {
                bool sent = sendWithValidation(op, *ports[op.dispatch_target], op.dispatch_target);
                dispatch_lane_.instant(0, trace_cat::DISPATCH, "dispatch"_ev, flow(op.instr_id),
                                       arg<"alu">(op.dispatch_target));
                trace<"Dispatch buffered to ALU{}: instr_id={} op_type={}">(
                    trace_cat::DISPATCH, op.dispatch_target, op.instr_id,
                    static_cast<int>(op.op_type));
                decode_buffer_.pop_front();
                dispatched++;
                if (sent) {
                    ++decoded_;
                }
            } else {
                break;
            }
        }

        // Then fetch and decode new instructions
        while (dispatched < decode_width_) {
            auto instr_opt = in_instr.tryReceive(localCycle());
            if (!instr_opt) {
                break;
            }

            auto& instr = *instr_opt;

            validatePacket(instr, validation::hashInstruction);

            DecodedOp op{.op_type = static_cast<OpType>(instr.id % 4),
                         .dest_reg = static_cast<uint8_t>(instr.id % 32),
                         .src_reg1 = static_cast<uint8_t>((instr.id + 1) % 32),
                         .src_reg2 = static_cast<uint8_t>((instr.id + 2) % 32),
                         .imm = static_cast<int16_t>(instr.pc & 0xFFFF),
                         .instr_id = instr.id,
                         .pc = instr.pc,
                         .dispatch_target = static_cast<uint8_t>(dispatch_rr_ % 4)};

            trace<"Decode: instr_id={} pc=0x{:x} op_type={} dest_reg=r{} dispatch_to_alu{}">(
                trace_cat::DECODE, instr.id, instr.pc, static_cast<int>(op.op_type), op.dest_reg,
                op.dispatch_target);

            if (ports[op.dispatch_target]->canSend()) {
                bool sent = sendWithValidation(op, *ports[op.dispatch_target], op.dispatch_target);
                dispatch_lane_.instant(0, trace_cat::DISPATCH, "dispatch"_ev, flow(op.instr_id),
                                       arg<"alu">(op.dispatch_target));
                trace<"Dispatch to ALU{}: instr_id={} op_type={}">(trace_cat::DISPATCH,
                                                                   op.dispatch_target, op.instr_id,
                                                                   static_cast<int>(op.op_type));
                dispatch_rr_++;
                dispatched++;
                if (sent) {
                    ++decoded_;
                }
            } else {
                decode_buffer_.push_back(op);
                trace<"Buffering decode for ALU{}: instr_id={} buffer_size={}">(
                    trace_cat::DECODE, op.dispatch_target, op.instr_id, decode_buffer_.size());
                dispatch_rr_++;
                break;
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
                warn<"Decode Validation FAIL: expected=0x{:02x} actual=0x{:02x} instr_id={}">(
                    expected_hash_hint_, actual_hint, pkt.id);
            }
        }
        first_packet_received_ = false;
        expected_hash_hint_ = pkt.next_hash_hint;
    }

    bool sendWithValidation(DecodedOp current, OutPort<DecodedOp>& port, uint8_t alu_id) {
        bool sent = false;
        if (pending_sends_[alu_id]) {
            pending_sends_[alu_id]->next_hash_hint =
                validation::high8(validation::hashDecodedOp(current));
            sent = port.send(*pending_sends_[alu_id]);
            if (!sent) {
                return false;
            }
        }
        pending_sends_[alu_id] = current;
        return sent;
    }

    uint32_t decode_width_;
    uint32_t dispatch_rr_ = 0;
    std::deque<DecodedOp> decode_buffer_;

    bool first_packet_received_ = true;
    bool skip_next_validation_ = false;
    uint8_t expected_hash_hint_ = 0;
    uint64_t validation_pass_count_ = 0;
    uint64_t validation_fail_count_ = 0;

    std::array<std::optional<DecodedOp>, 4> pending_sends_;
};

}  // namespace cpu_pipeline
