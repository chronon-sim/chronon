// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_cpu_pipeline_flush_validation.cpp
//
// Regression test: frequent flushes should not break the CPU pipeline example.

#include <cassert>
#include <iostream>

#include "chronon/Chronon.hpp"
#include "cpu_pipeline_common.hpp"

using namespace chronon;
using namespace cpu_pipeline;

namespace {

class FlushInjector : public TickableUnit {
public:
    OutPort<FlushSignal> out_flush{this, "out_flush"};

    FlushInjector(uint64_t start_cycle, uint64_t period, uint32_t count)
        : TickableUnit("flush_injector"),
          start_cycle_(start_cycle),
          period_(period),
          remaining_(count) {}

    void tick() override {
        if (remaining_ == 0) {
            return;
        }
        uint64_t c = localCycle();
        if (c < start_cycle_) {
            return;
        }
        if (((c - start_cycle_) % period_) != 0) {
            return;
        }
        if (!out_flush.canSend()) {
            return;
        }

        // Redirect to 0 to maximize chance of stale/in-flight mixing.
        if (out_flush.send(FlushSignal{
                .flush_id = c,
                .redirect_pc = 0,
                .flush_point_id = c,
            })) {
            --remaining_;
        }
    }

    bool isCompleted() const override { return remaining_ == 0; }

private:
    uint64_t start_cycle_;
    uint64_t period_;
    uint32_t remaining_;
};

}  // namespace

int main() {
    std::cout << "=== CPU Pipeline Flush Validation Test ===\n\n";

    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;
    config.enable_lookahead = false;
    config.epoch_size = 64;

    TickSimulation sim(config);

    // Use large I$ so we avoid L2 stalls.
    auto* fetch = sim.createUnit<FetchUnit>(5000, 1024);
    auto* decode = sim.createUnit<DecodeUnit>(4);

    auto* alu0 = sim.createUnit<ALUUnit>(0, 0.0);
    auto* alu1 = sim.createUnit<ALUUnit>(1, 0.0);
    auto* alu2 = sim.createUnit<ALUUnit>(2, 0.0);
    auto* alu3 = sim.createUnit<ALUUnit>(3, 0.0);

    auto* writeback = sim.createUnit<WritebackUnit>(0);
    auto* l2 = sim.createUnit<L2CacheUnit>(10, 1000);

    // Flush injector: start at cycle 10, every 7 cycles, 12 flushes.
    auto* injector = sim.createUnit<FlushInjector>(10, 7, 12);

    // Main pipeline connections with higher delays to keep more in-flight state.
    sim.connect(fetch->out_instr, decode->in_instr, 5);
    sim.connect(decode->out_decoded_0, alu0->in_op, 5);
    sim.connect(decode->out_decoded_1, alu1->in_op, 5);
    sim.connect(decode->out_decoded_2, alu2->in_op, 5);
    sim.connect(decode->out_decoded_3, alu3->in_op, 5);
    sim.connect(alu0->out_result, writeback->in_result_0, 5);
    sim.connect(alu1->out_result, writeback->in_result_1, 5);
    sim.connect(alu2->out_result, writeback->in_result_2, 5);
    sim.connect(alu3->out_result, writeback->in_result_3, 5);

    // Memory (should be mostly idle).
    sim.connect(fetch->out_icache_miss, l2->in_req, 1);
    sim.connect(l2->out_resp, fetch->in_l2_resp, 1);

    // Flush broadcast to all pipeline units.
    sim.connect(injector->out_flush, fetch->in_flush, 1);
    sim.connect(injector->out_flush, decode->in_flush, 1);
    sim.connect(injector->out_flush, alu0->in_flush, 1);
    sim.connect(injector->out_flush, alu1->in_flush, 1);
    sim.connect(injector->out_flush, alu2->in_flush, 1);
    sim.connect(injector->out_flush, alu3->in_flush, 1);
    sim.connect(injector->out_flush, writeback->in_flush, 1);

    sim.initialize();

    // Run long enough for multiple flushes to interact with in-flight messages.
    sim.run(300);

    assert(decode->validationPassed());
    assert(alu0->validationPassed());
    assert(alu1->validationPassed());
    assert(alu2->validationPassed());
    assert(alu3->validationPassed());
    assert(writeback->validationPassed());

    std::cout << "PASSED\n";
    return 0;
}
