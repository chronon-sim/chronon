// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <algorithm>
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "chronon/Chronon.hpp"

using namespace chronon::sender;

namespace {

class SourceUnit : public TickableUnit {
public:
    explicit SourceUnit(std::string name, int value)
        : TickableUnit(std::move(name)), value_(value) {}

    OutPort<int> out{this, "out"};

    void tick() override {
        if (!sent_) {
            send_succeeded_ = out.send(value_);
            sent_ = true;
        }
    }

    bool sendSucceeded() const noexcept { return send_succeeded_; }

private:
    int value_ = 0;
    bool sent_ = false;
    bool send_succeeded_ = false;
};

class RelayUnit : public TickableUnit {
public:
    explicit RelayUnit(std::string name) : TickableUnit(std::move(name)) {}

    InPort<int> in{this, "in", 256};
    OutPort<int> out{this, "out"};

    void tick() override {
        while (in.tryReceive(localCycle()).has_value()) {
            // Drain input to avoid queue growth in tight-coupling links.
        }
    }
};

class SinkUnit : public TickableUnit {
public:
    explicit SinkUnit(std::string name) : TickableUnit(std::move(name)) {}

    InPort<int> in{this, "in", 8192};

    void tick() override {
        while (auto value = in.tryReceive(localCycle())) {
            received_.push_back(*value);
        }
    }

    const std::vector<int>& received() const noexcept { return received_; }

private:
    std::vector<int> received_;
};

class ManualUnit : public Unit {
public:
    explicit ManualUnit(std::string name) : Unit(std::move(name)) {}

    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
};

void test_tick_simulation_mpsc_delivery() {
    std::cout << "Testing TickSimulation MPSC delivery... ";

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    // Consumer-tick-driven MPSC arbitration drains staging inside each
    // receiver tick; end-of-epoch flush via arbitrateAllMPSCPorts_
    // handles any tail entries not picked up this epoch.
    config.epoch_size = 4;

    TickSimulation sim(config);

    auto* src0 = sim.createUnit<SourceUnit>("src0", 100);
    auto* src1 = sim.createUnit<SourceUnit>("src1", 200);

    // Build two tight-coupling clusters so sources are assigned to different
    // threads and share one MPSC destination.
    auto* a0 = sim.createUnit<RelayUnit>("a0");
    auto* b0 = sim.createUnit<RelayUnit>("b0");
    auto* a1 = sim.createUnit<RelayUnit>("a1");
    auto* sink = sim.createUnit<SinkUnit>("sink");

    // Tight (delay=0) connections for cluster shaping.
    sim.connect(src0->out, a0->in, 0);
    sim.connect(a0->out, b0->in, 0);
    sim.connect(src1->out, a1->in, 0);

    // Two producers to one destination port (MPSC).
    sim.connect(src0->out, sink->in, 1);
    sim.connect(src1->out, sink->in, 1);

    sim.initialize();
    sim.run(2);

    assert(src0->sendSucceeded());
    assert(src1->sendSucceeded());

    std::vector<int> received = sink->received();
    std::sort(received.begin(), received.end());
    assert(received.size() == 2);
    assert(received[0] == 100);
    assert(received[1] == 200);

    std::cout << "PASSED\n";
}

void test_lockfree_backpressure_contract() {
    std::cout << "Testing lock-free backpressure contract... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out"};
    InPort<int> in{&cons, "in", 8};
    out.connect(&in, 1);

    in.useLockFreeQueue();
    prod.setCycle(0);

    [[maybe_unused]] size_t sent = 0;
    [[maybe_unused]] bool send_failed_while_can_send = false;
    for (size_t i = 0; i < 10000; ++i) {
        if (!out.canSend()) {
            break;
        }
        if (!out.send(static_cast<int>(i))) {
            send_failed_while_can_send = true;
            break;
        }
        sent++;
    }

    assert(sent > 0);
    assert(!send_failed_while_can_send);
    assert(!out.canSend());
    assert(in.queuedMessageCount() == sent);

    // Destination should now reject additional traffic until consumed.
    // Since LockFreeQueueAdapter::setCapacity now honors the declared
    // bound physically, this push actually fails at the ring level —
    // not just at the canSend() snapshot check.
    assert(!out.send(99999));

    // Drain one entry and cross the cycle boundary. The per-connection
    // "pushes this cycle" counter clears on a cycle advance, matching
    // what TickableUnit::executeTick does at each clock edge.
    [[maybe_unused]] auto v = in.tryReceive(1);
    assert(v.has_value());
    prod.setCycle(1);
    assert(out.canSend());

    std::cout << "PASSED\n";
}

void test_small_non_tight_graph_parallel_fallback() {
    std::cout << "Testing small non-tight graph parallel fallback... ";

    TickSimulationConfig config;
    config.num_threads = 4;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_weighted_partitioning = false;
    config.epoch_size = 8;

    TickSimulation sim(config);

    auto* src = sim.createUnit<SourceUnit>("src", 7);
    auto* relay0 = sim.createUnit<RelayUnit>("relay0");
    auto* relay1 = sim.createUnit<RelayUnit>("relay1");
    auto* relay2 = sim.createUnit<RelayUnit>("relay2");
    auto* relay3 = sim.createUnit<RelayUnit>("relay3");
    auto* relay4 = sim.createUnit<RelayUnit>("relay4");
    auto* relay5 = sim.createUnit<RelayUnit>("relay5");
    auto* relay6 = sim.createUnit<RelayUnit>("relay6");
    auto* sink = sim.createUnit<SinkUnit>("sink");

    // No tight edges: all connections have delay=1.
    sim.connect(src->out, relay0->in, 1);
    sim.connect(src->out, relay1->in, 1);
    sim.connect(src->out, relay2->in, 1);
    sim.connect(src->out, relay3->in, 1);
    sim.connect(src->out, relay4->in, 1);
    sim.connect(src->out, relay5->in, 1);
    sim.connect(src->out, relay6->in, 1);
    sim.connect(relay0->out, sink->in, 1);
    sim.connect(relay1->out, sink->in, 1);
    sim.connect(relay2->out, sink->in, 1);
    sim.connect(relay3->out, sink->in, 1);
    sim.connect(relay4->out, sink->in, 1);
    sim.connect(relay5->out, sink->in, 1);
    sim.connect(relay6->out, sink->in, 1);
    sim.connect(src->out, sink->in, 1);

    sim.initialize();

    assert(!sim.hasTightConnectionsInGraph());
    assert(!sim.isParallelBeneficial());
    assert(!sim.useParallelExecution());

    sim.run(2);
    assert(src->sendSucceeded());

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Thread Queue Hardening Tests ===\n\n";

    test_tick_simulation_mpsc_delivery();
    test_lockfree_backpressure_contract();
    test_small_non_tight_graph_parallel_fallback();

    std::cout << "\n=== Thread queue hardening tests PASSED ===\n";
    return 0;
}
