// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <iostream>

#include "chronon/Chronon.hpp"

using namespace chronon;

class Sm : public TickableUnit {
public:
    AsyncWritePort<uint64_t> requests{this, "requests"};
    uint64_t issued = 0;
    Sm() : TickableUnit("sm") {}
    void tick() override {
        if (issued < 100 && requests.canSend()) {
            // The packet, including data and a stable end-to-end identity, is owned.
            CdcPacket<uint64_t> packet{issued + 1, 0x1000 + issued * 64};
            if (requests.send(std::move(packet))) ++issued;
        }
    }
};
class Lts : public TickableUnit {
    std::optional<CdcPacket<uint64_t>> pending_;

public:
    AsyncReadPort<uint64_t> requests{this, "requests"};
    AsyncWritePort<uint64_t> memory{this, "memory"};
    Lts() : TickableUnit("lts") {}
    void tick() override {
        if (!pending_) pending_ = requests.take();
        if (pending_ && memory.send(std::move(*pending_))) pending_.reset();
        if (!pending_) requests.requestRead();
    }
};
class Dram : public TickableUnit {
public:
    AsyncReadPort<uint64_t> requests{this, "requests"};
    uint64_t received = 0;
    Dram() : TickableUnit("dram") {}
    void tick() override {
        // Illustrative backpressure; these are not calibrated GPU parameters.
        if (localCycle() % 5 == 0) {
            if (auto packet = requests.take()) {
                if (packet->transaction_id != received + 1)
                    requestError("transaction order failure");
                ++received;
            }
            requests.requestRead();
        }
    }
    bool isCompleted() const override { return received == 100; }
};

int main(int argc, char** argv) {
    TickSimulationConfig config;
    config.num_threads = 4;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "sm", 914'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "lts", 1'326'000'000, 1, SimTime::picoseconds(137)));
    sim.addClockDomain(ClockDomain::fromHz(3, "dram", 1'001'000'000, 1, SimTime::picoseconds(311)));
    auto* sm = sim.createUnitInDomain<Sm>(1);
    auto* lts = sim.createUnitInDomain<Lts>(2);
    auto* dram = sim.createUnitInDomain<Dram>(3);
    sim.connectAsyncFifo(1, sm->requests, lts->requests, {8, 2});
    sim.connectAsyncFifo(2, lts->memory, dram->requests, {4, 3});
    if (argc > 1) {
        ClockTraceRecorder::Config trace;
        trace.output_dir = argv[1];
        trace.run_id = "multiclock-example";
        sim.configureClockTrace(trace);
    }
    sim.initialize();
    sim.runUntil([&] { return dram->isCompleted(); }, 100'000);
    sim.drainCdc(10'000);
    sim.closeClockTrace();
    std::cout << "issued=" << sm->issued << " received=" << dram->received
              << " drained=" << sim.cdcDrained() << " last_time=" << sim.lastCommittedTime().str()
              << " scheduler_steps=" << sim.schedulerSteps() << '\n'
              << "policy: " << sim.parallelFallbackReason() << '\n';
    return sm->issued == 100 && dram->received == 100 && sim.cdcDrained() ? 0 : 1;
}
