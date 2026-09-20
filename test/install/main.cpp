// SPDX-License-Identifier: MPL-2.0
#include "chronon/Simulation.hpp"

using namespace chronon;

struct Source : TickableUnit {
    AsyncWritePort<int> out{this, "out"};
    bool sent = false;
    Source() : TickableUnit("source") {}
    void tick() override {
        if (!sent) {
            CdcPacket<int> packet{1, 42};
            sent = out.send(std::move(packet));
        }
    }
};
struct Sink : TickableUnit {
    AsyncReadPort<int> in{this, "in"};
    int value = 0;
    Sink() : TickableUnit("sink") {}
    void tick() override {
        if (auto packet = in.take()) value = packet->data;
        in.requestRead();
    }
};
int main() {
    TickSimulationConfig config;
    config.setExecutionPolicy(ExecutionPolicy::Sequential);
    config.num_threads = 1;
    TickSimulation sim(config);
    sim.addClockDomain(ClockDomain::fromHz(1, "sm", 914'000'000));
    sim.addClockDomain(ClockDomain::fromHz(2, "lts", 1'326'000'000));
    auto* source = sim.createUnitInDomain<Source>(1);
    auto* sink = sim.createUnitInDomain<Sink>(2);
    sim.connectAsyncFifo(1, source->out, sink->in, {4, 2});
    sim.initialize();
    sim.runClockEvents(100);
    return source->sent && sink->value == 42 ? 0 : 1;
}
