// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

// [quickstart]
#include "chronon/Chronon.hpp"

using namespace chronon;

class Producer : public TickableUnit {
public:
    OutPort<int> out{this, "out", SendRate{1}};

    Producer() : TickableUnit("producer") {}

    void tick() override {
        if (next_ < 100 && out.send(next_)) ++next_;
    }

private:
    int next_ = 0;
};

class Consumer : public TickableUnit {
public:
    InPort<int> in{this, "in", QueueDepth{16}};
    int sum = 0;

    Consumer() : TickableUnit("consumer") {}

    void tick() override {
        if (auto value = in.tryReceive()) sum += *value;
    }
};

int main() {
    TickSimulationConfig config;
    config.num_threads = 2;
    config.setExecutionPolicy(ExecutionPolicy::Auto);

    TickSimulation sim(config);
    auto* producer = sim.createUnit<Producer>();
    auto* consumer = sim.createUnit<Consumer>();
    sim.connect(producer->out, consumer->in, 1);

    sim.initialize();
    sim.run(101);
    sim.finalize();
    return consumer->sum == 4950 ? 0 : 1;
}
// [/quickstart]
