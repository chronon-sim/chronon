// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <cassert>
#include <cstdint>
#include <iostream>
#include <vector>

#include "chronon/Chronon.hpp"

using namespace chronon;

struct ModeConfig {
    const char* name;
    bool enable_parallel;
    bool enable_lookahead;
};

TickSimulationConfig makeConfig(const ModeConfig& mode) {
    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = mode.enable_parallel;
    config.enable_lookahead = mode.enable_lookahead;
    config.enable_weighted_partitioning = false;
    config.enable_dynamic_rebalance = false;
    config.epoch_size = 8;
    config.max_lookahead_cycles = 4;
    return config;
}

class SleepForeverUnit : public TickableUnit {
public:
    explicit SleepForeverUnit(std::string name = "sleep_forever") : TickableUnit(std::move(name)) {}

    void tick() override {
        ++ticks;
        sleepForever();
    }

    uint64_t ticks = 0;
};

class IntervalUnit : public TickableUnit {
public:
    IntervalUnit() : TickableUnit("interval") { setTickInterval(3); }

    void tick() override { ++ticks; }

    uint64_t ticks = 0;
};

class DriverUnit : public TickableUnit {
public:
    OutPort<int> out{this, "out"};

    DriverUnit() : TickableUnit("driver") {}

    void tick() override {
        if (localCycle() == 5) {
            bool sent = out.send(42);
            (void)sent;
            assert(sent);
        }
    }
};

class SleepingReceiverUnit : public TickableUnit {
public:
    InPort<int> in{this, "in"};

    SleepingReceiverUnit() : TickableUnit("receiver") {}

    void tick() override {
        ++ticks;
        while (auto msg = in.tryReceive(localCycle())) {
            received.push_back(*msg);
            receive_cycles.push_back(localCycle());
        }
        sleepForever();
    }

    uint64_t ticks = 0;
    std::vector<int> received;
    std::vector<uint64_t> receive_cycles;
};

void test_sleep_forever_advances_cycle(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<SleepForeverUnit>();
    (void)unit;
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    sim.run(10);

    assert(unit->ticks == 1);
    assert(unit->localCycle() == 10);
}

void test_tick_interval(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<IntervalUnit>();
    (void)unit;
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    sim.run(10);

    assert(unit->ticks == 4);  // cycles 0, 3, 6, 9
    assert(unit->localCycle() == 10);
}

void test_port_arrival_wakes_sleeping_receiver(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* driver = sim.createUnit<DriverUnit>();
    auto* receiver = sim.createUnit<SleepingReceiverUnit>();
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }
    sim.connect(driver->out, receiver->in, 1);

    sim.run(12);

    assert(receiver->received.size() == 1);
    assert(receiver->received[0] == 42);
    assert(receiver->receive_cycles.size() == 1);
    assert(receiver->receive_cycles[0] == 6);
    assert(receiver->ticks == 2);  // initial poll at cycle 0, then port wake at cycle 6
    assert(receiver->localCycle() == 12);
}

int main() {
    const ModeConfig modes[] = {
        {"sequential", false, false},
        {"barrier", true, false},
        {"lookahead", true, true},
    };

    for (const auto& mode : modes) {
        std::cout << "Testing lazy wakeup sleepForever (" << mode.name << ")... ";
        test_sleep_forever_advances_cycle(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup tick_interval (" << mode.name << ")... ";
        test_tick_interval(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup port arrival (" << mode.name << ")... ";
        test_port_arrival_wakes_sleeping_receiver(mode);
        std::cout << "PASSED\n";
    }

    return 0;
}
