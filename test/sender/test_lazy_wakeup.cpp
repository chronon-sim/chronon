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

class InitiallyDeferredUnit : public TickableUnit {
public:
    InitiallyDeferredUnit() : TickableUnit("initially_deferred") { sleepUntil(5); }

    void tick() override {
        ++ticks;
        tick_cycles.push_back(localCycle());
    }

    uint64_t ticks = 0;
    std::vector<uint64_t> tick_cycles;
};

class InitiallyDeferredForeverUnit : public TickableUnit {
public:
    InitiallyDeferredForeverUnit() : TickableUnit("initially_deferred_forever") { sleepForever(); }

    void tick() override {
        ++ticks;
        tick_cycles.push_back(localCycle());
    }

    uint64_t ticks = 0;
    std::vector<uint64_t> tick_cycles;
};

class FutureWakeSleepUnit : public TickableUnit {
public:
    FutureWakeSleepUnit() : TickableUnit("future_wake_sleep") {}

    void tick() override {
        ++ticks;
        tick_cycles.push_back(localCycle());
        sleepForever();
    }

    uint64_t ticks = 0;
    std::vector<uint64_t> tick_cycles;
};

class OverrideSleepTargetUnit : public TickableUnit {
public:
    OverrideSleepTargetUnit() : TickableUnit("override_sleep") {}

    void tick() override {
        ++ticks;
        tick_cycles.push_back(localCycle());
        if (localCycle() == 0) {
            sleepUntil(5);
            sleepForever();
        } else {
            sleepForever();
        }
    }

    uint64_t ticks = 0;
    std::vector<uint64_t> tick_cycles;
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

class TwoMessageDriverUnit : public TickableUnit {
public:
    OutPort<int> out{this, "out"};

    TwoMessageDriverUnit() : TickableUnit("two_message_driver") {}

    void tick() override {
        if (localCycle() == 0 || localCycle() == 1) {
            bool sent = out.send(static_cast<int>(localCycle()));
            (void)sent;
            assert(sent);
        }
    }
};

class EarlyMessageDriverUnit : public TickableUnit {
public:
    OutPort<int> out{this, "out"};

    EarlyMessageDriverUnit() : TickableUnit("early_message_driver") {}

    void tick() override {
        if (localCycle() == 0) {
            bool sent = out.send(7);
            (void)sent;
            assert(sent);
        }
    }
};

class SleepingReceiverUnit : public TickableUnit {
public:
    InPort<int> in{this, "in"};

    SleepingReceiverUnit() : TickableUnit("receiver") { enableActivityScheduling(); }

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

class LateSleepingReceiverUnit : public TickableUnit {
public:
    InPort<int> in{this, "in"};

    LateSleepingReceiverUnit() : TickableUnit("late_sleeping_receiver") {}

    void tick() override {
        ++ticks;
        while (auto msg = in.tryReceive(localCycle())) {
            received.push_back(*msg);
            receive_cycles.push_back(localCycle());
        }
        if (localCycle() >= 2) {
            sleepForever();
        }
    }

    uint64_t ticks = 0;
    std::vector<int> received;
    std::vector<uint64_t> receive_cycles;
};

class IntervalSleepingReceiverUnit : public TickableUnit {
public:
    InPort<int> in{this, "in"};

    IntervalSleepingReceiverUnit() : TickableUnit("interval_sleeping_receiver") {
        setTickInterval(3);
    }

    void tick() override {
        ++ticks;
        tick_cycles.push_back(localCycle());
        bool received_this_tick = false;
        while (auto msg = in.tryReceive(localCycle())) {
            received.push_back(*msg);
            receive_cycles.push_back(localCycle());
            received_this_tick = true;
        }
        if (received_this_tick) {
            sleepForever();
        }
    }

    uint64_t ticks = 0;
    std::vector<uint64_t> tick_cycles;
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

void test_initial_sleep_until_defers_first_tick(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<InitiallyDeferredUnit>();
    (void)unit;
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    sim.run(8);

    assert(unit->ticks == 3);  // cycles 5, 6, 7
    assert((unit->tick_cycles == std::vector<uint64_t>{5, 6, 7}));
    assert(unit->localCycle() == 8);
}

void test_tick_interval_preserves_constructor_sleep_target(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<InitiallyDeferredUnit>();
    unit->setTickInterval(3);
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    sim.run(10);

    assert(unit->ticks == 2);  // sleepUntil(5) plus interval 3 -> cycles 6, 9
    assert((unit->tick_cycles == std::vector<uint64_t>{6, 9}));
    assert(unit->localCycle() == 10);
}

void test_tick_interval_preserves_constructor_sleep_forever(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<InitiallyDeferredForeverUnit>();
    unit->setTickInterval(3);
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    sim.run(10);

    assert(unit->ticks == 0);
    assert(unit->tick_cycles.empty());
    assert(unit->localCycle() == 10);
}

void test_future_wake_survives_sleep_after_initial_tick(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<FutureWakeSleepUnit>();
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    unit->wakeAt(5);
    sim.run(8);

    assert(unit->ticks == 2);  // default cycle 0 tick, then preserved wake at cycle 5
    assert((unit->tick_cycles == std::vector<uint64_t>{0, 5}));
    assert(unit->localCycle() == 8);
}

void test_multiple_future_wake_requests_survive_sleep_after_initial_tick(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<FutureWakeSleepUnit>();
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    unit->wakeAt(5);
    unit->wakeAt(7);
    sim.run(10);

    assert(unit->ticks == 3);  // default cycle 0 tick, then preserved wakes at 5 and 7
    assert((unit->tick_cycles == std::vector<uint64_t>{0, 5, 7}));
    assert(unit->localCycle() == 10);
}

void test_later_sleep_overrides_prior_sleep_target_in_same_tick(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<OverrideSleepTargetUnit>();
    (void)unit;
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    sim.run(8);

    assert(unit->ticks == 1);
    assert((unit->tick_cycles == std::vector<uint64_t>{0}));
    assert(unit->localCycle() == 8);
}

void test_explicit_wake_survives_later_sleep_override_in_same_tick(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* unit = sim.createUnit<OverrideSleepTargetUnit>();
    (void)unit;
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }

    unit->wakeAt(5);
    sim.run(8);

    assert(unit->ticks == 2);
    assert((unit->tick_cycles == std::vector<uint64_t>{0, 5}));
    assert(unit->localCycle() == 8);
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

void test_multiple_future_port_arrivals_keep_later_wake(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* driver = sim.createUnit<TwoMessageDriverUnit>();
    auto* receiver = sim.createUnit<SleepingReceiverUnit>();
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }
    sim.connect(driver->out, receiver->in, 5);

    sim.run(8);

    assert((receiver->received == std::vector<int>{0, 1}));
    assert((receiver->receive_cycles == std::vector<uint64_t>{5, 6}));
    assert(receiver->ticks == 3);  // initial poll at cycle 0, then wakes at 5 and 6
    assert(receiver->localCycle() == 8);
}

void test_delayed_port_arrival_before_first_sleep_is_preserved(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* driver = sim.createUnit<EarlyMessageDriverUnit>();
    auto* receiver = sim.createUnit<LateSleepingReceiverUnit>();
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }
    sim.connect(driver->out, receiver->in, 5);

    sim.run(8);

    assert((receiver->received == std::vector<int>{7}));
    assert((receiver->receive_cycles == std::vector<uint64_t>{5}));
    assert(receiver->ticks == 4);  // cycles 0, 1, 2, then preserved wake at cycle 5
    assert(receiver->localCycle() == 8);
}

void test_interval_port_wake_is_consumed_before_sleep(const ModeConfig& mode) {
    TickSimulation sim(makeConfig(mode));
    auto* driver = sim.createUnit<EarlyMessageDriverUnit>();
    auto* receiver = sim.createUnit<IntervalSleepingReceiverUnit>();
    for (int i = 0; i < 8; ++i) {
        sim.createUnit<SleepForeverUnit>("filler_" + std::to_string(i));
    }
    sim.connect(driver->out, receiver->in, 1);

    sim.run(8);

    assert((receiver->received == std::vector<int>{7}));
    assert((receiver->receive_cycles == std::vector<uint64_t>{3}));
    assert((receiver->tick_cycles == std::vector<uint64_t>{0, 3}));
    assert(receiver->ticks == 2);
    assert(receiver->localCycle() == 8);
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

        std::cout << "Testing lazy wakeup initial sleepUntil (" << mode.name << ")... ";
        test_initial_sleep_until_defers_first_tick(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup tick_interval preserves sleepUntil (" << mode.name
                  << ")... ";
        test_tick_interval_preserves_constructor_sleep_target(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup tick_interval preserves sleepForever (" << mode.name
                  << ")... ";
        test_tick_interval_preserves_constructor_sleep_forever(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup pending future wake (" << mode.name << ")... ";
        test_future_wake_survives_sleep_after_initial_tick(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup multiple explicit future wakes (" << mode.name << ")... ";
        test_multiple_future_wake_requests_survive_sleep_after_initial_tick(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup later sleep overrides prior sleep target (" << mode.name
                  << ")... ";
        test_later_sleep_overrides_prior_sleep_target_in_same_tick(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup explicit wake survives sleep override (" << mode.name
                  << ")... ";
        test_explicit_wake_survives_later_sleep_override_in_same_tick(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup port arrival (" << mode.name << ")... ";
        test_port_arrival_wakes_sleeping_receiver(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup multiple future arrivals (" << mode.name << ")... ";
        test_multiple_future_port_arrivals_keep_later_wake(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup delayed arrival before first sleep (" << mode.name
                  << ")... ";
        test_delayed_port_arrival_before_first_sleep_is_preserved(mode);
        std::cout << "PASSED\n";

        std::cout << "Testing lazy wakeup interval wake consumed before sleep (" << mode.name
                  << ")... ";
        test_interval_port_wake_is_consumed_before_sleep(mode);
        std::cout << "PASSED\n";
    }

    return 0;
}
