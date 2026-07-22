// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "sender/core/TickSimulation.hpp"

using namespace chronon::sender;

namespace {

int failures = 0;

void check(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}

uint64_t valueKey(const uint64_t& value) { return value; }

class PassiveProducer : public TickableUnit {
public:
    PassiveProducer(std::string name, size_t rate = 8)
        : TickableUnit(std::move(name)), out(this, "out", rate) {}

    void tick() override {}
    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }

    OutPort<uint64_t> out;
};

class InitializingProducer : public PassiveProducer {
public:
    explicit InitializingProducer(std::string name) : PassiveProducer(std::move(name)) {}

    void initialize() override { check(out.send(7), "initialize-time send failed"); }
};

class PassiveConsumer : public TickableUnit {
public:
    explicit PassiveConsumer(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {}
    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }

    InPort<uint64_t> in{this, "in"};
};

class GappedProducer : public TickableUnit {
public:
    explicit GappedProducer(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {
        if (localCycle() == 0 || localCycle() == 2) {
            check(out.send(localCycle()), "gapped broadcast send failed");
        }
    }

    OutPort<uint64_t> out{this, "out", 1};
};

class LateSleepingConsumer : public TickableUnit {
public:
    LateSleepingConsumer(std::string name, bool sleep_after_receive)
        : TickableUnit(std::move(name)), sleep_after_receive_(sleep_after_receive) {}

    void tick() override {
        tick_cycles_.push_back(localCycle());
        while (auto value = in.tryReceive(localCycle())) received_.push_back(*value);
        if (sleep_after_receive_ && localCycle() >= 1) sleepForever();
    }

    const std::vector<uint64_t>& received() const noexcept { return received_; }
    const std::vector<uint64_t>& tickCycles() const noexcept { return tick_cycles_; }

    InPort<uint64_t> in{this, "in"};

private:
    bool sleep_after_receive_ = false;
    std::vector<uint64_t> received_;
    std::vector<uint64_t> tick_cycles_;
};

class StageSelectiveConsumer : public TickableUnit {
public:
    explicit StageSelectiveConsumer(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {}

    InPort<uint64_t> in{this, "in", PortPolicy::StageSelective};
};

class InitializingCancelConsumer : public TickableUnit {
public:
    explicit InitializingCancelConsumer(std::string name) : TickableUnit(std::move(name)) {}

    void initialize() override { in.cancelYoungerThan<&valueKey>(50); }
    void tick() override {}

    InPort<uint64_t> in{this, "in"};
};

class MoveOnlyProducer : public TickableUnit {
public:
    explicit MoveOnlyProducer(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {}

    OutPort<std::unique_ptr<int>> out{this, "out", 0};
};

class MoveOnlyConsumer : public TickableUnit {
public:
    explicit MoveOnlyConsumer(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {}

    InPort<std::unique_ptr<int>> in{this, "in"};
};

struct PassiveBus {
    std::array<PassiveProducer*, 2> producers{};
    std::array<PassiveConsumer*, 4> consumers{};
};

PassiveBus makePassiveBus(TickSimulation& simulation) {
    PassiveBus bus;
    for (size_t producer = 0; producer < bus.producers.size(); ++producer) {
        bus.producers[producer] =
            simulation.createUnit<PassiveProducer>("producer" + std::to_string(producer));
    }
    for (size_t consumer = 0; consumer < bus.consumers.size(); ++consumer) {
        bus.consumers[consumer] =
            simulation.createUnit<PassiveConsumer>("consumer" + std::to_string(consumer));
    }
    for (auto* producer : bus.producers) {
        for (auto* consumer : bus.consumers) {
            simulation.connect(producer->out, consumer->in, 1);
        }
    }
    return bus;
}

std::vector<uint64_t> drainTryReceive(InPort<uint64_t>& port, uint64_t cycle) {
    std::vector<uint64_t> values;
    while (auto value = port.tryReceive(cycle)) values.push_back(*value);
    return values;
}

void testAutomaticSelectionAndPortSemantics() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto bus = makePassiveBus(simulation);
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == 8,
          "complete 2x4 delay-one bus was not selected automatically");
    for (auto* producer : bus.producers) {
        check(producer->out.transparentBroadcastEnabled(),
              "producer OutPort did not switch to shared transport");
    }
    for (auto* consumer : bus.consumers) {
        check(consumer->in.usesTransparentBroadcast(),
              "consumer InPort did not switch to shared replay");
        check(!consumer->in.hasData(0), "delay-one payload became visible in the send cycle");
    }
    bool reconfiguration_rejected = false;
    try {
        bus.consumers[0]->in.useSingleThreadQueue();
    } catch (const std::logic_error&) {
        reconfiguration_rejected = true;
    }
    check(reconfiguration_rejected,
          "queue reconfiguration silently detached an active shared transport");

    check(bus.producers[1]->out.send(90), "pre-cancel producer 1 send failed");
    bus.consumers[0]->setCycle(1);
    bus.consumers[0]->in.cancelYoungerThan<&valueKey>(50);
    bus.producers[1]->setCycle(1);
    check(bus.producers[1]->out.send(91), "post-cancel producer 1 send failed");

    check(bus.producers[0]->out.send(10), "pre-cancel producer 0 send failed");
    bus.producers[0]->out.cancelInFlight();
    check(bus.producers[0]->out.send(11), "post-cancel producer 0 send failed");

    check(!bus.consumers[0]->in.hasData(0),
          "shared queue facade exposed a future payload as ready");
    check(bus.consumers[0]->in.hasData(1), "shared queue facade did not expose a ready payload");
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    constexpr size_t expected_queued_messages = 3;
#else
    constexpr size_t expected_queued_messages = 4;
#endif
    check(bus.consumers[0]->in.queuedMessageCount() == expected_queued_messages,
          "shared queue facade reported the wrong compile-time cancellation semantics");
    check(bus.consumers[0]->in.capacity() == InPort<uint64_t>::UNLIMITED_CAPACITY &&
              bus.consumers[0]->in.available() == InPort<uint64_t>::UNLIMITED_CAPACITY,
          "shared queue facade introduced model-visible backpressure");
    check(bus.consumers[0]->in.minArrivalCycle() == 1,
          "shared transport reported the wrong earliest arrival");
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    const std::vector<uint64_t> consumer0_expected{11, 91};
    const std::vector<uint64_t> other_expected{11, 90, 91};
#else
    const std::vector<uint64_t> consumer0_expected{10, 11, 91};
    const std::vector<uint64_t> other_expected{10, 11, 90, 91};
#endif
    const auto consumer0_actual = drainTryReceive(bus.consumers[0]->in, 2);
    if (consumer0_actual != consumer0_expected) {
        std::cerr << "consumer0 actual:";
        for (uint64_t value : consumer0_actual) std::cerr << ' ' << value;
        std::cerr << '\n';
    }
    check(consumer0_actual == consumer0_expected,
          "sender or receiver cancellation semantics changed");
    check(bus.consumers[1]->in.receiveAll(2) == other_expected,
          "receiveAll lost deterministic producer order");
    const auto& buffered = bus.consumers[2]->in.receiveAllBuffered(2);
    check(buffered == other_expected, "receiveAllBuffered differs from queue transport");
    check(drainTryReceive(bus.consumers[3]->in, 2) == other_expected,
          "shared replay was destructive across consumers");
    for (auto* consumer : bus.consumers) {
        check(consumer->in.queuedMessageCount() == 0,
              "shared queue facade retained a drained payload");
    }

    check(bus.producers[0]->out.send(12), "flush test send failed");
    bus.consumers[3]->in.flush();
    for (size_t consumer = 0; consumer < 3; ++consumer) {
        check(drainTryReceive(bus.consumers[consumer]->in, 1) == std::vector<uint64_t>({12}),
              "flush on one consumer affected another consumer cursor");
    }
    check(!bus.consumers[3]->in.hasData(1), "flush did not drop future shared payloads");
    check(bus.consumers[3]->in.queuedMessageCount() == 0,
          "flush did not clear the shared queue facade");
}

void testReceiverFilterIsCursorLocal() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto* producer = simulation.createUnit<PassiveProducer>("producer");
    std::array<PassiveConsumer*, 4> consumers{};
    for (size_t i = 0; i < consumers.size(); ++i) {
        consumers[i] = simulation.createUnit<PassiveConsumer>("consumer" + std::to_string(i));
        simulation.connect(producer->out, consumers[i]->in, 1);
    }
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == consumers.size(),
          "filter test did not select transparent broadcast");
    for (uint64_t value = 1; value <= 4; ++value) {
        check(producer->out.send(value), "shared filter test send failed");
    }

    size_t inspected = 0;
    auto keep_even = [&](const uint64_t& value) noexcept {
        ++inspected;
        return (value & 1U) == 0;
    };
    check(consumers[0]->in.tryReceiveFiltered(1, keep_even) == std::optional<uint64_t>{2},
          "shared receiver filter did not skip its first rejected payload");
    check(consumers[0]->in.tryReceiveFiltered(1, keep_even) == std::optional<uint64_t>{4},
          "shared receiver filter changed accepted-payload order");
    check(!consumers[0]->in.tryReceiveFiltered(1, keep_even).has_value(),
          "shared receiver filter left a ready payload behind");
    check(inspected == 4, "shared receiver filter inspected a payload more than once");

    const std::vector<uint64_t> all_values{1, 2, 3, 4};
    check(consumers[1]->in.receiveAll(1) == all_values,
          "filtering one shared cursor destructively changed another consumer");
    check(consumers[2]->in.receiveAll(1) == all_values,
          "shared cursor replay diverged after another consumer filtered");
}

void testLateActivityOptInInvalidatesWakeCache() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto* producer = simulation.createUnit<GappedProducer>("gapped_producer");
    auto* sleeping = simulation.createUnit<LateSleepingConsumer>("late_sleeping_consumer", true);
    simulation.connect(producer->out, sleeping->in, 1);
    for (size_t index = 0; index < 3; ++index) {
        auto* active = simulation.createUnit<LateSleepingConsumer>(
            "active_consumer" + std::to_string(index), false);
        simulation.connect(producer->out, active->in, 1);
    }

    simulation.run(5);

    check(simulation.transparentBroadcastConnectionCount() == 4,
          "late-activity test did not select transparent broadcast");
    check(sleeping->received() == std::vector<uint64_t>({0, 2}),
          "late activity opt-in missed a later broadcast wakeup");
    check(std::find(sleeping->tickCycles().begin(), sleeping->tickCycles().end(), 2) ==
              sleeping->tickCycles().end(),
          "late-activity consumer did not sleep across the producer gap");
    check(std::find(sleeping->tickCycles().begin(), sleeping->tickCycles().end(), 3) !=
              sleeping->tickCycles().end(),
          "broadcast wake cache was not invalidated after activity opt-in");
}

void testNucleusScaleDelayOneBusReplay() {
    constexpr size_t kProducerCount = 10;
    constexpr size_t kConsumerCount = 11;
    constexpr size_t kMessagesPerProducerCycle = 3;
    constexpr uint64_t kCycles = 32;

    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    std::array<PassiveProducer*, kProducerCount> producers{};
    std::array<PassiveConsumer*, kConsumerCount> consumers{};
    for (size_t producer = 0; producer < producers.size(); ++producer) {
        producers[producer] = simulation.createUnit<PassiveProducer>(
            "nucleus_producer" + std::to_string(producer), kMessagesPerProducerCycle);
    }
    for (size_t consumer = 0; consumer < consumers.size(); ++consumer) {
        consumers[consumer] =
            simulation.createUnit<PassiveConsumer>("nucleus_consumer" + std::to_string(consumer));
    }
    for (auto* producer : producers) {
        for (auto* consumer : consumers) {
            simulation.connect(producer->out, consumer->in, 1);
        }
    }
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == kProducerCount * kConsumerCount,
          "complete Nucleus-scale delay-one bus was not selected automatically");
    for (uint64_t cycle = 0; cycle < kCycles; ++cycle) {
        std::vector<uint64_t> expected;
        expected.reserve(kProducerCount * kMessagesPerProducerCycle);
        for (size_t producer = 0; producer < producers.size(); ++producer) {
            producers[producer]->setCycle(cycle);
            for (size_t message = 0; message < kMessagesPerProducerCycle; ++message) {
                const uint64_t value = (cycle << 16) | (producer << 8) | message;
                check(producers[producer]->out.send(value),
                      "Nucleus-scale producer failed to publish");
                expected.push_back(value);
            }
        }
        for (auto* consumer : consumers) {
            check(drainTryReceive(consumer->in, cycle + 1) == expected,
                  "Nucleus-scale consumer changed producer or in-cycle order");
        }
    }
}

void testCachedHeadInvalidation() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto bus = makePassiveBus(simulation);
    simulation.initialize();

    check(bus.producers[0]->out.send(10), "cached-cancel producer 0 send failed");
    check(bus.producers[1]->out.send(90), "cached-cancel producer 1 send failed");
    check(bus.consumers[0]->in.tryReceive(1) == std::optional<uint64_t>{10},
          "cached-cancel setup did not select the first producer");
    bus.producers[1]->out.cancelInFlight();
    bus.producers[1]->setCycle(1);
    check(bus.producers[1]->out.send(91), "cached-cancel replacement send failed");
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    check(!bus.consumers[0]->in.tryReceive(1),
          "sender cancellation did not invalidate a cached lane head");
    check(bus.consumers[0]->in.tryReceive(2) == std::optional<uint64_t>{91},
          "sender cancellation lost a later replacement payload");
#else
    check(bus.consumers[0]->in.tryReceive(1) == std::optional<uint64_t>{90},
          "disabled sender cancellation changed cached replay");
    check(bus.consumers[0]->in.tryReceive(2) == std::optional<uint64_t>{91},
          "disabled sender cancellation lost the replacement payload");
#endif

    check(bus.producers[0]->out.send(20), "cached-flush producer 0 send failed");
    check(bus.producers[1]->out.send(21), "cached-flush producer 1 send failed");
    check(bus.consumers[1]->in.tryReceive(1) == std::optional<uint64_t>{10},
          "cached-flush setup did not consume the oldest first lane head");
    bus.consumers[1]->in.flush();
    check(!bus.consumers[1]->in.tryReceive(1), "flush retained a cached lane head");
    bus.producers[0]->setCycle(1);
    check(bus.producers[0]->out.send(22), "post-flush producer 0 send failed");
    check(bus.producers[1]->out.send(23), "post-flush producer 1 send failed");
    check(drainTryReceive(bus.consumers[1]->in, 2) == std::vector<uint64_t>({22, 23}),
          "post-flush replay reused stale cached heads");
}

void testFrontierRefreshesSparseAndFutureLanes() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    std::array<PassiveProducer*, 3> producers{};
    std::array<PassiveConsumer*, 4> consumers{};
    for (size_t producer = 0; producer < producers.size(); ++producer) {
        producers[producer] =
            simulation.createUnit<PassiveProducer>("frontier_producer" + std::to_string(producer));
    }
    for (size_t consumer = 0; consumer < consumers.size(); ++consumer) {
        consumers[consumer] =
            simulation.createUnit<PassiveConsumer>("frontier_consumer" + std::to_string(consumer));
    }
    for (auto* producer : producers) {
        for (auto* consumer : consumers) simulation.connect(producer->out, consumer->in, 1);
    }
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == producers.size() * consumers.size(),
          "generic sparse frontier topology did not select transparent broadcast");

    producers[0]->setCycle(4);
    check(producers[0]->out.send(400), "future frontier publish failed");
    check(!consumers[0]->in.hasData(2), "future frontier head became ready too early");

    producers[1]->setCycle(2);
    check(producers[1]->out.send(200), "earlier sparse-lane publish failed");
    check(consumers[0]->in.tryReceive(3) == std::optional<uint64_t>{200},
          "cached future head hid an earlier sparse-lane publish");
    check(!consumers[0]->in.tryReceive(3), "sparse frontier retained a ready payload");

    producers[2]->setCycle(2);
    check(producers[2]->out.send(201), "same-cycle late sparse-lane publish failed");
    check(consumers[0]->in.tryReceive(3) == std::optional<uint64_t>{201},
          "empty frontier lookup hid a same-cycle late publish");
    check(consumers[0]->in.tryReceive(5) == std::optional<uint64_t>{400},
          "frontier lost its cached future head");
}

void testFrontierPreservesMultiCycleBacklogOrder() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    std::array<PassiveProducer*, 3> producers{};
    std::array<PassiveConsumer*, 4> consumers{};
    for (size_t producer = 0; producer < producers.size(); ++producer) {
        producers[producer] =
            simulation.createUnit<PassiveProducer>("backlog_producer" + std::to_string(producer));
    }
    for (size_t consumer = 0; consumer < consumers.size(); ++consumer) {
        consumers[consumer] =
            simulation.createUnit<PassiveConsumer>("backlog_consumer" + std::to_string(consumer));
    }
    for (auto* producer : producers) {
        for (auto* consumer : consumers) simulation.connect(producer->out, consumer->in, 1);
    }
    simulation.initialize();

    producers[2]->setCycle(0);
    check(producers[2]->out.send(20), "backlog producer 2 cycle 0 send failed");
    producers[0]->setCycle(0);
    check(producers[0]->out.send(0), "backlog producer 0 cycle 0 send failed");
    producers[1]->setCycle(0);
    check(producers[1]->out.send(10), "backlog producer 1 cycle 0 send failed");
    producers[2]->setCycle(1);
    check(producers[2]->out.send(21), "backlog producer 2 cycle 1 send failed");
    producers[0]->setCycle(1);
    check(producers[0]->out.send(1), "backlog producer 0 cycle 1 send failed");

    const std::vector<uint64_t> expected{0, 10, 20, 1, 21};
    check(drainTryReceive(consumers[0]->in, 2) == expected,
          "frontier changed arrival-cycle or stable producer order");
    check(consumers[1]->in.receiveAll(2) == expected,
          "frontier ordering differed across independent consumers");
}

void testUnsafeComponentsFallBackAtomically() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto* producer = simulation.createUnit<PassiveProducer>("producer");
    std::array<PassiveConsumer*, 4> consumers{};
    for (size_t i = 0; i < consumers.size(); ++i) {
        consumers[i] = simulation.createUnit<PassiveConsumer>("consumer" + std::to_string(i));
        if (i == 0) consumers[i]->in.setCapacity(8);
        simulation.connect(producer->out, consumers[i]->in, 1);
    }
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == 0,
          "bounded destination did not veto the whole Port component");
    check(!producer->out.transparentBroadcastEnabled(),
          "unsafe component was partially switched to shared transport");
    check(producer->out.send(7), "fallback queue send failed");
    for (auto* consumer : consumers) {
        check(consumer->in.tryReceive(1) == std::optional<uint64_t>{7},
              "fallback queue changed delivery semantics");
    }
}

void testOversizedRateFallsBackAtomically() {
    using OversizedPayload = std::array<uint8_t, 1u << 20>;
    check(!detail::SharedBroadcastTransport<OversizedPayload>::automaticAllocationEligible(512, 1),
          "oversized payload passed the automatic allocation limit");

    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    constexpr size_t kTooManyMessagesPerCycle = (1u << 20) / 512 + 1;
    auto* producer = simulation.createUnit<PassiveProducer>("producer", kTooManyMessagesPerCycle);
    std::array<PassiveConsumer*, 4> consumers{};
    for (size_t i = 0; i < consumers.size(); ++i) {
        consumers[i] = simulation.createUnit<PassiveConsumer>("consumer" + std::to_string(i));
        simulation.connect(producer->out, consumers[i]->in, 1);
    }
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == 0,
          "oversized producer rate did not veto the whole Port component");
    check(!producer->out.transparentBroadcastEnabled(),
          "oversized component was partially switched to shared transport");
}

void testInitializedQueueFallsBackAtomically() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto* producer = simulation.createUnit<InitializingProducer>("producer");
    std::array<PassiveConsumer*, 4> consumers{};
    for (size_t i = 0; i < consumers.size(); ++i) {
        consumers[i] = simulation.createUnit<PassiveConsumer>("consumer" + std::to_string(i));
        simulation.connect(producer->out, consumers[i]->in, 1);
    }
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == 0,
          "an initialized queue did not veto the whole Port component");
    check(!producer->out.transparentBroadcastEnabled(),
          "an initialized component was partially switched to shared transport");
}

void testStageSelectiveCancellation() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto* producer = simulation.createUnit<PassiveProducer>("producer");
    std::array<StageSelectiveConsumer*, 4> consumers{};
    for (size_t i = 0; i < consumers.size(); ++i) {
        consumers[i] =
            simulation.createUnit<StageSelectiveConsumer>("consumer" + std::to_string(i));
        simulation.connect(producer->out, consumers[i]->in, 1);
    }
    simulation.initialize();

    check(producer->out.send(90), "StageSelective pre-flush send failed");
    simulation.run(1);
    consumers[0]->in.cancelYoungerThan<&valueKey>(50);
    check(producer->out.send(91), "StageSelective post-flush send failed");

    check(drainTryReceive(consumers[0]->in, 2) == std::vector<uint64_t>({91}),
          "StageSelective enqueue-cycle scope changed on shared replay");
    for (size_t i = 1; i < consumers.size(); ++i) {
        check(drainTryReceive(consumers[i]->in, 2) == std::vector<uint64_t>({90, 91}),
              "StageSelective filter affected another consumer");
    }
}

void testInitializeTimeCancellationScope() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto* producer = simulation.createUnit<PassiveProducer>("producer");
    auto* canceled = simulation.createUnit<InitializingCancelConsumer>("canceled");
    std::array<PassiveConsumer*, 3> other_consumers{
        simulation.createUnit<PassiveConsumer>("consumer0"),
        simulation.createUnit<PassiveConsumer>("consumer1"),
        simulation.createUnit<PassiveConsumer>("consumer2")};
    simulation.connect(producer->out, canceled->in, 1);
    for (auto* consumer : other_consumers) simulation.connect(producer->out, consumer->in, 1);
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == 4,
          "initialize-time cancellation prevented transparent broadcast selection");
    check(producer->out.send(90), "post-initialize broadcast send failed");
    check(canceled->in.tryReceive(1) == std::optional<uint64_t>{90},
          "initialize-time cancellation leaked into later shared publishes");
    for (auto* consumer : other_consumers) {
        check(consumer->in.tryReceive(1) == std::optional<uint64_t>{90},
              "initialize-time cancellation affected another consumer");
    }
}

void testMoveOnlyPayloadUsesQueuePath() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto* producer = simulation.createUnit<MoveOnlyProducer>("producer");
    auto* consumer = simulation.createUnit<MoveOnlyConsumer>("consumer");
    simulation.connect(producer->out, consumer->in, 1);
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == 0,
          "move-only payload unexpectedly selected transparent broadcast");
    check(producer->out.send(std::make_unique<int>(7)), "move-only tryReceive send failed");
    auto first = consumer->in.tryReceive(1);
    check(first && **first == 7, "move-only tryReceive changed queue-path delivery");

    check(producer->out.send(std::make_unique<int>(8)), "move-only receiveAll send 1 failed");
    check(producer->out.send(std::make_unique<int>(9)), "move-only receiveAll send 2 failed");
    auto all = consumer->in.receiveAll(1);
    check(all.size() == 2 && *all[0] == 8 && *all[1] == 9,
          "move-only receiveAll changed queue-path delivery");

    check(producer->out.send(std::make_unique<int>(10)),
          "move-only receiveAllBuffered send failed");
    const auto& buffered = consumer->in.receiveAllBuffered(1);
    check(buffered.size() == 1 && *buffered[0] == 10,
          "move-only receiveAllBuffered changed queue-path delivery");
}

class StreamingProducer : public TickableUnit {
public:
    StreamingProducer(std::string name, uint64_t producer_id)
        : TickableUnit(std::move(name)), producer_id_(producer_id) {}

    void tick() override {
        const uint64_t value = (localCycle() << 1) | producer_id_;
        if (!out.send(value)) ++failed_sends_;
    }

    OutPort<uint64_t> out{this, "out", 1};
    uint64_t failedSends() const noexcept { return failed_sends_; }

private:
    uint64_t producer_id_ = 0;
    uint64_t failed_sends_ = 0;
};

class StreamingConsumer : public TickableUnit {
public:
    explicit StreamingConsumer(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {
        for (uint64_t value : in.receiveAll(localCycle())) {
            received_.push_back({localCycle(), value});
        }
    }

    InPort<uint64_t> in{this, "in"};
    const auto& received() const noexcept { return received_; }

private:
    std::vector<std::pair<uint64_t, uint64_t>> received_;
};

class WideStreamingProducer : public TickableUnit {
public:
    WideStreamingProducer(std::string name, uint64_t producer_id)
        : TickableUnit(std::move(name)), producer_id_(producer_id) {}

    void tick() override {
        if (!out.send((localCycle() << 8) | producer_id_)) ++failed_sends_;
    }

    OutPort<uint64_t> out{this, "out", 1};
    uint64_t failedSends() const noexcept { return failed_sends_; }

private:
    uint64_t producer_id_ = 0;
    uint64_t failed_sends_ = 0;
};

void testStalledConsumerKeepsUnlimitedSemantics() {
    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto* producer = simulation.createUnit<StreamingProducer>("producer", 0);
    auto* stalled = simulation.createUnit<PassiveConsumer>("stalled");
    std::array<StreamingConsumer*, 3> draining{
        simulation.createUnit<StreamingConsumer>("consumer0"),
        simulation.createUnit<StreamingConsumer>("consumer1"),
        simulation.createUnit<StreamingConsumer>("consumer2")};
    simulation.connect(producer->out, stalled->in, 1);
    for (auto* consumer : draining) simulation.connect(producer->out, consumer->in, 1);

    constexpr uint64_t kCycles = 1'200;
    simulation.run(kCycles);
    check(simulation.transparentBroadcastConnectionCount() == 4,
          "stalled-consumer bus did not use transparent broadcast");
    check(producer->failedSends() == 0,
          "an unlimited stalled consumer introduced broadcast backpressure");
    check(stalled->in.queuedMessageCount() == kCycles,
          "stalled consumer did not retain every broadcast payload");
    for (auto* consumer : draining) {
        check(consumer->received().size() == kCycles - 1,
              "draining consumer lost payloads while another consumer stalled");
    }
    check(stalled->in.receiveAll(kCycles).size() == kCycles,
          "stalled consumer could not replay payloads across chunk boundaries");
}

void testParallelLookaheadReplay() {
    TickSimulationConfig config;
    config.num_threads = 4;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_epoch_free_lookahead = true;
    config.enable_dynamic_rebalance = false;
    config.enable_weighted_partitioning = true;
    config.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    config.initial_partition_sync_cost_ns = 0.0;
    TickSimulation simulation(config);

    std::array<StreamingProducer*, 2> producers{
        simulation.createUnit<StreamingProducer>("producer0", 0),
        simulation.createUnit<StreamingProducer>("producer1", 1)};
    std::array<StreamingConsumer*, 4> consumers{
        simulation.createUnit<StreamingConsumer>("consumer0"),
        simulation.createUnit<StreamingConsumer>("consumer1"),
        simulation.createUnit<StreamingConsumer>("consumer2"),
        simulation.createUnit<StreamingConsumer>("consumer3")};
    for (auto* producer : producers) {
        for (auto* consumer : consumers) simulation.connect(producer->out, consumer->in, 1);
    }
    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 0.0;
    simulation.setPrecomputedUnitCosts(std::vector<double>(6, 100.0), metrics);

    constexpr uint64_t kCycles = 1'200;
    simulation.run(kCycles);
    check(simulation.transparentBroadcastConnectionCount() == 8,
          "parallel bus did not use transparent broadcast");
    check(simulation.epochFreeRunCount() > 0,
          "parallel transport test did not exercise epoch-free lookahead");
    for (auto* producer : producers) {
        check(producer->failedSends() == 0, "shared ring back-pressured a draining consumer");
    }
    for (auto* consumer : consumers) {
        const auto& received = consumer->received();
        check(received.size() == 2 * (kCycles - 1),
              "parallel consumer received the wrong message count");
        if (received.size() != 2 * (kCycles - 1)) continue;
        for (uint64_t cycle = 1; cycle < kCycles; ++cycle) {
            const size_t base = static_cast<size_t>((cycle - 1) * 2);
            check(received[base] == std::pair<uint64_t, uint64_t>{cycle, (cycle - 1) << 1},
                  "producer 0 replay cycle or order changed");
            check(
                received[base + 1] == std::pair<uint64_t, uint64_t>{cycle, ((cycle - 1) << 1) | 1},
                "producer 1 replay cycle or order changed");
        }
    }
}

void testNucleusScaleDynamicRebalanceReplay() {
    constexpr size_t kProducerCount = 10;
    constexpr size_t kConsumerCount = 11;
    TickSimulationConfig config;
    config.num_threads = 6;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_epoch_free_lookahead = true;
    config.enable_dynamic_rebalance = true;
    config.enable_weighted_partitioning = true;
    config.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    config.initial_partition_sync_cost_ns = 0.0;
    TickSimulation simulation(config);

    std::array<WideStreamingProducer*, kProducerCount> producers{};
    std::array<StreamingConsumer*, kConsumerCount> consumers{};
    for (size_t producer = 0; producer < producers.size(); ++producer) {
        producers[producer] = simulation.createUnit<WideStreamingProducer>(
            "dynamic_producer" + std::to_string(producer), producer);
    }
    for (size_t consumer = 0; consumer < consumers.size(); ++consumer) {
        consumers[consumer] =
            simulation.createUnit<StreamingConsumer>("dynamic_consumer" + std::to_string(consumer));
    }
    for (auto* producer : producers) {
        for (auto* consumer : consumers) {
            simulation.connect(producer->out, consumer->in, 1);
        }
    }
    PlatformMetrics metrics{};
    metrics.atomic_roundtrip_ns = 0.0;
    simulation.setPrecomputedUnitCosts(std::vector<double>(kProducerCount + kConsumerCount, 100.0),
                                       metrics);

    constexpr uint64_t kCycles = 400;
    simulation.run(kCycles);
    check(simulation.transparentBroadcastConnectionCount() == kProducerCount * kConsumerCount,
          "dynamic Nucleus-scale bus did not use transparent broadcast");
    check(simulation.epochFreeRunCount() > 0,
          "dynamic Nucleus-scale bus did not use epoch-free execution");
    for (auto* producer : producers) {
        check(producer->failedSends() == 0,
              "dynamic Nucleus-scale broadcast producer was back-pressured");
    }
    for (auto* consumer : consumers) {
        const auto& received = consumer->received();
        check(received.size() == kProducerCount * (kCycles - 1),
              "dynamic Nucleus-scale consumer lost a payload");
        if (received.size() != kProducerCount * (kCycles - 1)) continue;
        for (uint64_t cycle = 1; cycle < kCycles; ++cycle) {
            for (size_t producer = 0; producer < kProducerCount; ++producer) {
                const size_t index = static_cast<size_t>((cycle - 1) * kProducerCount + producer);
                check(received[index] ==
                          std::pair<uint64_t, uint64_t>{cycle, ((cycle - 1) << 8) | producer},
                      "dynamic Nucleus-scale replay changed cycle or producer order");
            }
        }
    }
}

void testEnvironmentOptOut() {
    setenv("CHRONON_EXPERIMENTAL_TRANSPARENT_BROADCAST", "0", 1);

    TickSimulationConfig config;
    config.enable_parallel = false;
    TickSimulation simulation(config);
    auto bus = makePassiveBus(simulation);
    simulation.initialize();

    check(simulation.transparentBroadcastConnectionCount() == 0,
          "environment opt-out did not disable automatic transport selection");
    check(!bus.producers[0]->out.transparentBroadcastEnabled(),
          "environment opt-out left an OutPort on shared transport");
    check(bus.producers[0]->out.send(42), "opt-out fallback send failed");
    for (auto* consumer : bus.consumers) {
        check(consumer->in.tryReceive(1) == std::optional<uint64_t>{42},
              "opt-out fallback changed Port delivery");
    }

    unsetenv("CHRONON_EXPERIMENTAL_TRANSPARENT_BROADCAST");
}

}  // namespace

int main() {
    testAutomaticSelectionAndPortSemantics();
    testReceiverFilterIsCursorLocal();
    testLateActivityOptInInvalidatesWakeCache();
    testNucleusScaleDelayOneBusReplay();
    testCachedHeadInvalidation();
    testFrontierRefreshesSparseAndFutureLanes();
    testFrontierPreservesMultiCycleBacklogOrder();
    testUnsafeComponentsFallBackAtomically();
    testOversizedRateFallsBackAtomically();
    testInitializedQueueFallsBackAtomically();
    testStageSelectiveCancellation();
    testInitializeTimeCancellationScope();
    testMoveOnlyPayloadUsesQueuePath();
    testStalledConsumerKeepsUnlimitedSemantics();
    testParallelLookaheadReplay();
    testNucleusScaleDynamicRebalanceReplay();
    testEnvironmentOptOut();
    std::cout << (failures == 0 ? "ALL PASSED\n" : "FAILED\n");
    return failures == 0 ? 0 : 1;
}
