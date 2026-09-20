// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <cassert>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/port/MessageQueue.hpp"

namespace chronon::sender {
struct InPortIngressTestAccess {
    template <typename T>
    static bool certified(const InPort<T>& port, uint64_t cycle) {
        return port.ingressCompleteForTick_(cycle);
    }
};
}  // namespace chronon::sender

using namespace chronon::sender;

namespace {

template <typename T>
std::optional<T> receive(MultiProducerQueueAdapter<T>& queue, uint64_t cycle,
                         bool complete = true) {
    std::optional<T> result;
    queue.consumeReady(cycle, [&](T& value) { result.emplace(std::move(value)); }, complete);
    return result;
}

void testQueue(size_t lanes) {
    MultiProducerQueueAdapter<std::string> queue(4, 32);
    for (size_t i = 0; i < lanes; ++i) queue.addProducerThread(i, false);
    assert(queue.pushFromThread(0, "second", 0, 2));
    assert(queue.pushFromThread(lanes - 1, "first", 0, 1));
    queue.prepareSharedFifo(0, true);
    assert(receive(queue, 0) == "first");
    assert(receive(queue, 0) == "second");
    for (int i = 0; i < 4; ++i) assert(!receive(queue, 0));

    // A future publication does not invalidate the certificate for cycle 0.
    assert(queue.pushFromThread(0, "future", 1));
    assert(!receive(queue, 0));
    assert(receive(queue, 1) == "future");
    assert(!receive(queue, 1));

    // An uncertified caller must discard the negative result, including when
    // a producer publishes an eligible entry after the certified drain.
    std::thread producer([&] { assert(queue.pushFromThread(lanes - 1, "late", 1)); });
    producer.join();
    assert(queue.tryPop(1) == "late");
    assert(!queue.tryPop(1));
    assert(queue.pushFromThread(0, "later", 1));
    assert(queue.tryPop(1) == "later");

    // Fill in cycle 2, carry occupancy into cycle 3. A full destination is
    // not exhausted ingress: each pop must make room for another admission.
    for (int i = 0; i < 4; ++i) assert(queue.pushFromThread(0, std::to_string(i), 2));
    queue.prepareSharedFifo(2, true);
    assert(queue.size() == 4);
    for (int i = 4; i < 9; ++i) assert(queue.pushFromThread(0, std::to_string(i), 3));
    queue.prepareSharedFifo(3, true);
    for (int i = 0; i < 8; ++i) assert(receive(queue, 3) == std::to_string(i));
    assert(!receive(queue, 3));  // four new admissions, despite eight receives
    assert(queue.stagedSize() == 1);
    assert(receive(queue, 4) == "8");
    assert(!receive(queue, 4));

    queue.clear();
    assert(queue.pushFromThread(0, "after-clear", 4));
    assert(receive(queue, 4) == "after-clear");
    // Clear cannot reset same-cycle aggregate credit.
    assert(queue.pushFromThread(0, "a", 4));
    assert(queue.pushFromThread(0, "b", 4));
    assert(queue.pushFromThread(0, "c", 4));
    queue.resetIngressCache();  // new certified publication batch at the same cycle
    queue.prepareSharedFifo(4, true);
    queue.clear();
    assert(queue.pushFromThread(0, "next-cycle", 4));
    assert(!queue.tryPop(4));
    assert(queue.tryPop(5) == "next-cycle");
    assert(queue.sharedFifoHighWatermark() == 4);

    // A new invocation at the same cycle (e.g. retry) gets a new certificate.
    assert(!receive(queue, 6));
    assert(queue.pushFromThread(0, "retry", 6));
    queue.resetIngressCache();
    assert(receive(queue, 6) == "retry");
}

void testDifferential(size_t lanes) {
    MultiProducerQueueAdapter<uint64_t> reference(8, 64), cached(8, 64);
    for (size_t i = 0; i < lanes; ++i) {
        reference.addProducerThread(i, false);
        cached.addProducerThread(i, false);
    }
    std::minstd_rand random(153);
    uint64_t sequence = 0;
    for (uint64_t cycle = 0; cycle < 2000; ++cycle) {
        // All eligible publishes precede the certificate. Vary sparse/bursty
        // traffic, retained ingress, and partial drains independently.
        const size_t batch = cycle % 4 == 0 ? 0 : random() % 24;
        for (size_t i = 0; i < batch; ++i) {
            const auto lane = random() % lanes;
            ++sequence;
            assert(reference.pushFromThread(lane, sequence, cycle, lane) ==
                   cached.pushFromThread(lane, sequence, cycle, lane));
        }
        reference.prepareSharedFifo(cycle);
        cached.prepareSharedFifo(cycle, true);
        const size_t reads = random() % 16;
        for (size_t i = 0; i < reads; ++i) {
            if (random() % 17 == 0) {
                reference.clear();
                cached.clear();
            } else {
                assert(receive(reference, cycle, false) == receive(cached, cycle));
            }
            assert(reference.size() == cached.size());
            assert(reference.stagedSize() == cached.stagedSize());
            assert(reference.sharedFifoHighWatermark() == cached.sharedFifoHighWatermark());
        }
    }
}

void testMoveOnly() {
    // Ownership crosses lane -> bounded FIFO -> result exactly once. Ring
    // wrap, cancellation by clear, and destruction must not duplicate owners.
    struct Counted {
        explicit Counted(int& destroyed) : destroyed(destroyed) {}
        ~Counted() { ++destroyed; }
        int& destroyed;
    };
    int destroyed = 0;
    {
        MultiProducerQueueAdapter<std::unique_ptr<Counted>> queue(4, 8);
        auto lane = queue.addProducerThread(1, false);
        for (uint64_t cycle = 0; cycle < 40; ++cycle) {
            assert(queue.pushFromThread(lane, std::make_unique<Counted>(destroyed), cycle));
            queue.prepareSharedFifo(cycle, true);
            auto result = receive(queue, cycle);
            assert(result && *result);
            assert(!receive(queue, cycle));
        }
        assert(destroyed == 40);
        assert(queue.pushFromThread(lane, std::make_unique<Counted>(destroyed), 41));
        queue.prepareSharedFifo(41, true);
        queue.clear();
        assert(!receive(queue, 41));
    }
    assert(destroyed == 41);
}

class OwnedProducer final : public TickableUnit {
public:
    explicit OwnedProducer(uint64_t id) : TickableUnit("owned" + std::to_string(id)), id_(id) {}
    OutPort<std::unique_ptr<uint64_t>> out{this, "out", 1};
    void tick() override {
        if (localCycle() < 20) assert(out.send(std::make_unique<uint64_t>(localCycle() * 8 + id_)));
    }

private:
    uint64_t id_;
};

class OwnedConsumer final : public TickableUnit {
public:
    OwnedConsumer() : TickableUnit("owned_consumer") {}
    InPort<std::unique_ptr<uint64_t>> in{this, "in", 16};
    uint64_t count = 0, sum = 0;
    void tick() override {
        while (auto message = in.tryReceive()) {
            assert(*message);
            sum += **message;
            ++count;
        }
    }
};

void testOwnedTransport() {
    TickSimulationConfig config;
    config.num_threads = 4;
    config.enable_parallel = true;
    config.max_lookahead_cycles = 8;
    config.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    config.initial_partition_sync_cost_ns = 0;
    TickSimulation sim(config);
    auto* consumer = sim.createUnit<OwnedConsumer>();
    for (uint64_t i = 0; i < 8; ++i) {
        auto* producer = sim.createUnit<OwnedProducer>(i);
        sim.connect(producer->out, consumer->in, i % 3 + 1);
    }
    sim.setPrecomputedUnitCosts(std::vector<double>(9, 1000.0), {});
    sim.initialize();
    assert(consumer->in.isMultiProducerMode());
    sim.run(40);
    assert(sim.epochFreeRunCount() > 0);
    assert(consumer->count == 160);
    assert(consumer->sum == 159 * 160 / 2);
    assert(consumer->in.queuedMessageCount() == 0);
    assert(consumer->in.transportPendingMessageCount() == 0);
}

class QuietProducer final : public TickableUnit {
public:
    explicit QuietProducer(std::string name, bool emit = false)
        : TickableUnit(std::move(name)), emit_(emit) {}
    OutPort<int> out{this, "out", 1};
    void tick() override {
        if (emit_) assert(out.send(7));
    }

private:
    bool emit_;
};

class InjectionConsumer final : public TickableUnit {
public:
    explicit InjectionConsumer(bool zero_delay) : TickableUnit("consumer"), zero_(zero_delay) {}
    InPort<int> in{this, "in", 16};
    size_t lane = SIZE_MAX;
    size_t ticks = 0;
    bool zero_;

    void tick() override {
        assert(in.isMultiProducerMode());
        assert(InPortIngressTestAccess::certified(in, localCycle()) == (!zero_ && ticks == 0));
        assert(!InPortIngressTestAccess::certified(in, localCycle() + 1));
        if (zero_) assert(in.tryReceive() == 7);
        assert(!in.tryReceive());  // may reuse the pre-tick negative result
        {
            // Explicit timestamped injection must invalidate the scheduler
            // certificate, even on an otherwise fully registered port.
            if (ticks % 2 == 0)
                assert(in.pushToThreadQueue(lane, 42, localCycle()));
            else
                assert(in.pushToThreadQueueCancelable(lane, 42, localCycle(), nullptr, 0));
        }
        assert(in.tryReceive() == 42);
        assert(!in.tryReceiveFiltered([](const int&) noexcept { return true; }));
        assert(in.receiveAllBuffered().empty());
        ++ticks;
    }
};

void testScheduledFallback(bool zero_delay) {
    TickSimulationConfig config;
    config.num_threads = 4;
    config.enable_parallel = true;
    config.enable_dynamic_rebalance = true;
    config.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    config.initial_partition_sync_cost_ns = 0;
    TickSimulation sim(config);
    auto* consumer = sim.createUnit<InjectionConsumer>(zero_delay);
    Connection<int>* first = nullptr;
    for (int i = 0; i < 8; ++i) {
        auto* producer =
            sim.createUnit<QuietProducer>("producer" + std::to_string(i), zero_delay && i == 0);
        auto* conn = sim.connect(producer->out, consumer->in, zero_delay && i == 0 ? 0 : 1);
        if (!first) first = conn;
    }
    sim.setPrecomputedUnitCosts(std::vector<double>(9, 1000.0), {});
    sim.initialize();
    assert(consumer->in.isMultiProducerMode());
    consumer->lane = consumer->in.getQueueIdForThread(first->connId() + 1);
    assert(consumer->lane != SIZE_MAX);
    sim.run(3);
    sim.run(3);
    assert(consumer->ticks == 6);
    assert(sim.epochFreeRunCount() > 0);
    assert(!InPortIngressTestAccess::certified(consumer->in, consumer->localCycle()));
    // Outside the tick, a read at the previously cached cycle stays uncached.
    const auto cycle = consumer->localCycle() - 1;
    assert(!consumer->in.tryReceive(cycle));
    assert(consumer->in.pushToThreadQueue(consumer->lane, 99, cycle));
    assert(consumer->in.tryReceive(cycle) == 99);
}

}  // namespace

int main() {
    for (size_t lanes : {2, 31, 32, 65}) {
        testQueue(lanes);
        testDifferential(lanes);
    }
    testMoveOnly();
    testOwnedTransport();
    testScheduledFallback(false);
    testScheduledFallback(true);
    std::cout << "Bounded MPSC ingress cache tests passed.\n";
}
