// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <cstdint>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

#include "chronon/Chronon.hpp"

using namespace chronon::sender;

namespace {

class ManualUnit : public Unit {
public:
    explicit ManualUnit(std::string name) : Unit(std::move(name)) {}

    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
};

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void testRegisteredEdgeRejectsZeroCapacityAndRate() {
    ManualUnit source("source");
    ManualUnit sink("sink");
    OutPort<int> out{&source, "out"};
    InPort<int> in{&sink, "in"};
    auto* connection = out.connect(&in, 1);

    bool zero_capacity_rejected = false;
    try {
        connection->configureRegisteredEdge(/*capacity=*/0, /*rate=*/std::nullopt);
    } catch (const std::invalid_argument&) {
        zero_capacity_rejected = true;
    }
    require(zero_capacity_rejected, "registered edge accepted zero capacity");

    bool zero_rate_rejected = false;
    try {
        connection->configureRegisteredEdge(/*capacity=*/std::nullopt, /*rate=*/0);
    } catch (const std::invalid_argument&) {
        zero_rate_rejected = true;
    }
    require(zero_rate_rejected, "registered edge accepted zero rate");

    connection->configureRegisteredEdge(/*capacity=*/1, /*rate=*/1);
}

void testMPSCCapacityIgnoresSameCycleConsumerPopInterleaving() {
    constexpr size_t kRate = 2;
    constexpr size_t kDepth = 2;

    ManualUnit producer("producer");
    ManualUnit consumer("consumer");
    OutPort<int> out{&producer, "out", kRate};
    InPort<int> in{&consumer, "in", kDepth};
    auto* connection = out.connect(&in, 1);
    connection->optimizeForMPSC();
    const size_t queue_id = connection->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    connection->setThreadQueueId(queue_id);
    require(connection->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    producer.setCycle(0);
    require(out.send(1), "MPSC failed to fill first slot");
    require(out.send(2), "MPSC failed to fill second slot");
    require(in.queuedMessageCount() == 0,
            "future transport entries entered the shared FIFO before arrival");
    require(in.transportPendingMessageCount() == kDepth,
            "MPSC ingress lane did not retain future entries");

    consumer.setCycle(1);
    in.prepareConsumerCycle(1);
    require(in.queuedMessageCount() == kDepth,
            "receiver cycle did not fill the bounded shared FIFO");
    require(in.transportPendingMessageCount() == 0,
            "receiver cycle did not drain eligible ingress entries");
    auto first = in.tryReceive(1);
    require(first.has_value() && *first == 1, "MPSC drain changed delivery order");
    require(in.queuedMessageCount() == kDepth - 1, "MPSC drain did not remove one entry");

    producer.setCycle(1);
    require(!out.canSend(), "MPSC capacity used same-cycle host pop for admission");
    require(!out.send(3), "MPSC send succeeded after same-cycle host pop");

    producer.setCycle(2);
    require(out.canSend(), "MPSC capacity did not release prior-cycle consumer credit");
    require(out.send(3), "MPSC send failed after prior-cycle consumer credit");
}

void testSPSCConservativeUpperBoundFallsBackAtCapacity() {
    constexpr size_t kDepth = 8;

    ManualUnit producer("spsc_producer");
    ManualUnit consumer("spsc_consumer");
    OutPort<int> out{&producer, "out", kDepth};
    InPort<int> in{&consumer, "in", kDepth};
    auto* connection = out.connect(&in, 1);
    connection->optimizeForSPSC();

    // After the first exact query, the remaining sends are admitted from the
    // producer-only conservative upper bound until it reaches capacity.
    producer.setCycle(0);
    for (size_t value = 0; value < kDepth; ++value) {
        require(out.send(static_cast<int>(value)), "SPSC failed to fill declared capacity");
    }
    require(!out.canSend(), "SPSC exceeded its per-cycle/depth bound");

    // Host execution lets the consumer pop first, but architectural credit is
    // cycle-strict: the producer must fall back to exact history and reject a
    // same-cycle refill when the conservative bound reaches the limit.
    consumer.setCycle(1);
    const auto first_batch = in.receiveAll(1);
    require(first_batch.size() == kDepth, "SPSC consumer did not drain the first burst");
    producer.setCycle(1);
    require(!out.canSend(), "SPSC upper bound exposed same-cycle consumer credit");
    require(!out.send(99), "SPSC refilled from a same-cycle consumer pop");

    // On the next producer cycle the original admission history retires those
    // pops, refreshes the bound exactly, and permits the full burst again.
    producer.setCycle(2);
    for (size_t value = 0; value < kDepth; ++value) {
        require(out.send(static_cast<int>(100 + value)),
                "SPSC did not release prior-cycle consumer credit");
    }
}

void testSPSCUpperBoundRefreshRecoversCycleStartOccupancy() {
    constexpr size_t kDepth = 8;
    constexpr size_t kInitialOccupancy = 5;

    ManualUnit producer("spsc_refresh_producer");
    ManualUnit consumer("spsc_refresh_consumer");
    OutPort<int> out{&producer, "out", kDepth};
    InPort<int> in{&consumer, "in", kDepth};
    auto* connection = out.connect(&in, 1);
    connection->optimizeForSPSC();

    producer.setCycle(0);
    for (size_t value = 0; value < kInitialOccupancy; ++value) {
        require(out.send(static_cast<int>(value)), "SPSC failed to seed refresh test");
    }

    consumer.setCycle(1);
    const auto drained = in.receiveAll(1);
    require(drained.size() == kInitialOccupancy, "SPSC refresh test did not drain seed data");

    // The carried bound admits the first three publications without touching
    // receiver state. The fourth request refreshes occupancy after those
    // publications are already visible. It must subtract them exactly once
    // and recover the empty cycle-start snapshot, allowing all eight sends.
    producer.setCycle(2);
    for (size_t value = 0; value < kDepth; ++value) {
        require(out.send(static_cast<int>(100 + value)),
                "SPSC mid-cycle refresh double-counted current publications");
    }
    require(!out.canSend(), "SPSC refresh test exceeded declared capacity");
}

void testMPSCUpperBoundRefreshRecoversCycleStartOccupancy() {
    constexpr size_t kDepth = 8;
    constexpr size_t kInitialOccupancy = 5;

    ManualUnit producer("mpsc_refresh_producer");
    ManualUnit consumer("mpsc_refresh_consumer");
    OutPort<int> out{&producer, "out", kDepth};
    InPort<int> in{&consumer, "in", kDepth};
    auto* connection = out.connect(&in, 1);
    connection->optimizeForMPSC();
    const size_t queue_id = connection->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC refresh producer registration failed");
    connection->setThreadQueueId(queue_id);
    require(connection->registerOnDestMPSC() != nullptr,
            "MPSC refresh destination registration failed");

    producer.setCycle(0);
    for (size_t value = 0; value < kInitialOccupancy; ++value) {
        require(out.send(static_cast<int>(value)), "MPSC failed to seed refresh test");
    }

    consumer.setCycle(1);
    in.prepareConsumerCycle(1);
    const auto drained = in.receiveAll(1);
    require(drained.size() == kInitialOccupancy, "MPSC refresh test did not drain seed data");

    producer.setCycle(2);
    for (size_t value = 0; value < kDepth; ++value) {
        require(out.send(static_cast<int>(100 + value)),
                "MPSC mid-cycle refresh double-counted current publications");
    }
    require(!out.canSend(), "MPSC refresh test exceeded declared capacity");
}

void testMPSCAggregateSharedDepthAcrossProducers() {
    constexpr size_t kRate = 2;
    constexpr size_t kDepth = 2;

    ManualUnit producer0("producer0");
    ManualUnit producer1("producer1");
    ManualUnit producer2("producer2");
    ManualUnit consumer("consumer");
    OutPort<int> out0{&producer0, "out0", kRate};
    OutPort<int> out1{&producer1, "out1", kRate};
    OutPort<int> out2{&producer2, "out2", kRate};
    InPort<int> in{&consumer, "in", kDepth};

    std::array<Connection<int>*, 3> connections{out0.connect(&in, 1), out1.connect(&in, 1),
                                                out2.connect(&in, 1)};
    for (size_t i = 0; i < connections.size(); ++i) {
        auto* connection = connections[i];
        connection->setConnId(static_cast<uint32_t>(i));
        connection->optimizeForMPSC();
        const size_t queue_id = connection->registerProducerThread(i + 1);
        require(queue_id != SIZE_MAX, "MPSC producer registration failed");
        connection->setThreadQueueId(queue_id);
        require(connection->registerOnDestMPSC() != nullptr,
                "MPSC destination registration failed");
    }

    std::array<ManualUnit*, 3> producers{&producer0, &producer1, &producer2};
    std::array<OutPort<int>*, 3> outputs{&out0, &out1, &out2};
    for (size_t producer = 0; producer < outputs.size(); ++producer) {
        producers[producer]->setCycle(0);
        require(outputs[producer]->send(static_cast<int>(producer * 10)),
                "burst producer failed first ingress send");
        require(outputs[producer]->send(static_cast<int>(producer * 10 + 1)),
                "burst producer failed second ingress send");
    }
    require(in.transportPendingMessageCount() == 6,
            "burst traffic did not remain in private ingress lanes");

    size_t received = 0;
    for (uint64_t cycle = 1; cycle <= 3; ++cycle) {
        consumer.setCycle(cycle);
        const size_t staged_before = in.transportPendingMessageCount();
        in.prepareConsumerCycle(cycle);
        require(in.queuedMessageCount() <= kDepth, "aggregate shared FIFO exceeded capacity");
        const size_t staged_after = in.transportPendingMessageCount();
        require(staged_before - staged_after <= kDepth,
                "aggregate MPSC admission exceeded its per-cycle capacity");
        const auto batch = in.receiveAll(cycle);
        received += batch.size();
        require(in.queuedMessageCount() <= kDepth,
                "shared FIFO exceeded capacity while receiver drained it");
    }

    require(received == 6, "bounded shared FIFO lost burst messages");
    require(in.sharedFifoHighWatermark() == kDepth,
            "shared FIFO high-water mark did not reach exactly its capacity");
    require(in.transportPendingMessageCount() == 0,
            "bounded shared FIFO left ingress entries after drain");
}

void testMPSCRegisteredCapacityCreatesAggregateSharedFifo() {
    constexpr size_t kRate = 2;
    constexpr size_t kDepth = 2;

    ManualUnit producer0("producer0");
    ManualUnit producer1("producer1");
    ManualUnit producer2("producer2");
    ManualUnit consumer("consumer");
    OutPort<int> out0{&producer0, "out0", kRate};
    OutPort<int> out1{&producer1, "out1", kRate};
    OutPort<int> out2{&producer2, "out2", kRate};
    InPort<int> in{&consumer, "in"};

    std::array<Connection<int>*, 3> connections{out0.connect(&in, 1), out1.connect(&in, 1),
                                                out2.connect(&in, 1)};
    for (size_t i = 0; i < connections.size(); ++i) {
        auto* connection = connections[i];
        connection->setConnId(static_cast<uint32_t>(i));
        connection->configureRegisteredEdge(kDepth, std::nullopt);
        connection->optimizeForMPSC();
        const size_t queue_id = connection->registerProducerThread(i + 1);
        require(queue_id != SIZE_MAX, "registered MPSC producer registration failed");
        connection->setThreadQueueId(queue_id);
        require(connection->registerOnDestMPSC() != nullptr,
                "registered MPSC destination registration failed");
    }

    require(in.capacity() == kDepth,
            "registered MPSC capacity did not configure the destination FIFO");

    std::array<ManualUnit*, 3> producers{&producer0, &producer1, &producer2};
    std::array<OutPort<int>*, 3> outputs{&out0, &out1, &out2};
    for (size_t producer = 0; producer < outputs.size(); ++producer) {
        producers[producer]->setCycle(0);
        require(outputs[producer]->send(static_cast<int>(producer * 10)),
                "registered burst producer failed first ingress send");
        require(outputs[producer]->send(static_cast<int>(producer * 10 + 1)),
                "registered burst producer failed second ingress send");
    }
    require(in.transportPendingMessageCount() == 6,
            "registered burst traffic did not remain in private ingress lanes");

    size_t received = 0;
    for (uint64_t cycle = 1; cycle <= 3; ++cycle) {
        consumer.setCycle(cycle);
        in.prepareConsumerCycle(cycle);
        require(in.queuedMessageCount() <= kDepth,
                "registered aggregate FIFO exceeded destination capacity");
        const auto batch = in.receiveAll(cycle);
        received += batch.size();
    }

    require(received == 6, "registered aggregate FIFO lost burst messages");
    require(in.sharedFifoHighWatermark() == kDepth,
            "registered aggregate FIFO did not saturate at its destination capacity");
    require(in.transportPendingMessageCount() == 0,
            "registered aggregate FIFO left ingress entries after drain");
}

}  // namespace

int main() {
    try {
        testRegisteredEdgeRejectsZeroCapacityAndRate();
        testSPSCConservativeUpperBoundFallsBackAtCapacity();
        testSPSCUpperBoundRefreshRecoversCycleStartOccupancy();
        testMPSCUpperBoundRefreshRecoversCycleStartOccupancy();
        testMPSCCapacityIgnoresSameCycleConsumerPopInterleaving();
        testMPSCAggregateSharedDepthAcrossProducers();
        testMPSCRegisteredCapacityCreatesAggregateSharedFifo();
    } catch (const std::exception& error) {
        std::cerr << "Connection admission test failed: " << error.what() << '\n';
        return 1;
    }
    std::cout << "Connection admission tests passed\n";
    return 0;
}
