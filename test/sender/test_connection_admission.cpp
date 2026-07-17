// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

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
    require(in.queuedMessageCount() == kDepth, "MPSC lane did not fill to declared depth");

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

}  // namespace

int main() {
    try {
        testRegisteredEdgeRejectsZeroCapacityAndRate();
        testMPSCCapacityIgnoresSameCycleConsumerPopInterleaving();
    } catch (const std::exception& error) {
        std::cerr << "Connection admission test failed: " << error.what() << '\n';
        return 1;
    }
    std::cout << "Connection admission tests passed\n";
    return 0;
}
