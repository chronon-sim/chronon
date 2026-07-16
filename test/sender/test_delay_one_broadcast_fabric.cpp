// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "chronon/Chronon.hpp"

using namespace chronon;

namespace {

#define EXPECT(cond, msg)                                                                     \
    do {                                                                                      \
        if (!(cond)) {                                                                        \
            std::cerr << "FAIL: " << msg << " (" #cond ") at " << __FILE__ << ":" << __LINE__ \
                      << "\n";                                                                \
            std::abort();                                                                     \
        }                                                                                     \
    } while (0)

class TestUnit : public Unit {
public:
    explicit TestUnit(std::string name) : Unit(std::move(name)) {}
};

struct Message {
    uint32_t producer = 0;
    uint32_t sequence = 0;
    uint64_t cycle = 0;
};

using Fabric = DelayOneBroadcastFabric<Message, 2, 2, 8, 3>;

void test_stable_replay_order_and_sparse_cycles() {
    std::cout << "Testing delay-one broadcast replay order... ";

    Fabric fabric;
    fabric.publish(1, 4, Message{1, 0, 4});
    fabric.publish(0, 4, Message{0, 0, 4});
    fabric.publish(0, 4, Message{0, 1, 4});

    for (size_t consumer = 0; consumer < Fabric::CONSUMER_COUNT; ++consumer) {
        std::vector<Message> received;
        for (uint64_t cycle = 1; cycle < 5; ++cycle) {
            fabric.consume(consumer, cycle,
                           [&](const Message& message) { received.push_back(message); });
        }
        EXPECT(received.empty(), "unpublished source cycles must advance the consumer cursor");
        fabric.consume(consumer, 5, [&](const Message& message) { received.push_back(message); });
        EXPECT(received.size() == 3, "consumer must replay every published message");
        EXPECT(received[0].producer == 0 && received[0].sequence == 0,
               "producer id is the primary replay key");
        EXPECT(received[1].producer == 0 && received[1].sequence == 1,
               "original per-producer order must be stable");
        EXPECT(received[2].producer == 1, "higher producer id must replay last");
        EXPECT(fabric.consumedExclusive(consumer) == 5,
               "consumer cursor must publish only after the full replay");

        received.clear();
        fabric.consume(consumer, 6, [&](const Message& message) { received.push_back(message); });
        EXPECT(received.empty(), "an unpublished source cycle is a valid empty cycle");
    }

    std::cout << "PASSED\n";
}

void test_consumer_cycle_guards_and_idle_fast_forward() {
    std::cout << "Testing delay-one broadcast consumer cycle guards... ";

    Fabric fabric;
    fabric.consume(0, 5, [](const Message&) {});
    EXPECT(fabric.consumedExclusive(0) == 5,
           "an activity-scheduled consumer may fast-forward across empty cycles");
    fabric.consume(0, 5, [](const Message&) {});

    bool rejected = false;
    try {
        fabric.consume(0, 4, [](const Message&) {});
    } catch (const std::logic_error&) {
        rejected = true;
    }
    EXPECT(rejected, "a consumer must not move its published cursor backwards");

    std::cout << "PASSED\n";
}

void test_producer_cycle_is_globally_monotonic() {
    std::cout << "Testing delay-one broadcast producer cycle guard... ";

    Fabric fabric;
    fabric.publish(0, 10, Message{0, 0, 10});
    fabric.publish(0, 10, Message{0, 1, 10});

    bool rejected = false;
    try {
        // Cycles 9 and 10 map to different empty ring slots, so this covers
        // global monotonicity rather than the existing slot-reuse guard.
        fabric.publish(0, 9, Message{0, 0, 9});
    } catch (const std::logic_error&) {
        rejected = true;
    }
    EXPECT(rejected, "a producer must reject a globally backwards cycle");

    std::cout << "PASSED\n";
}

void test_ring_reuse_and_slow_consumer_guard() {
    std::cout << "Testing delay-one broadcast ring reuse guard... ";

    Fabric fabric;
    for (uint64_t cycle = 0; cycle < Fabric::RING_DEPTH; ++cycle) {
        fabric.publish(0, cycle, Message{0, 0, cycle});
        for (size_t consumer = 0; consumer < Fabric::CONSUMER_COUNT; ++consumer) {
            fabric.consume(consumer, cycle + 1, [](const Message&) {});
        }
    }
    fabric.publish(0, Fabric::RING_DEPTH, Message{0, 0, Fabric::RING_DEPTH});

    Fabric guarded;
    guarded.publish(0, 0, Message{0, 0, 0});
    bool rejected = false;
    try {
        guarded.publish(0, Fabric::RING_DEPTH, Message{0, 0, Fabric::RING_DEPTH});
    } catch (const std::overflow_error&) {
        rejected = true;
    }
    EXPECT(rejected, "producer must not overwrite a bucket an idle consumer has not read");

    std::cout << "PASSED\n";
}

void test_port_topology_binding() {
    std::cout << "Testing delay-one broadcast port topology binding... ";

    TestUnit p0("p0");
    TestUnit p1("p1");
    TestUnit c0("c0");
    TestUnit c1("c1");
    OutPort<Message> out0{&p0, "out0", 3};
    OutPort<Message> out1{&p1, "out1", 3};
    InPort<Message> in0{&c0, "in0"};
    InPort<Message> in1{&c1, "in1"};

    out0.connect(&in0, 1);
    out0.connect(&in1, 1);
    out1.connect(&in0, 1);
    out1.connect(&in1, 1);

    Fabric fabric;
    fabric.bindProducer(0, out0);
    fabric.bindProducer(1, out1);
    fabric.bindConsumer(0, in0);
    fabric.bindConsumer(1, in1);
    fabric.sealPortTopology();

    EXPECT(fabric.portTopologySealed(), "complete topology must seal");
    EXPECT(out0.dependencyOnlyTransport() && out1.dependencyOnlyTransport(),
           "sealed producer ports must retain dependency-only edges");
    for (const auto& connection : out0.connections()) {
        EXPECT(connection->dependencyOnlyTransport(),
               "every bound connection must stop transporting payloads");
    }

    c0.sleepForever();
    c1.sleepForever();
    fabric.publish(0, 7, Message{0, 0, 7});
    EXPECT(c0.nextActiveCycle() == 8 && c1.nextActiveCycle() == 8,
           "fabric publication must preserve delay-one port wakeups");

    std::cout << "PASSED\n";
}

void test_bad_topology_is_atomic() {
    std::cout << "Testing delay-one broadcast topology rejection... ";

    TestUnit p0("p0");
    TestUnit p1("p1");
    TestUnit c0("c0");
    TestUnit c1("c1");
    OutPort<Message> out0{&p0, "out0", 3};
    OutPort<Message> out1{&p1, "out1", 3};
    InPort<Message> in0{&c0, "in0"};
    InPort<Message> in1{&c1, "in1"};

    out0.connect(&in0, 1);
    out0.connect(&in1, 2);  // invalid delay
    out1.connect(&in0, 1);
    out1.connect(&in1, 1);

    Fabric fabric;
    fabric.bindProducer(0, out0);
    fabric.bindProducer(1, out1);
    fabric.bindConsumer(0, in0);
    fabric.bindConsumer(1, in1);

    bool rejected = false;
    try {
        fabric.sealPortTopology();
    } catch (const std::logic_error&) {
        rejected = true;
    }
    EXPECT(rejected, "non-delay-one topology must be rejected");
    EXPECT(!out0.dependencyOnlyTransport() && !out1.dependencyOnlyTransport(),
           "failed validation must not partially mutate the bus");

    std::cout << "PASSED\n";
}

void test_release_acquire_publication() {
    std::cout << "Testing delay-one broadcast release/acquire publication... ";

    constexpr uint64_t kCycles = 20000;
    using ConcurrentFabric = DelayOneBroadcastFabric<Message, 2, 2, 64, 3>;
    ConcurrentFabric fabric;
    std::atomic<uint64_t> produced{0};
    std::atomic<uint64_t> consumed0{0};
    std::atomic<uint64_t> consumed1{0};

    std::thread producer([&] {
        for (uint64_t cycle = 0; cycle < kCycles; ++cycle) {
            // Keep ring reuse within the same safety contract as lookahead:
            // every consumer stays less than RingDepth cycles behind.
            while (cycle >= ConcurrentFabric::RING_DEPTH &&
                   (consumed0.load(std::memory_order_acquire) <=
                        cycle - ConcurrentFabric::RING_DEPTH ||
                    consumed1.load(std::memory_order_acquire) <=
                        cycle - ConcurrentFabric::RING_DEPTH)) {
                std::this_thread::yield();
            }
            fabric.publish(0, cycle, Message{0, 0, cycle});
            fabric.publish(1, cycle, Message{1, 0, cycle});
            produced.store(cycle + 1, std::memory_order_release);
        }
    });

    auto consume = [&](size_t consumer, std::atomic<uint64_t>& progress) {
        for (uint64_t cycle = 0; cycle < kCycles; ++cycle) {
            while (produced.load(std::memory_order_acquire) <= cycle) {
                std::this_thread::yield();
            }
            size_t count = 0;
            fabric.consume(consumer, cycle + 1, [&](const Message& message) {
                EXPECT(message.cycle == cycle, "acquire must expose the matching payload cycle");
                EXPECT(message.producer == count,
                       "concurrent replay must retain producer-id ordering");
                ++count;
            });
            EXPECT(count == 2, "every concurrent cycle must contain both producer messages");
            progress.store(cycle + 1, std::memory_order_release);
        }
    };

    std::thread consumer0(consume, 0, std::ref(consumed0));
    std::thread consumer1(consume, 1, std::ref(consumed1));
    producer.join();
    consumer0.join();
    consumer1.join();

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    test_stable_replay_order_and_sparse_cycles();
    test_consumer_cycle_guards_and_idle_fast_forward();
    test_producer_cycle_is_globally_monotonic();
    test_ring_reuse_and_slow_consumer_guard();
    test_port_topology_binding();
    test_bad_topology_is_atomic();
    test_release_acquire_publication();
    std::cout << "All delay-one broadcast fabric tests passed.\n";
    return 0;
}
