// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_port_flush_cancel.cpp
//
// Tests for OutPort::cancelInFlight(), InPort::flush(), and InPort selective cancellation.

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "chronon/Chronon.hpp"

using namespace chronon;
using namespace chronon::sender;

namespace {

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

class TestUnit : public Unit {
public:
    explicit TestUnit(std::string name) : Unit(std::move(name)) {}

    void setCycle(uint64_t c) { setLocalCycle(c); }
};

struct TaggedMsg {
    uint64_t id;
    uint64_t seq;
    int payload;
};

#if CHRONON_ENABLE_OUTPORT_CANCELLATION
void test_delayed_cancel_drops_message() {
    std::cout << "Testing delayed cancel drops message... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 16};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 5);

    prod.setCycle(0);
    assert(out.send(123));

    out.cancelInFlight();

    // Message should be dropped at arrival.
    assert(!in.tryReceive(4).has_value());
    assert(!in.tryReceive(5).has_value());

    std::cout << "PASSED\n";
}

void test_delayed_cancel_only_affects_prior_epoch() {
    std::cout << "Testing delayed cancel keeps new epoch... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 16};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 3);

    prod.setCycle(0);
    assert(out.send(1));

    out.cancelInFlight();

    prod.setCycle(2);
    assert(out.send(2));

    // Cycle 3: first message would arrive, but should be canceled.
    assert(!in.tryReceive(3).has_value());

    // Cycle 5: second message arrives.
    [[maybe_unused]] auto v = in.tryReceive(5);
    assert(v.has_value());
    assert(*v == 2);

    std::cout << "PASSED\n";
}

void test_fanout_cancel_drops_all() {
    std::cout << "Testing fanout cancel drops all... ";

    TestUnit prod("prod");
    TestUnit cons0("cons0");
    TestUnit cons1("cons1");

    OutPort<int> out{&prod, "out", 16};
    InPort<int> in0{&cons0, "in0", 64};
    InPort<int> in1{&cons1, "in1", 64};

    out.connect(&in0, 2);
    out.connect(&in1, 2);

    prod.setCycle(10);
    assert(out.send(99));
    out.cancelInFlight();

    assert(!in0.tryReceive(12).has_value());
    assert(!in1.tryReceive(12).has_value());

    std::cout << "PASSED\n";
}

void test_delay0_cancel_before_receive() {
    std::cout << "Testing delay=0 cancel before receive... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 16};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 0);

    prod.setCycle(0);
    assert(out.send(7));
    out.cancelInFlight();

    // Same-cycle receive should observe cancellation if not yet consumed.
    assert(!in.tryReceive(0).has_value());

    std::cout << "PASSED\n";
}
#endif

void test_inport_flush_drops_future_messages() {
    std::cout << "Testing InPort flush drops future messages... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 16};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 10);

    prod.setCycle(0);
    assert(out.send(1));
    assert(out.send(2));

    // Flush before arrival.
    in.flush();

    assert(!in.tryReceive(10).has_value());
    assert(!in.tryReceive(100).has_value());

    std::cout << "PASSED\n";
}

void test_inport_selective_cancel_basic() {
    std::cout << "Testing InPort selective cancel basic... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<TaggedMsg> out{&prod, "out", 16};
    InPort<TaggedMsg> in{&cons, "in", 64};
    out.connect(&in, 5);

    // Send messages with ids 10, 20, 30, 40, 50.
    for ([[maybe_unused]] uint64_t id : {10, 20, 30, 40, 50}) {
        prod.setCycle(0);
        assert(out.send(TaggedMsg{.id = id, .seq = id, .payload = static_cast<int>(id * 10)}));
    }

    // Cancel messages with id < 30 via receiver-side API.
    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(30));

    // Receive at arrival cycle.
    std::vector<uint64_t> received_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(5)) received_ids.push_back(msg->id);
    }

    // ids 10, 20 should be dropped; 30, 40, 50 should survive.
    assert(received_ids.size() == 3);
    assert(received_ids[0] == 30);
    assert(received_ids[1] == 40);
    assert(received_ids[2] == 50);

    std::cout << "PASSED\n";
}

void test_inport_selective_cancel_monotonic_watermark() {
    std::cout << "Testing InPort selective cancel monotonic watermark... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<TaggedMsg> out{&prod, "out", 16};
    InPort<TaggedMsg> in{&cons, "in", 64};
    out.connect(&in, 5);

    // Send messages with ids 5, 10, 15, 20, 25.
    for ([[maybe_unused]] uint64_t id : {5, 10, 15, 20, 25}) {
        prod.setCycle(0);
        assert(out.send(TaggedMsg{.id = id, .seq = id, .payload = 0}));
    }

    // First watermark: cancel id < 10.
    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(10));
    // Raise watermark: cancel id < 20.
    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(20));
    // Lower watermark attempt: should be a no-op (monotonic).
    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(15));

    // Only ids >= 20 should survive.
    std::vector<uint64_t> received_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(5)) received_ids.push_back(msg->id);
    }

    assert(received_ids.size() == 2);
    assert(received_ids[0] == 20);
    assert(received_ids[1] == 25);

    std::cout << "PASSED\n";
}

void test_inport_selective_cancel_younger_than() {
    std::cout << "Testing InPort selective cancel younger-than... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<TaggedMsg> out{&prod, "out", 16};
    InPort<TaggedMsg> in{&cons, "in", 64};
    out.connect(&in, 4);

    for ([[maybe_unused]] uint64_t id : {10, 20, 30, 40, 50}) {
        prod.setCycle(0);
        assert(out.send(TaggedMsg{.id = id, .seq = id, .payload = 0}));
    }

    // Drop keys > 30 (keep 10, 20, 30).
    in.cancelYoungerThan<&TaggedMsg::id>(static_cast<uint64_t>(30));

    std::vector<uint64_t> received_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(4)) received_ids.push_back(msg->id);
    }

    assert(received_ids.size() == 3);
    assert(received_ids[0] == 10);
    assert(received_ids[1] == 20);
    assert(received_ids[2] == 30);

    std::cout << "PASSED\n";
}

void test_inport_selective_cancel_reset() {
    std::cout << "Testing InPort selective cancel reset clears bounds/extractor... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<TaggedMsg> out{&prod, "out", 16};
    InPort<TaggedMsg> in{&cons, "in", 64};
    out.connect(&in, 4);

    // First configure id-based filtering.
    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(20));

    // Reset, then switch to seq-based filtering. Allowed after reset.
    in.resetSelectiveCancellation();

    // Send messages BEFORE the new cancel so they are in-flight when
    // the seq-based policy fires (selective cancel scopes to in-flight).
    prod.setCycle(0);
    assert(out.send(TaggedMsg{.id = 10, .seq = 150, .payload = 0}));  // keep (seq >= 100)
    assert(out.send(TaggedMsg{.id = 30, .seq = 90, .payload = 0}));   // drop (seq < 100)
    assert(out.send(TaggedMsg{.id = 5, .seq = 200, .payload = 0}));   // keep (seq >= 100)

    in.cancelOlderThan<&TaggedMsg::seq>(static_cast<uint64_t>(100));

    std::vector<uint64_t> received_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(4)) received_ids.push_back(msg->id);
    }

    // id=10 must survive, proving prior id<20 bound was cleared.
    assert(received_ids.size() == 2);
    assert(received_ids[0] == 10);
    assert(received_ids[1] == 5);

    std::cout << "PASSED\n";
}

#if CHRONON_ENABLE_OUTPORT_CANCELLATION
void test_inport_selective_cancel_with_epoch_cancel() {
    std::cout << "Testing InPort selective + epoch cancel interaction... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<TaggedMsg> out{&prod, "out", 16};
    InPort<TaggedMsg> in{&cons, "in", 64};
    out.connect(&in, 5);

    // Send messages with ids 10, 20, 30.
    for ([[maybe_unused]] uint64_t id : {10, 20, 30}) {
        prod.setCycle(0);
        assert(out.send(TaggedMsg{.id = id, .seq = id, .payload = 0}));
    }

    // Selective cancel: drop id < 20 (drops id=10).
    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(20));

    // Epoch cancel: drops ALL remaining (ids 20, 30).
    out.cancelInFlight();

    // Nothing should survive.
    assert(!in.tryReceive(5).has_value());

    // New messages after epoch bump are in a new generation and should not
    // be filtered (receiver-side selective cancel was scoped to in-flight).
    prod.setCycle(1);
    assert(out.send(TaggedMsg{.id = 15, .seq = 15, .payload = 0}));
    assert(out.send(TaggedMsg{.id = 25, .seq = 25, .payload = 0}));

    std::vector<uint64_t> received_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(6)) received_ids.push_back(msg->id);
    }

    // Both survive because selective cancel was scoped to old generation.
    assert(received_ids.size() == 2);
    assert(received_ids[0] == 15);
    assert(received_ids[1] == 25);

    std::cout << "PASSED\n";
}
#endif

void test_inport_selective_cancel_scoped_to_inflight() {
    std::cout << "Testing InPort selective cancel scoped to in-flight... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<TaggedMsg> out{&prod, "out", 16};
    InPort<TaggedMsg> in{&cons, "in", 64};
    out.connect(&in, 5);

    // In-flight generation at cycle 0.
    prod.setCycle(0);
    assert(out.send(TaggedMsg{.id = 10, .seq = 10, .payload = 0}));
    assert(out.send(TaggedMsg{.id = 30, .seq = 30, .payload = 0}));

    // InPort selective cancel defaults to in-flight scope.
    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(20));

    // New generation (after cancellation call) should remain unaffected.
    prod.setCycle(1);
    assert(out.send(TaggedMsg{.id = 15, .seq = 15, .payload = 0}));
    assert(out.send(TaggedMsg{.id = 25, .seq = 25, .payload = 0}));

    std::vector<uint64_t> cycle5_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(5)) cycle5_ids.push_back(msg->id);
    }
    assert(cycle5_ids.size() == 1);
    assert(cycle5_ids[0] == 30);

    std::vector<uint64_t> cycle6_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(6)) cycle6_ids.push_back(msg->id);
    }
    assert(cycle6_ids.size() == 2);
    assert(cycle6_ids[0] == 15);
    assert(cycle6_ids[1] == 25);

    std::cout << "PASSED\n";
}

void test_inport_selective_cancel_mismatched_keyfn_is_noop() {
    std::cout << "Testing InPort mismatched KeyFn keeps original extractor... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<TaggedMsg> out{&prod, "out", 16};
    InPort<TaggedMsg> in{&cons, "in", 64};
    out.connect(&in, 4);

    // Send messages BEFORE the cancel so they are in-flight when
    // the id-based policy fires (selective cancel scopes to in-flight).
    prod.setCycle(0);
    assert(out.send(TaggedMsg{.id = 10, .seq = 200, .payload = 0}));
    assert(out.send(TaggedMsg{.id = 30, .seq = 1, .payload = 0}));

    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(20));
    in.cancelOlderThan<&TaggedMsg::seq>(
        static_cast<uint64_t>(100));  // ignored (mismatched extractor)

    [[maybe_unused]] auto msg = in.tryReceive(4);
    assert(msg.has_value());
    assert(msg->id == 30);
    assert(!in.tryReceive(4).has_value());

    std::cout << "PASSED\n";
}

void test_inport_selective_cancel_mpsc_thread_queues() {
    std::cout << "Testing future-arrival selective cancel through MPSC connections... ";

    TestUnit prod0("prod0");
    TestUnit prod1("prod1");
    TestUnit cons("cons");
    OutPort<TaggedMsg> out0{&prod0, "out0", 4};
    OutPort<TaggedMsg> out1{&prod1, "out1", 4};
    InPort<TaggedMsg> in{&cons, "in", 64};
    auto* conn0 = out0.connect(&in, 5);
    auto* conn1 = out1.connect(&in, 5);

    conn0->setConnId(10);
    conn1->setConnId(20);
    conn0->optimizeForMPSC();
    conn1->optimizeForMPSC();
    const size_t lane0 = conn0->registerProducerThread(100);
    const size_t lane1 = conn1->registerProducerThread(200);
    require(lane0 != SIZE_MAX && lane1 != SIZE_MAX && lane0 != lane1,
            "failed to register direct MPSC lanes");
    conn0->setThreadQueueId(lane0);
    conn1->setThreadQueueId(lane1);
    require(conn0->registerOnDestMPSC() != nullptr && conn1->registerOnDestMPSC() != nullptr,
            "failed to register MPSC connections on the destination");
    require(in.isMultiProducerMode(), "test did not select the direct MPSC transport");

    // Already-published messages are in flight even though delay=5 keeps them
    // invisible until cycle 5. This scope must match the ordinary queue path.
    prod0.setCycle(0);
    prod1.setCycle(0);
    require(out0.send(TaggedMsg{.id = 10, .seq = 10, .payload = 0}),
            "failed to publish old low-key MPSC message");
    require(out1.send(TaggedMsg{.id = 30, .seq = 30, .payload = 0}),
            "failed to publish old retained MPSC message");

    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(20));

    // Messages published after the cancellation enter the next generation.
    prod0.setCycle(1);
    prod1.setCycle(1);
    require(out0.send(TaggedMsg{.id = 15, .seq = 15, .payload = 0}),
            "failed to publish new low-key MPSC message");
    require(out1.send(TaggedMsg{.id = 25, .seq = 25, .payload = 0}),
            "failed to publish new retained MPSC message");

    std::vector<uint64_t> cycle5_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(5)) cycle5_ids.push_back(msg->id);
    }
    assert(cycle5_ids.size() == 1);
    assert(cycle5_ids[0] == 30);

    auto first_new = in.tryReceive(6);
    auto second_new = in.tryReceive(6);
    assert(first_new.has_value() && first_new->id == 15);
    assert(second_new.has_value() && second_new->id == 25);
    assert(!in.tryReceive(6).has_value());

    std::cout << "PASSED\n";
}

void test_receiver_filter_cancels_rejected_messages() {
    std::cout << "Testing receiver filter cancels rejected messages... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 8};
    InPort<int> in{&cons, "in", 16};
    out.connect(&in, 1);

    prod.setCycle(0);
    for (int value = 1; value <= 6; ++value) {
        require(out.send(value), "failed to seed receiver-filter test");
    }

    size_t inspected = 0;
    auto keep_even = [&](const int& value) noexcept {
        ++inspected;
        return (value & 1) == 0;
    };
    for (int expected : {2, 4, 6}) {
        auto value = in.tryReceiveFiltered(1, keep_even);
        require(value.has_value() && *value == expected,
                "receiver filter changed accepted-message order");
    }
    require(!in.tryReceiveFiltered(1, keep_even).has_value(),
            "receiver filter left a ready message behind");
    require(inspected == 6, "receiver filter did not inspect every ready message exactly once");
    require(in.queuedMessageCount() == 0,
            "receiver filter did not permanently consume rejected messages");

    std::cout << "PASSED\n";
}

void test_receiver_filter_direct_spsc_path() {
    std::cout << "Testing receiver filter on direct SPSC backend... ";

    const char* previous = std::getenv("CHRONON_EXPERIMENTAL_DIRECT_SPSC");
    const std::string previous_value = previous ? previous : "";
    const bool had_previous = previous != nullptr;
    unsetenv("CHRONON_EXPERIMENTAL_DIRECT_SPSC");

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 8};
    InPort<int> in{&cons, "in", 16};
    out.connect(&in, 1);
    in.useLockFreeQueue();
    require(in.usesDirectSPSC(), "production-default direct SPSC backend was not selected");

    prod.setCycle(0);
    for (int value = 1; value <= 6; ++value) {
        require(out.send(value), "failed to seed direct-SPSC receiver-filter test");
    }

    size_t inspected = 0;
    auto keep_even = [&](const int& value) noexcept {
        ++inspected;
        return (value & 1) == 0;
    };
    for (int expected : {2, 4, 6}) {
        auto value = in.tryReceiveFiltered(1, keep_even);
        require(value.has_value() && *value == expected,
                "direct SPSC receiver filter changed accepted-message order");
    }
    require(!in.tryReceiveFiltered(1, keep_even).has_value(),
            "direct SPSC receiver filter left a ready message behind");
    require(inspected == 6, "direct SPSC filter did not inspect each message exactly once");
    require(in.queuedMessageCount() == 0, "direct SPSC filter did not consume rejected messages");

    if (had_previous) {
        setenv("CHRONON_EXPERIMENTAL_DIRECT_SPSC", previous_value.c_str(), 1);
    } else {
        unsetenv("CHRONON_EXPERIMENTAL_DIRECT_SPSC");
    }

    std::cout << "PASSED\n";
}

void test_receiver_filter_preserves_mpsc_order() {
    std::cout << "Testing receiver filter preserves MPSC order... ";

    TestUnit cons("cons");
    InPort<int> in{&cons, "in", 16};
    in.useMultiProducerQueue();
    const size_t q0 = in.registerProducerThread(1);
    const size_t q1 = in.registerProducerThread(2);

    require(in.pushToThreadQueue(q1, 3, 5, 0, 20), "failed to seed MPSC lane 1");
    require(in.pushToThreadQueue(q0, 1, 5, 0, 10), "failed to seed MPSC lane 0 head");
    require(in.pushToThreadQueue(q0, 2, 6, 1, 10), "failed to seed MPSC lane 0 tail");

    auto keep_not_one = [](const int& value) noexcept { return value != 1; };
    auto first = in.tryReceiveFiltered(5, keep_not_one);
    require(first.has_value() && *first == 3,
            "receiver filter did not preserve same-cycle MPSC order");
    auto second = in.tryReceiveFiltered(6, keep_not_one);
    require(second.has_value() && *second == 2,
            "receiver filter did not preserve the next-cycle lane head");
    require(!in.tryReceiveFiltered(6, keep_not_one).has_value(),
            "receiver filter left an unexpected MPSC message");

    std::cout << "PASSED\n";
}

#if CHRONON_ENABLE_OUTPORT_CANCELLATION
void test_outport_cancel_isolated_between_mpsc_lanes() {
    std::cout << "Testing OutPort cancellation isolation across MPSC lanes... ";

    TestUnit prod0("prod0");
    TestUnit prod1("prod1");
    TestUnit cons("cons");
    OutPort<int> out0{&prod0, "out0", 4};
    OutPort<int> out1{&prod1, "out1", 4};
    InPort<int> in{&cons, "in", 16};
    auto* conn0 = out0.connect(&in, 2);
    auto* conn1 = out1.connect(&in, 2);

    conn0->setConnId(10);
    conn1->setConnId(20);
    conn0->optimizeForMPSC();
    conn1->optimizeForMPSC();
    const size_t lane0 = conn0->registerProducerThread(101);
    const size_t lane1 = conn1->registerProducerThread(202);
    require(lane0 != SIZE_MAX && lane1 != SIZE_MAX && lane0 != lane1,
            "failed to register independent MPSC lanes");
    conn0->setThreadQueueId(lane0);
    conn1->setThreadQueueId(lane1);

    prod0.setCycle(0);
    prod1.setCycle(0);
    require(out0.send(100), "failed to send cancelable lane-0 message");
    require(out1.send(200), "failed to send lane-1 control message");
    out0.cancelInFlight();

    prod0.setCycle(1);
    prod1.setCycle(1);
    require(out0.send(101), "failed to send post-cancel lane-0 message");
    require(out1.send(201), "failed to send second lane-1 control message");

    auto cycle2 = in.tryReceive(2);
    require(cycle2.has_value() && *cycle2 == 200,
            "canceling one OutPort discarded or reordered another producer lane");
    require(!in.tryReceive(2).has_value(), "canceled MPSC entry remained observable");

    auto first_cycle3 = in.tryReceive(3);
    auto second_cycle3 = in.tryReceive(3);
    require(first_cycle3.has_value() && *first_cycle3 == 101,
            "post-cancel epoch did not survive on its producer lane");
    require(second_cycle3.has_value() && *second_cycle3 == 201,
            "MPSC sender-id order changed after lazy cancellation");
    require(!in.tryReceive(3).has_value(), "unexpected MPSC message remained after drain");

    std::cout << "PASSED\n";
}
#endif

}  // namespace

int main() {
    std::cout << "=== Port Flush/Cancel Tests ===\n\n";

#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    test_delayed_cancel_drops_message();
    test_delayed_cancel_only_affects_prior_epoch();
    test_fanout_cancel_drops_all();
    test_delay0_cancel_before_receive();
#else
    static_assert(sizeof(detail::PortEnvelope<uint64_t>) == 32,
                  "non-cancelable envelope should omit epoch pointer and snapshot");
#endif
    test_inport_flush_drops_future_messages();
    test_inport_selective_cancel_basic();
    test_inport_selective_cancel_monotonic_watermark();
    test_inport_selective_cancel_younger_than();
    test_inport_selective_cancel_reset();
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    test_inport_selective_cancel_with_epoch_cancel();
#endif
    test_inport_selective_cancel_scoped_to_inflight();
    test_inport_selective_cancel_mismatched_keyfn_is_noop();
    test_inport_selective_cancel_mpsc_thread_queues();
    test_receiver_filter_cancels_rejected_messages();
    test_receiver_filter_direct_spsc_path();
    test_receiver_filter_preserves_mpsc_order();
#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    test_outport_cancel_isolated_between_mpsc_lanes();
#endif

    std::cout << "\n=== All Port Flush/Cancel tests PASSED ===\n";
    return 0;
}
