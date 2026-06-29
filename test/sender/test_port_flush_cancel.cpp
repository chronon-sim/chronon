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
#include <iostream>
#include <vector>

#include "chronon/Chronon.hpp"

using namespace chronon;
using namespace chronon::sender;

namespace {

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
    std::cout << "Testing InPort selective cancel on MPSC thread queues... ";

    TestUnit cons("cons");
    InPort<TaggedMsg> in{&cons, "in", 64};
    in.useMultiProducerQueue();

    [[maybe_unused]] const size_t q0 = in.registerProducerThread(0);
    [[maybe_unused]] const size_t q1 = in.registerProducerThread(1);
    assert(q0 != SIZE_MAX);
    assert(q1 != SIZE_MAX);

    // Existing in-flight messages (generation 0).
    assert(in.pushToThreadQueue(q0, TaggedMsg{.id = 10, .seq = 10, .payload = 0}, 5));
    assert(in.pushToThreadQueue(q1, TaggedMsg{.id = 30, .seq = 30, .payload = 0}, 5));

    in.cancelOlderThan<&TaggedMsg::id>(static_cast<uint64_t>(20));

    // New message should be in generation 1 and not filtered by prior scope.
    assert(in.pushToThreadQueue(q0, TaggedMsg{.id = 15, .seq = 15, .payload = 0}, 6));

    std::vector<uint64_t> cycle5_ids;
    for (int i = 0; i < 10; ++i) {
        if (auto msg = in.tryReceive(5)) cycle5_ids.push_back(msg->id);
    }
    assert(cycle5_ids.size() == 1);
    assert(cycle5_ids[0] == 30);

    [[maybe_unused]] auto msg = in.tryReceive(6);
    assert(msg.has_value());
    assert(msg->id == 15);
    assert(!in.tryReceive(6).has_value());

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Port Flush/Cancel Tests ===\n\n";

    test_delayed_cancel_drops_message();
    test_delayed_cancel_only_affects_prior_epoch();
    test_fanout_cancel_drops_all();
    test_delay0_cancel_before_receive();
    test_inport_flush_drops_future_messages();
    test_inport_selective_cancel_basic();
    test_inport_selective_cancel_monotonic_watermark();
    test_inport_selective_cancel_younger_than();
    test_inport_selective_cancel_reset();
    test_inport_selective_cancel_with_epoch_cancel();
    test_inport_selective_cancel_scoped_to_inflight();
    test_inport_selective_cancel_mismatched_keyfn_is_noop();
    test_inport_selective_cancel_mpsc_thread_queues();

    std::cout << "\n=== All Port Flush/Cancel tests PASSED ===\n";
    return 0;
}
