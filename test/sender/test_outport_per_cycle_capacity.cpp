// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_outport_per_cycle_capacity.cpp
//
// Tests for OutPort per-cycle bandwidth limiting: capacity enforcement,
// counter reset, runtime modification, and query methods.

#include <cassert>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>

#include "chronon/Chronon.hpp"

using namespace chronon;
using namespace chronon::sender;

namespace {

class TestUnit : public Unit {
public:
    explicit TestUnit(std::string name) : Unit(std::move(name)) {}
    void setCycle(uint64_t c) { setLocalCycle(c); }
};

void require(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << "\n";
        std::abort();
    }
}

void test_default_single_entry() {
    std::cout << "Testing default single-entry capacity... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out"};
    InPort<int> in{&cons, "in", 256};
    out.connect(&in, 0);

    prod.setCycle(0);

    // Default output rate models one registered-edge entry per cycle.
    assert(out.canSend());
    assert(out.send(1));
    assert(!out.canSend());
    assert(!out.send(2));

    prod.setCycle(1);
    assert(out.canSend());
    assert(out.send(3));

    std::cout << "PASSED\n";
}

void test_explicit_unlimited() {
    std::cout << "Testing explicit unlimited capacity... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", OutPort<int>::UNLIMITED_CAPACITY};
    InPort<int> in{&cons, "in", 256};
    out.connect(&in, 0);

    prod.setCycle(0);

    // Explicit unlimited capacity should allow all sends until downstream
    // backpressure applies.
    for (int i = 0; i < 100; ++i) {
        assert(out.canSend());
        assert(out.send(i));
    }

    std::cout << "PASSED\n";
}

void test_cap_limits_sends() {
    std::cout << "Testing cap limits sends... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 2};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 0);

    prod.setCycle(0);

    // First two sends should succeed.
    assert(out.canSend());
    assert(out.send(1));

    assert(out.canSend());
    assert(out.send(2));

    // Third send should fail: both canSend() and send().
    assert(!out.canSend());
    assert(!out.send(3));

    std::cout << "PASSED\n";
}

void test_counter_resets_on_new_cycle() {
    std::cout << "Testing counter resets on new cycle... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 2};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 0);

    prod.setCycle(0);
    assert(out.send(1));
    assert(out.send(2));
    assert(!out.canSend());

    // Advance to next cycle: counter should reset.
    prod.setCycle(1);
    assert(out.canSend());
    assert(out.send(3));
    assert(out.send(4));
    assert(!out.canSend());

    std::cout << "PASSED\n";
}

void test_failed_send_no_slot_consumed() {
    std::cout << "Testing failed send does not consume bandwidth slot... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 2};
    // Capacity 1: InPort can only hold 1 item, so second send fails due to backpressure.
    InPort<int> in{&cons, "in", 1};
    out.connect(&in, 0);

    prod.setCycle(0);

    // First send succeeds and fills the InPort.
    assert(out.send(1));
    assert(out.sentThisCycle() == 1);

    // Second send fails because InPort is full (not because of bandwidth cap).
    // The invariant under test: a failed send MUST NOT consume a bandwidth slot.
    assert(!out.send(2));
    assert(out.sentThisCycle() == 1);  // <- the property this test guards

    // Advance to next cycle: bandwidth counter resets on cycle advance,
    // independent of when the receiver drains (rate-based admission).
    [[maybe_unused]] auto drain = in.tryReceive(0);
    assert(drain.has_value());
    prod.setCycle(1);

    // Fresh cycle: two bandwidth slots, InPort drained — both sends should fit.
    assert(out.canSend());
    assert(out.send(3));
    assert(out.sentThisCycle() == 1);

    std::cout << "PASSED\n";
}

void test_fanout_send_is_all_or_none_for_const_payload() {
    std::cout << "Testing fanout send is all-or-none for const payload... ";

    TestUnit prod("prod");
    TestUnit cons0("cons0");
    TestUnit cons1("cons1");
    OutPort<int> out{&prod, "out", 2};
    InPort<int> in0{&cons0, "in0", 2};
    InPort<int> in1{&cons1, "in1", 1};
    out.connect(&in0, 0);
    out.connect(&in1, 0);

    prod.setCycle(0);

    int first = 1;
    require(out.send(first), "first const fanout send should succeed");

    int second = 2;
    require(!out.send(second), "second const fanout send should fail atomically");

    auto in0_first = in0.tryReceive(0);
    require(in0_first.has_value(), "destination 0 should receive first message");
    require(*in0_first == 1, "destination 0 first message value");
    require(!in0.tryReceive(0).has_value(), "destination 0 should not receive failed fanout");

    auto in1_first = in1.tryReceive(0);
    require(in1_first.has_value(), "destination 1 should receive first message");
    require(*in1_first == 1, "destination 1 first message value");
    require(!in1.tryReceive(0).has_value(), "destination 1 should not receive failed fanout");

    std::cout << "PASSED\n";
}

void test_fanout_send_is_all_or_none_for_rvalue_payload() {
    std::cout << "Testing fanout send is all-or-none for rvalue payload... ";

    TestUnit prod("prod");
    TestUnit cons0("cons0");
    TestUnit cons1("cons1");
    OutPort<int> out{&prod, "out", 2};
    InPort<int> in0{&cons0, "in0", 2};
    InPort<int> in1{&cons1, "in1", 1};
    out.connect(&in0, 0);
    out.connect(&in1, 0);

    prod.setCycle(0);

    require(out.send(1), "first rvalue fanout send should succeed");
    require(!out.send(2), "second rvalue fanout send should fail atomically");

    auto in0_first = in0.tryReceive(0);
    require(in0_first.has_value(), "destination 0 should receive first rvalue message");
    require(*in0_first == 1, "destination 0 first rvalue message value");
    require(!in0.tryReceive(0).has_value(),
            "destination 0 should not receive failed rvalue fanout");

    auto in1_first = in1.tryReceive(0);
    require(in1_first.has_value(), "destination 1 should receive first rvalue message");
    require(*in1_first == 1, "destination 1 first rvalue message value");
    require(!in1.tryReceive(0).has_value(),
            "destination 1 should not receive failed rvalue fanout");

    std::cout << "PASSED\n";
}

void test_set_per_cycle_capacity_runtime() {
    std::cout << "Testing setPerCycleCapacity runtime modification... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 1};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 0);

    prod.setCycle(0);

    // Cap=1: one send allowed.
    assert(out.send(1));
    assert(!out.canSend());

    // Increase capacity at runtime to 3.
    out.setPerCycleCapacity(3);
    assert(out.perCycleCapacity() == 3);

    // Should now allow 2 more sends (already sent 1 this cycle).
    assert(out.canSend());
    assert(out.send(2));
    assert(out.send(3));
    assert(!out.canSend());

    // Set to unlimited.
    out.setPerCycleCapacity(0);
    assert(out.perCycleCapacity() == OutPort<int>::UNLIMITED_CAPACITY);
    assert(out.canSend());
    assert(out.send(4));

    std::cout << "PASSED\n";
}

void test_cancel_does_not_reset_counter() {
    std::cout << "Testing cancelInFlight does not reset per-cycle counter... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 2};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 5);

    prod.setCycle(0);

    // Use both bandwidth slots.
    assert(out.send(1));
    assert(out.send(2));
    assert(!out.canSend());

    // Cancel in-flight should not reset the per-cycle counter.
    out.cancelInFlight();
    assert(!out.canSend());
    assert(!out.send(3));

    std::cout << "PASSED\n";
}

void test_query_methods() {
    std::cout << "Testing sentThisCycle and remainingThisCycle... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 3};
    InPort<int> in{&cons, "in", 64};
    out.connect(&in, 0);

    prod.setCycle(0);

    // Before any sends.
    assert(out.sentThisCycle() == 0);
    assert(out.remainingThisCycle() == 3);

    // After one send.
    assert(out.send(1));
    assert(out.sentThisCycle() == 1);
    assert(out.remainingThisCycle() == 2);

    // After two sends.
    assert(out.send(2));
    assert(out.sentThisCycle() == 2);
    assert(out.remainingThisCycle() == 1);

    // After three sends (exhausted).
    assert(out.send(3));
    assert(out.sentThisCycle() == 3);
    assert(out.remainingThisCycle() == 0);

    // Test unlimited query behavior.
    TestUnit prod2("prod2");
    TestUnit cons2("cons2");
    OutPort<int> out2{&prod2, "out2", OutPort<int>::UNLIMITED_CAPACITY};
    InPort<int> in2{&cons2, "in2", 64};
    out2.connect(&in2, 0);

    prod2.setCycle(0);
    assert(out2.sentThisCycle() == 0);
    assert(out2.remainingThisCycle() == OutPort<int>::UNLIMITED_CAPACITY);

    // Even after sends, unlimited reports 0 sent and UNLIMITED_CAPACITY remaining.
    assert(out2.send(42));
    assert(out2.sentThisCycle() == 0);
    assert(out2.remainingThisCycle() == OutPort<int>::UNLIMITED_CAPACITY);

    std::cout << "PASSED\n";
}

void test_connection_and_receive_convenience_methods() {
    std::cout << "Testing connection and receive convenience methods... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 2};
    InPort<int> in{&cons, "in", 64};
    InPort<int> other{&cons, "other", 64};
    [[maybe_unused]] auto* conn = out.connect(&in, 0);

    prod.setCycle(7);
    cons.setCycle(7);

    assert(out.connectionTo(&in) == conn);
    assert(out.connectionTo(&other) == nullptr);
    assert(!in.hasMessages());

    assert(out.send(1));
    assert(out.send(2));
    assert(in.hasMessages());

    auto messages = in.receiveAll();
    assert(messages.size() == 2);
    assert(messages[0] == 1);
    assert(messages[1] == 2);
    assert(!in.hasMessages());

    std::cout << "PASSED\n";
}

void test_capacity_one_rate_one_special_case_requires_delay_one() {
    std::cout << "Testing capacity-one/rate-one special case requires delay one... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 1};
    InPort<int> in{&cons, "in", 1};
    auto* conn = out.connect(&in, 2);
    conn->configureRegisteredEdge(/*capacity=*/1, /*rate=*/1);
    conn->optimizeForSPSC();

    require(conn->crossThreadHeadroom() == 0,
            "delay>1 capacity-one/rate-one edge used DFF-style headroom");
    require(!conn->ensureEpochFreeHeadroom(8),
            "delay>1 capacity-one/rate-one edge accepted bounded epoch-free headroom");

    prod.setCycle(0);
    require(out.canSend(), "delay>1 edge rejected first send");
    require(out.send(1), "delay>1 edge failed first send");

    prod.setCycle(1);
    require(!out.canSend(), "delay>1 edge reused capacity before arrival cycle");
    require(!out.send(2), "delay>1 edge sent before arrival cycle freed capacity");

    auto first = in.tryReceive(2);
    require(first.has_value() && *first == 1, "delay>1 edge did not deliver at arrival cycle");

    require(!out.canSend(), "delay>1 edge reused same-cycle drain credit");
    prod.setCycle(3);
    require(out.canSend(), "delay>1 edge did not release prior-cycle drain credit");
    require(out.send(2), "delay>1 edge failed after prior-cycle drain credit");

    std::cout << "PASSED\n";
}

void test_capacity_one_rate_one_delay_one_backpressures_after_missed_drain() {
    std::cout << "Testing capacity-one/rate-one delay-one backpressures after missed drain... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 1};
    InPort<int> in{&cons, "in", 1};
    auto* conn = out.connect(&in, 1);
    conn->configureRegisteredEdge(/*capacity=*/1, /*rate=*/1);
    conn->optimizeForSameThread();

    prod.setCycle(0);
    require(out.canSend(), "delay-1 edge rejected first send");
    require(out.send(1), "delay-1 edge failed first send");

    prod.setCycle(1);
    require(out.canSend(), "delay-1 edge rejected same-cycle DFF refill");
    require(out.send(2), "delay-1 edge failed same-cycle DFF refill");

    prod.setCycle(2);
    require(!out.canSend(), "delay-1 edge ignored a stale undrained output");
    require(!out.send(3), "delay-1 edge kept accepting while receiver stalled");

    auto first = in.tryReceive(2);
    require(first.has_value() && *first == 1, "delay-1 edge did not deliver at arrival cycle");
    require(out.canSend(), "delay-1 edge did not reopen after receiver drained stale output");
    require(out.send(3), "delay-1 edge failed after receiver drained stale output");

    std::cout << "PASSED\n";
}

void test_capacity_one_rate_one_spsc_reconstructs_cycle_start_min_arrival() {
    std::cout << "Testing capacity-one/rate-one SPSC reconstructs cycle-start min arrival... ";

    TestUnit prod("prod");
    TestUnit cons("cons");
    OutPort<int> out{&prod, "out", 1};
    InPort<int> in{&cons, "in", 1};
    auto* conn = out.connect(&in, 1);
    conn->configureRegisteredEdge(/*capacity=*/1, /*rate=*/1);
    conn->optimizeForSPSC();

    prod.setCycle(0);
    require(out.canSend(), "SPSC delay-1 edge rejected first send");
    require(out.send(1), "SPSC delay-1 edge failed first send");

    prod.setCycle(1);
    require(out.canSend(), "SPSC delay-1 edge rejected current-cycle DFF output");
    require(out.send(2), "SPSC delay-1 edge failed current-cycle DFF refill");

    auto first = in.tryReceive(2);
    require(first.has_value() && *first == 1, "SPSC delay-1 edge did not drain stale output");
    prod.setCycle(2);
    require(!out.canSend(), "SPSC delay-1 edge ignored stale cycle-start output");
    require(!out.send(3), "SPSC delay-1 edge accepted after same-cycle stale pop");

    auto second = in.tryReceive(3);
    require(second.has_value() && *second == 2, "SPSC delay-1 edge did not drain second output");
    prod.setCycle(3);
    require(!out.canSend(), "SPSC delay-1 edge ignored second stale cycle-start output");
    require(!out.send(3), "SPSC delay-1 edge accepted while second output was stale");

    prod.setCycle(4);
    require(out.canSend(), "SPSC delay-1 edge did not reopen after stale outputs drained");
    require(out.send(3), "SPSC delay-1 edge failed after stale outputs drained");

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== OutPort Per-Cycle Capacity Tests ===\n\n";

    test_default_single_entry();
    test_explicit_unlimited();
    test_cap_limits_sends();
    test_counter_resets_on_new_cycle();
    test_failed_send_no_slot_consumed();
    test_fanout_send_is_all_or_none_for_const_payload();
    test_fanout_send_is_all_or_none_for_rvalue_payload();
    test_set_per_cycle_capacity_runtime();
    test_cancel_does_not_reset_counter();
    test_query_methods();
    test_connection_and_receive_convenience_methods();
    test_capacity_one_rate_one_special_case_requires_delay_one();
    test_capacity_one_rate_one_delay_one_backpressures_after_missed_drain();
    test_capacity_one_rate_one_spsc_reconstructs_cycle_start_min_arrival();

    std::cout << "\n=== All OutPort Per-Cycle Capacity tests PASSED ===\n";
    return 0;
}
