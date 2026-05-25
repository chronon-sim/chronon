// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_stage_selective_port.cpp
//
// Tests for PortPolicy::StageSelective: enqueue_cycle stamping, StagePredicate
// install/retire/shouldCancel, overlapping flushes (#7), and the removal of
// sender-side cancellation filtering (#8).

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>

#include "chronon/Chronon.hpp"

using namespace chronon;
using namespace chronon::sender;

namespace {

// Always-on assertion (does not vanish under NDEBUG).
#define EXPECT(cond, msg)                                                               \
    do {                                                                                \
        if (!(cond)) {                                                                  \
            std::cerr << "FAIL: " << msg << " (" #cond ")" << " at " << __FILE__ << ":" \
                      << __LINE__ << "\n";                                              \
            std::abort();                                                               \
        }                                                                               \
    } while (0)

// Test-only Unit that exposes setLocalCycle().
class TestUnit : public Unit {
public:
    explicit TestUnit(std::string name) : Unit(std::move(name)) {}
    void setCycle(uint64_t c) { setLocalCycle(c); }
};

struct TaggedMsg {
    uint64_t key;
    int payload = 0;

    static uint64_t getKey(const TaggedMsg& m) { return m.key; }
};

// -----------------------------------------------------------------------------
// Test A: basic flush cancellation semantic
//
// Producer pushes keys 10, 20, 30 at cycles 0, 1, 2 (delay=1, so they arrive
// at cycles 1, 2, 3 with enqueue_cycle stamps 0, 1, 2). At consumer cycle 2,
// install cancelYoungerThan(15) -> flush_cycle=2, max_keep=15.
//
// Expected at consumer cycle 3:
//   key=10: enqueue_cycle=0 < 2 (flush) AND 10 <= 15 (max_keep) -> SURVIVES
//   key=20: enqueue_cycle=1 < 2 AND 20 > 15                     -> CANCELLED
//   key=30: enqueue_cycle=2 NOT < 2                             -> SURVIVES
// -----------------------------------------------------------------------------
void test_basic_flush_semantic() {
    std::cout << "Testing basic StageSelective flush semantic... ";

    TestUnit prod("prod");
    TestUnit cons("cons");

    OutPort<TaggedMsg> out{&prod, "out"};
    InPort<TaggedMsg> in{&cons, "in", InPort<TaggedMsg>::UNLIMITED_CAPACITY,
                         PortPolicy::StageSelective};
    out.connect(&in, /*delay=*/1);

    prod.setCycle(0);
    EXPECT(out.send(TaggedMsg{.key = 10, .payload = 100}), "push 10");
    prod.setCycle(1);
    EXPECT(out.send(TaggedMsg{.key = 20, .payload = 200}), "push 20");
    prod.setCycle(2);
    EXPECT(out.send(TaggedMsg{.key = 30, .payload = 300}), "push 30");

    cons.setCycle(2);
    in.cancelYoungerThan<&TaggedMsg::getKey>(uint64_t{15});

    cons.setCycle(3);
    auto v1 = in.tryReceive(3);
    EXPECT(v1.has_value(), "first pop should return key=10");
    EXPECT(v1->key == 10, "first survivor must be key=10");

    auto v2 = in.tryReceive(3);
    EXPECT(v2.has_value(), "second pop should return key=30 (skipping cancelled key=20)");
    EXPECT(v2->key == 30, "second survivor must be key=30");

    auto v3 = in.tryReceive(3);
    EXPECT(!v3.has_value(), "no more messages");

    std::cout << "PASSED\n";
}

// -----------------------------------------------------------------------------
// Test B: overlapping flush (#7 scenario)
//
// Two flushes installed at the same receiver cycle: max_keep=100 and
// max_keep=200. Both predicates must remain live simultaneously and apply
// independently. A message with key=300 triggers BOTH (300 > 100 AND 300 > 200).
// A message with key=150 triggers ONLY the first (150 > 100, 150 <= 200)
// -> still cancelled because ANY matching predicate cancels. A message with
// key=50 matches NEITHER -> survives.
// -----------------------------------------------------------------------------
void test_overlapping_flush_predicates() {
    std::cout << "Testing overlapping flush predicates (#7)... ";

    TestUnit prod("prod");
    TestUnit cons("cons");

    OutPort<TaggedMsg> out{&prod, "out"};
    InPort<TaggedMsg> in{&cons, "in", 64, PortPolicy::StageSelective};
    out.connect(&in, /*delay=*/1);

    prod.setCycle(4);
    EXPECT(out.send(TaggedMsg{.key = 50}), "push 50");
    EXPECT(out.send(TaggedMsg{.key = 150}), "push 150");
    EXPECT(out.send(TaggedMsg{.key = 300}), "push 300");

    cons.setCycle(5);
    in.cancelYoungerThan<&TaggedMsg::getKey>(uint64_t{100});
    in.cancelYoungerThan<&TaggedMsg::getKey>(uint64_t{200});

    auto v1 = in.tryReceive(5);
    EXPECT(v1.has_value(), "key=50 should survive");
    EXPECT(v1->key == 50, "first survivor must be key=50");

    auto v2 = in.tryReceive(5);
    EXPECT(!v2.has_value(),
           "key=150 cancelled by predicate#1, key=300 cancelled by both -> none left");

    std::cout << "PASSED\n";
}

// -----------------------------------------------------------------------------
// Test C: retirement boundary
//
// A predicate installed at receiver cycle 5 with max_keep=100 is live.
// A message stamped enqueue_cycle=4, key=200 pushed such that arrive_cycle=6
// is still cancelled (enqueue_cycle=4 < 5).
// After the predicate retires pop-driven (fresh message has enqueue_cycle >= 5),
// a fresh message survives.
// -----------------------------------------------------------------------------
void test_retirement_boundary() {
    std::cout << "Testing pop-driven predicate retirement... ";

    TestUnit prod("prod");
    TestUnit cons("cons");

    OutPort<TaggedMsg> out{&prod, "out"};
    InPort<TaggedMsg> in{&cons, "in", 64, PortPolicy::StageSelective};
    out.connect(&in, /*delay=*/2);

    // Producer pushes at cycle 4 -> enqueue_cycle=4, arrive_cycle=6.
    prod.setCycle(4);
    EXPECT(out.send(TaggedMsg{.key = 200, .payload = 1}), "push #1");
    EXPECT(out.send(TaggedMsg{.key = 200, .payload = 2}), "push #2");

    // Install predicate at cycle 5: flush_cycle=5, max_keep=100.
    cons.setCycle(5);
    in.cancelYoungerThan<&TaggedMsg::getKey>(uint64_t{100});

    // At cycle 6, the predicate is still live. Both queued messages have
    // enqueue_cycle=4 < flush_cycle=5 AND key=200 > 100, so both must be
    // cancelled.
    cons.setCycle(6);
    auto v_drain = in.tryReceive(6);
    EXPECT(!v_drain.has_value(), "both pre-flush messages must be cancelled");

    // Push a fresh message at producer cycle 6 -> enqueue_cycle=6, arrive=8.
    prod.setCycle(6);
    EXPECT(out.send(TaggedMsg{.key = 300, .payload = 3}), "push fresh");

    // At cycle 8, tryReceive pops the fresh message (enqueue_cycle=6 >=
    // flush_cycle=5), which retires the predicate pop-driven.
    cons.setCycle(8);
    auto v3 = in.tryReceive(8);
    EXPECT(v3.has_value(), "fresh message must survive");
    EXPECT(v3->key == 300, "survivor must be key=300");

    std::cout << "PASSED\n";
}

// -----------------------------------------------------------------------------
// Test D: sender-side filter removal does not block pushes
//
// Push N messages all of which would have been filtered by the OLD sender-side
// receiver-canceled check. With StageSelective, pushes succeed regardless of
// the cancel predicate; filtering happens at pop time. Verify all pushes
// returned true (no early-reject) and that the queue actually holds them.
// -----------------------------------------------------------------------------
void test_sender_filter_removal() {
    std::cout << "Testing sender filter removal (no early-reject)... ";

    constexpr size_t kN = 16;

    TestUnit prod("prod");
    TestUnit cons("cons");

    OutPort<TaggedMsg> out{&prod, "out"};
    InPort<TaggedMsg> in{&cons, "in", /*capacity=*/kN * 2, PortPolicy::StageSelective};
    out.connect(&in, /*delay=*/1);

    // Install a predicate FIRST so the OLD design would reject all sends.
    // Predicate flush_cycle=0 with max_keep=0: would catch any key>0 message
    // whose enqueue_cycle < 0 (impossible). So practically no message will be
    // cancelled, but the predicate IS active, which historically triggered
    // sender-side cancellation reads.
    cons.setCycle(0);
    in.cancelYoungerThan<&TaggedMsg::getKey>(uint64_t{0});

    // Producer pushes N messages at cycle 1 -> enqueue_cycle=1.
    prod.setCycle(1);
    for (size_t i = 1; i <= kN; ++i) {
        EXPECT(out.send(TaggedMsg{.key = i, .payload = static_cast<int>(i)}),
               "push must not be early-rejected");
    }

    // Verify all N messages actually occupy queue slots.
    EXPECT(in.queuedMessageCount() == kN, "queue must hold all N messages");

    // Drain at cycle 2. enqueue_cycle=1 is NOT less than flush_cycle=0
    // (1 < 0 is false), so NO message matches. All survive.
    cons.setCycle(2);
    size_t received = 0;
    while (auto m = in.tryReceive(2)) {
        ++received;
    }
    EXPECT(received == kN, "all N messages must survive (none were in flight at flush)");

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    test_basic_flush_semantic();
    test_overlapping_flush_predicates();
    test_retirement_boundary();
    test_sender_filter_removal();
    std::cout << "All StageSelective port tests passed.\n";
    return 0;
}
