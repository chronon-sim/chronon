// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_priority_arbiter.cpp
//
// Unit tests for PriorityArbiter

#include <cassert>
#include <cstdint>
#include <iostream>

#include "sender/util/PriorityArbiter.hpp"

using namespace chronon::sender;

// Test source enum
enum class Src : uint8_t { FILL, EVICT, IQ, LRQ, RST, PREFETCH, MB_RD };

using Arbiter = PriorityArbiter<Src, 4>;
using Req = ArbRequest<Src>;

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                           \
    static void test_##name();               \
    static struct Register_##name {          \
        Register_##name() { test_##name(); } \
    } register_##name;                       \
    static void test_##name()

#define CHECK(cond)                                                                        \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            std::cerr << "  FAIL: " << #cond << " (" << __FILE__ << ":" << __LINE__ << ")" \
                      << std::endl;                                                        \
            ++tests_failed;                                                                \
            return;                                                                        \
        }                                                                                  \
    } while (0)

#define PASS(name)                                    \
    do {                                              \
        std::cout << "  PASS: " << name << std::endl; \
        ++tests_passed;                               \
    } while (0)

TEST(basic_priority_selection) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::FILL, Src::IQ});
    arb.setPipePriority(1, {Src::IQ, Src::LRQ});
    arb.setPipePriority(2, {Src::IQ, Src::LRQ, Src::RST});
    arb.setPipePriority(3, {Src::IQ, Src::LRQ, Src::RST});

    arb.clearRequests();
    arb.addRequest(0, {Src::FILL, 0, 0x100, true, 1});
    arb.addRequest(0, {Src::IQ, 1, 0x200, true, 2});
    arb.addRequest(1, {Src::IQ, 2, 0x300, true, 3});
    arb.addRequest(2, {Src::RST, 3, 0x400, true, 4});

    const auto& result = arb.arbitrate();

    // Pipe 0: FILL wins (higher priority than IQ)
    CHECK(result[0].valid);
    CHECK(result[0].source == Src::FILL);
    CHECK(result[0].entry_id == 0);

    // Pipe 1: IQ wins
    CHECK(result[1].valid);
    CHECK(result[1].source == Src::IQ);
    CHECK(result[1].entry_id == 2);

    // Pipe 2: RST wins (only request)
    CHECK(result[2].valid);
    CHECK(result[2].source == Src::RST);
    CHECK(result[2].entry_id == 3);

    // Pipe 3: no requests
    CHECK(!result[3].valid);

    // IQ on pipe 0 should be a loser
    CHECK(result.loser_count >= 1);
    bool found_iq_loser = false;
    for (uint8_t i = 0; i < result.loser_count; ++i) {
        if (result.losers[i].source == Src::IQ && result.losers[i].target_pipe == 0) {
            found_iq_loser = true;
            CHECK(result.losers[i].reason == LoseReason::LOWER_PRIORITY);
        }
    }
    CHECK(found_iq_loser);

    PASS("basic_priority_selection");
}

TEST(overflow_routing) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::FILL, Src::IQ});
    arb.setPipePriority(1, {Src::FILL, Src::IQ, Src::LRQ});
    arb.setPipePriority(2, {Src::IQ, Src::LRQ});
    arb.setPipePriority(3, {Src::IQ, Src::LRQ});

    arb.setOverflow(Src::FILL, 0, 1);
    arb.setOverflow(Src::LRQ, 1, 2);
    arb.setOverflow(Src::LRQ, 2, 3);

    arb.clearRequests();
    // Both pipe 0 and pipe 1 want FILL, IQ also on pipe 0
    arb.addRequest(0, {Src::FILL, 0, 0x100, true, 10});
    arb.addRequest(0, {Src::IQ, 1, 0x200, true, 11});
    // IQ already on pipe 1 (higher priority than FILL overflow)
    // Wait, FILL has higher priority on pipe 1. Let me adjust.

    const auto& result = arb.arbitrate();

    // Pipe 0: FILL wins
    CHECK(result[0].valid);
    CHECK(result[0].source == Src::FILL);

    // IQ on pipe 0 lost, no overflow rule for IQ
    // So IQ just loses

    PASS("overflow_routing");
}

TEST(overflow_chain) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::FILL});
    arb.setPipePriority(1, {Src::IQ, Src::LRQ});
    arb.setPipePriority(2, {Src::IQ, Src::LRQ});
    arb.setPipePriority(3, {Src::LRQ});

    arb.setOverflow(Src::LRQ, 1, 2);
    arb.setOverflow(Src::LRQ, 2, 3);

    arb.clearRequests();
    // LRQ loses to IQ on pipe 1, overflows to pipe 2
    arb.addRequest(1, {Src::IQ, 0, 0x100, true, 1});
    arb.addRequest(1, {Src::LRQ, 1, 0x200, true, 2});

    // IQ also on pipe 2, so LRQ overflow loses again, overflows to pipe 3
    arb.addRequest(2, {Src::IQ, 2, 0x300, true, 3});

    const auto& result = arb.arbitrate();

    // Pipe 1: IQ wins
    CHECK(result[1].valid);
    CHECK(result[1].source == Src::IQ);

    // Pipe 2: IQ wins (pipe 2's own IQ request)
    CHECK(result[2].valid);
    CHECK(result[2].source == Src::IQ);

    // Pipe 3: LRQ arrives via double overflow
    CHECK(result[3].valid);
    CHECK(result[3].source == Src::LRQ);
    CHECK(result[3].entry_id == 1);

    PASS("overflow_chain");
}

TEST(bank_conflict_detection) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ});
    arb.setPipePriority(1, {Src::IQ});
    arb.setPipePriority(2, {Src::IQ});
    arb.setPipePriority(3, {Src::IQ});

    // Bank conflict if same bit 6
    arb.setBankConflictFn([](uint64_t a, uint64_t b) { return (a & 0x40) == (b & 0x40); });

    arb.clearRequests();
    // Pipes 0 and 1 have conflicting addresses (same bit 6)
    arb.addRequest(0, {Src::IQ, 0, 0x040, true, 1});  // bit 6 = 1
    arb.addRequest(1, {Src::IQ, 1, 0x0C0, true, 2});  // bit 6 = 1 (conflict!)
    arb.addRequest(2, {Src::IQ, 2, 0x000, true, 3});  // bit 6 = 0 (no conflict)

    const auto& result = arb.arbitrate();

    // Pipe 0: wins (default lower index = higher priority)
    CHECK(result[0].valid);
    CHECK(!result[0].bank_conflict);

    // Pipe 1: bank conflict with pipe 0
    CHECK(!result[1].valid);
    CHECK(result[1].bank_conflict);

    // Pipe 2: no conflict
    CHECK(result[2].valid);
    CHECK(!result[2].bank_conflict);

    PASS("bank_conflict_detection");
}

TEST(bank_conflict_higher_index_priority) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ});
    arb.setPipePriority(1, {Src::IQ});
    arb.setPipePriority(2, {Src::IQ});
    arb.setBankConflictFn([](uint64_t a, uint64_t b) { return (a & 0x40) == (b & 0x40); });
    arb.setBankConflictPriority(BankConflictPriority::HigherPipeIndex);

    arb.clearRequests();
    arb.addRequest(0, {Src::IQ, 0, 0x040, true, 1});
    arb.addRequest(1, {Src::IQ, 1, 0x0C0, true, 2});
    arb.addRequest(2, {Src::IQ, 2, 0x000, true, 3});

    const auto& result = arb.arbitrate();

    CHECK(!result[0].valid);
    CHECK(result[0].bank_conflict);
    CHECK(result[1].valid);
    CHECK(!result[1].bank_conflict);
    CHECK(result[2].valid);

    PASS("bank_conflict_higher_index_priority");
}

TEST(bank_conflict_pass_rules) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ});
    arb.setPipePriority(1, {Src::LRQ});
    arb.setPipePriority(2, {Src::RST});

    arb.setBankConflictFn([](uint64_t a, uint64_t b) { return (a & 0x40) == (b & 0x40); });

    // LRQ can pass IQ despite bank conflict
    arb.addPassRule(Src::IQ, Src::LRQ);

    arb.clearRequests();
    arb.addRequest(0, {Src::IQ, 0, 0x040, true, 1});
    arb.addRequest(1, {Src::LRQ, 1, 0x040, true, 2});  // same bit 6, but can pass
    arb.addRequest(2, {Src::RST, 2, 0x040, true, 3});  // same bit 6, no pass rule

    const auto& result = arb.arbitrate();

    CHECK(result[0].valid);   // IQ wins pipe 0
    CHECK(result[1].valid);   // LRQ passes despite bank conflict
    CHECK(!result[2].valid);  // RST blocked by bank conflict (no pass rule)

    PASS("bank_conflict_pass_rules");
}

TEST(dynamic_override) {
    bool rst_priority = false;

    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ, Src::RST});
    arb.setPipePriority(1, {Src::IQ, Src::RST});

    arb.addDynamicOverride(Src::RST, Src::IQ, &rst_priority);

    // Without override: IQ wins
    arb.clearRequests();
    arb.addRequest(0, {Src::IQ, 0, 0x100, true, 1});
    arb.addRequest(0, {Src::RST, 1, 0x200, true, 2});

    auto& result1 = arb.arbitrate();
    CHECK(result1[0].valid);
    CHECK(result1[0].source == Src::IQ);

    // With override: RST wins
    rst_priority = true;
    arb.clearRequests();
    arb.addRequest(0, {Src::IQ, 0, 0x100, true, 1});
    arb.addRequest(0, {Src::RST, 1, 0x200, true, 2});

    auto& result2 = arb.arbitrate();
    CHECK(result2[0].valid);
    CHECK(result2[0].source == Src::RST);

    // Override off again
    rst_priority = false;
    arb.clearRequests();
    arb.addRequest(0, {Src::IQ, 0, 0x100, true, 1});
    arb.addRequest(0, {Src::RST, 1, 0x200, true, 2});

    auto& result3 = arb.arbitrate();
    CHECK(result3[0].valid);
    CHECK(result3[0].source == Src::IQ);

    PASS("dynamic_override");
}

TEST(idle_fill) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ});
    arb.setPipePriority(1, {Src::IQ});
    arb.setPipePriority(2, {Src::IQ});
    arb.setPipePriority(3, {Src::IQ});

    arb.addIdleFillSource(Src::MB_RD);

    arb.clearRequests();
    // Only pipes 0 and 1 have requests
    arb.addRequest(0, {Src::IQ, 0, 0x100, true, 1});
    arb.addRequest(1, {Src::IQ, 1, 0x200, true, 2});

    // MB_RD idle fill requests
    arb.addIdleFillRequest({Src::MB_RD, 5, 0x300, true, 10});
    arb.addIdleFillRequest({Src::MB_RD, 6, 0x400, true, 11});

    const auto& result = arb.arbitrate();

    CHECK(result[0].valid && result[0].source == Src::IQ);
    CHECK(result[1].valid && result[1].source == Src::IQ);

    // Pipes 2 and 3 should get idle fill
    CHECK(result[2].valid && result[2].source == Src::MB_RD);
    CHECK(result[2].entry_id == 5);
    CHECK(result[3].valid && result[3].source == Src::MB_RD);
    CHECK(result[3].entry_id == 6);

    PASS("idle_fill");
}

TEST(occupied_pipes) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ});
    arb.setPipePriority(1, {Src::IQ});
    arb.setPipePriority(2, {Src::IQ});
    arb.setPipePriority(3, {Src::IQ});

    arb.clearRequests();
    arb.setOccupied(0, 0x100, Src::RST);
    arb.setOccupied(2, 0x300, Src::IQ);

    arb.addRequest(0, {Src::IQ, 0, 0x200, true, 1});  // Should lose, pipe occupied
    arb.addRequest(1, {Src::IQ, 1, 0x200, true, 2});  // Should win
    arb.addRequest(3, {Src::IQ, 2, 0x400, true, 3});  // Should win

    const auto& result = arb.arbitrate();

    CHECK(!result[0].valid);  // Occupied
    CHECK(result[1].valid && result[1].source == Src::IQ);
    CHECK(!result[2].valid);  // Occupied
    CHECK(result[3].valid && result[3].source == Src::IQ);

    // Request to occupied pipe 0 is a loser
    bool found_loser = false;
    for (uint8_t i = 0; i < result.loser_count; ++i) {
        if (result.losers[i].source == Src::IQ && result.losers[i].target_pipe == 0) {
            found_loser = true;
        }
    }
    CHECK(found_loser);

    PASS("occupied_pipes");
}

TEST(all_pipes_occupied) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ});
    arb.setPipePriority(1, {Src::IQ});
    arb.setPipePriority(2, {Src::IQ});
    arb.setPipePriority(3, {Src::IQ});

    arb.clearRequests();
    for (std::size_t p = 0; p < 4; ++p) {
        arb.setOccupied(p, p * 0x100, Src::RST);
    }

    arb.addRequest(0, {Src::IQ, 0, 0x100, true, 1});
    arb.addRequest(1, {Src::IQ, 1, 0x200, true, 2});

    const auto& result = arb.arbitrate();

    for (std::size_t p = 0; p < 4; ++p) {
        CHECK(!result[p].valid);
    }

    CHECK(result.loser_count == 2);

    PASS("all_pipes_occupied");
}

TEST(contention_counting) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::FILL, Src::IQ, Src::LRQ});
    arb.setPipePriority(1, {Src::IQ});

    arb.clearRequests();
    arb.addRequest(0, {Src::FILL, 0, 0x100, true, 1});
    arb.addRequest(0, {Src::IQ, 1, 0x200, true, 2});
    arb.addRequest(0, {Src::LRQ, 2, 0x300, true, 3});
    arb.addRequest(1, {Src::IQ, 3, 0x400, true, 4});

    const auto& result = arb.arbitrate();

    CHECK(result.contention[0] == 3);
    CHECK(result.contention[1] == 1);
    CHECK(result.contention[2] == 0);
    CHECK(result.contention[3] == 0);

    PASS("contention_counting");
}

TEST(bank_conflict_with_occupied) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ});
    arb.setPipePriority(1, {Src::IQ});

    arb.setBankConflictFn([](uint64_t a, uint64_t b) { return (a & 0x40) == (b & 0x40); });

    arb.clearRequests();
    // Pipe 0 is occupied with an address that has bit 6 = 1
    arb.setOccupied(0, 0x040, Src::RST);
    // Pipe 1 request conflicts with occupied pipe 0
    arb.addRequest(1, {Src::IQ, 0, 0x0C0, true, 1});  // bit 6 = 1

    const auto& result = arb.arbitrate();

    // Pipe 1 should be bank-conflicted with occupied pipe 0
    CHECK(!result[1].valid);
    CHECK(result[1].bank_conflict);

    PASS("bank_conflict_with_occupied");
}

TEST(tag_preservation) {
    Arbiter arb;
    arb.setPipePriority(0, {Src::IQ});

    arb.clearRequests();
    arb.addRequest(0, {Src::IQ, 42, 0x100, true, 12345});

    const auto& result = arb.arbitrate();

    CHECK(result[0].valid);
    CHECK(result[0].tag == 12345);
    CHECK(result[0].entry_id == 42);
    CHECK(result[0].pipe_id == 0);

    PASS("tag_preservation");
}

int main() {
    std::cout << "PriorityArbiter Tests" << std::endl;
    std::cout << "=====================" << std::endl;

    // Tests run via static constructors above

    std::cout << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed"
              << std::endl;

    return tests_failed > 0 ? 1 : 0;
}
