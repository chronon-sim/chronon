// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <iostream>
#include <stdexcept>
#include <string>

#include "chronon/Chronon.hpp"

using namespace chronon::sender;

namespace {

#define CHECK(cond)                                                                        \
    do {                                                                                   \
        if (!(cond)) {                                                                     \
            std::cerr << "  FAIL: " << #cond << " (" << __FILE__ << ":" << __LINE__ << ")" \
                      << std::endl;                                                        \
            std::exit(1);                                                                  \
        }                                                                                  \
    } while (0)

class PassUnit : public TickableUnit {
public:
    explicit PassUnit(std::string name) : TickableUnit(std::move(name)) {}

    InPort<int> in{this, "in"};
    OutPort<int> out{this, "out"};

    void tick() override {
        if (auto value = in.tryReceive(localCycle())) {
            [[maybe_unused]] bool sent = out.send(*value);
        }
    }
};

bool initializeThrowsZeroDelayCycle(TickSimulation& sim, const std::string& expected_fragment) {
    try {
        sim.initialize();
    } catch (const std::invalid_argument& e) {
        const std::string message = e.what();
        return message.find("zero-delay cycle") != std::string::npos &&
               message.find(expected_fragment) != std::string::npos;
    }
    return false;
}

void test_two_unit_zero_delay_cycle_rejected() {
    std::cout << "Testing two-unit zero-delay cycle rejection... ";

    TickSimulation sim;
    auto* a = sim.createUnit<PassUnit>("a");
    auto* b = sim.createUnit<PassUnit>("b");

    sim.connect(a->out, b->in, 0);
    sim.connect(b->out, a->in, 0);

    CHECK(initializeThrowsZeroDelayCycle(sim, "a -> b"));

    std::cout << "PASSED\n";
}

void test_zero_delay_self_loop_rejected() {
    std::cout << "Testing zero-delay self-loop rejection... ";

    TickSimulation sim;
    auto* unit = sim.createUnit<PassUnit>("loop");

    sim.connect(unit->out, unit->in, 0);

    CHECK(initializeThrowsZeroDelayCycle(sim, "loop"));

    std::cout << "PASSED\n";
}

void test_acyclic_zero_delay_path_allowed() {
    std::cout << "Testing acyclic zero-delay path allowed... ";

    TickSimulation sim;
    auto* a = sim.createUnit<PassUnit>("a");
    auto* b = sim.createUnit<PassUnit>("b");
    auto* c = sim.createUnit<PassUnit>("c");

    sim.connect(a->out, b->in, 0);
    sim.connect(b->out, c->in, 0);

    sim.initialize();

    std::cout << "PASSED\n";
}

void test_registered_feedback_allowed() {
    std::cout << "Testing registered feedback allowed... ";

    TickSimulation sim;
    auto* a = sim.createUnit<PassUnit>("a");
    auto* b = sim.createUnit<PassUnit>("b");

    sim.connect(a->out, b->in, 0);
    sim.connect(b->out, a->in, 1);

    sim.initialize();

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    test_two_unit_zero_delay_cycle_rejected();
    test_zero_delay_self_loop_rejected();
    test_acyclic_zero_delay_path_allowed();
    test_registered_feedback_allowed();

    std::cout << "All zero-delay cycle validation tests passed!\n";
    return 0;
}
