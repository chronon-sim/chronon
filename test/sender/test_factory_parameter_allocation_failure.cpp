// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <cstdlib>
#include <iostream>
#include <new>

#include "../TestAssertions.hpp"
#include "chronon/Chronon.hpp"

namespace {
// Arm only after the unit constructor has finished allocating. The next two
// allocations grow the simulation's owning-unit and borrowed-pointer vectors.
thread_local int allocations_before_failure = -1;
}  // namespace

[[gnu::noinline]] void* operator new(std::size_t size) {
    if (allocations_before_failure >= 0 && allocations_before_failure-- == 0) {
        throw std::bad_alloc();
    }
    if (auto* pointer = std::malloc(size == 0 ? 1 : size)) return pointer;
    throw std::bad_alloc();
}

[[gnu::noinline]] void operator delete(void* pointer) noexcept { std::free(pointer); }
[[gnu::noinline]] void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }

using namespace chronon;

namespace {
struct FailureParams : ParameterSet {
    inline static size_t live = 0;
    const int value = 42;
    FailureParams() { ++live; }
    ~FailureParams() override { --live; }
};

class FailureUnit final : public TickableUnit {
public:
    using ParameterSet = FailureParams;
    static constexpr const char* unit_type_name = "FactoryAllocationFailureTest";
    static constexpr const char* unit_description = "Unit registration failure regression";
    inline static int failure_after_constructor = -1;
    inline static size_t live = 0;

    explicit FailureUnit(const ParameterSet* params) : TickableUnit("failure"), params_(params) {
        ++live;
        allocations_before_failure = failure_after_constructor;
    }
    ~FailureUnit() override {
        CHECK(FailureParams::live > 0);
        CHECK(params_->value == 42);
        --live;
    }
    void tick() override { CHECK(params_->value == 42); }

private:
    const ParameterSet* params_;
};
}  // namespace

int main() {
    chronon::sender::factory::SenderFactory<FailureUnit> factory(FailureUnit::unit_type_name,
                                                                 FailureUnit::unit_description);
    TickSimulationConfig config;
    config.num_threads = 1;
    config.enable_parallel = false;

    for (int allocation = 0; allocation < 2; ++allocation) {
        {
            TickSimulation simulation(config);
            FailureUnit::failure_after_constructor = allocation;
            bool threw = false;
            try {
                factory.createUnit(&simulation, "failure", YAML::Node{});
            } catch (const std::bad_alloc&) {
                threw = true;
            }
            allocations_before_failure = -1;
            FailureUnit::failure_after_constructor = -1;
            CHECK(threw);
            CHECK(FailureUnit::live == 0);
            CHECK(FailureParams::live == 0);
            CHECK(simulation.unitCount() == 0);

            auto* next = factory.createUnit(&simulation, "next", YAML::Node{});
            CHECK(next->id() == 0);
            CHECK(simulation.unitCount() == 1);
            simulation.initialize();
            CHECK(simulation.run(2) == 2);
        }
        CHECK(FailureUnit::live == 0);
        CHECK(FailureParams::live == 0);
    }
    std::cout << "PASSED allocation failures: live_params=0 live_units=0\n";
}
