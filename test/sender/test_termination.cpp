// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_termination.cpp
//
// Unit tests for the unit-initiated simulation termination system.

#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>

#include "sender/core/TerminationRequest.hpp"
#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"

using namespace chronon::sender;

// =============================================================================
// Test Units
// =============================================================================

/**
 * A unit that terminates after reaching a target count.
 */
class CountingUnit : public TickableUnit {
public:
    explicit CountingUnit(std::string name, uint64_t target)
        : TickableUnit(std::move(name)), target_(target) {}

    void tick() override {
        ++count_;
        if (count_ >= target_) {
            requestTermination(TerminationReason::Completed, 0,
                               "Reached target count " + std::to_string(target_));
        }
    }

    bool isCompleted() const override { return count_ >= target_; }
    uint64_t count() const { return count_; }

private:
    uint64_t count_ = 0;
    uint64_t target_;
};

/**
 * A unit that terminates with an exit syscall.
 */
class ExitSyscallUnit : public TickableUnit {
public:
    explicit ExitSyscallUnit(std::string name, uint64_t trigger_cycle, int exit_code)
        : TickableUnit(std::move(name)), trigger_cycle_(trigger_cycle), exit_code_(exit_code) {}

    void tick() override {
        if (localCycle() >= trigger_cycle_ && !triggered_) {
            triggered_ = true;
            requestExitSyscall(exit_code_);
        }
    }

private:
    uint64_t trigger_cycle_;
    int exit_code_;
    bool triggered_ = false;
};

/**
 * A unit that terminates with an error.
 */
class ErrorUnit : public TickableUnit {
public:
    explicit ErrorUnit(std::string name, uint64_t trigger_cycle)
        : TickableUnit(std::move(name)), trigger_cycle_(trigger_cycle) {}

    void tick() override {
        if (localCycle() >= trigger_cycle_ && !triggered_) {
            triggered_ = true;
            requestError("Simulated error condition");
        }
    }

private:
    uint64_t trigger_cycle_;
    bool triggered_ = false;
};

/**
 * A simple unit that never terminates (for testing with other units).
 */
class InfiniteUnit : public TickableUnit {
public:
    explicit InfiniteUnit(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override { ++count_; }

    uint64_t count() const { return count_; }

private:
    uint64_t count_ = 0;
};

// =============================================================================
// TerminationController Tests
// =============================================================================

void test_termination_controller_basic() {
    std::cout << "Testing TerminationController basic... ";

    TerminationController ctrl;

    // Initially not terminated
    assert(!ctrl.isTerminationRequested());

    // Request termination
    [[maybe_unused]] bool was_first = ctrl.requestTermination(TerminationReason::Completed, 42,
                                                              1000, "test_unit", "Test message");

    assert(was_first);
    assert(ctrl.isTerminationRequested());

    // Check request details
    [[maybe_unused]] const auto& req = ctrl.getRequest();
    assert(req.reason == TerminationReason::Completed);
    assert(req.exit_code == 42);
    assert(req.cycle == 1000);
    assert(req.unit_name == "test_unit");
    assert(req.message == "Test message");
    assert(req.reasonString() == "Completed");

    std::cout << "PASSED\n";
}

void test_termination_controller_first_wins() {
    std::cout << "Testing TerminationController first-wins... ";

    TerminationController ctrl;

    // First request wins
    [[maybe_unused]] bool first =
        ctrl.requestTermination(TerminationReason::Completed, 0, 100, "unit1", "First");
    assert(first);

    // Second request is ignored
    [[maybe_unused]] bool second =
        ctrl.requestTermination(TerminationReason::Error, 1, 200, "unit2", "Second");
    assert(!second);

    // Request should still be from first unit
    [[maybe_unused]] const auto& req = ctrl.getRequest();
    assert(req.reason == TerminationReason::Completed);
    assert(req.unit_name == "unit1");
    assert(req.message == "First");

    std::cout << "PASSED\n";
}

void test_termination_controller_reset() {
    std::cout << "Testing TerminationController reset... ";

    TerminationController ctrl;

    // Request and verify
    ctrl.requestTermination(TerminationReason::Completed, 0, 100, "unit", "msg");
    assert(ctrl.isTerminationRequested());

    // Reset and verify cleared
    ctrl.reset();
    assert(!ctrl.isTerminationRequested());

    // Should be able to request again
    [[maybe_unused]] bool was_first =
        ctrl.requestTermination(TerminationReason::Error, 1, 200, "unit2", "new msg");
    assert(was_first);
    assert(ctrl.isTerminationRequested());
    assert(ctrl.getRequest().reason == TerminationReason::Error);

    std::cout << "PASSED\n";
}

void test_termination_controller_concurrent() {
    std::cout << "Testing TerminationController concurrent access... ";

    TerminationController ctrl;
    std::atomic<int> winners{0};
    constexpr int NUM_THREADS = 8;

    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);

    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&ctrl, &winners, i]() {
            bool won = ctrl.requestTermination(
                TerminationReason::Completed, i, static_cast<uint64_t>(i * 100),
                "thread_" + std::to_string(i), "Thread " + std::to_string(i));
            if (won) {
                winners.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Exactly one thread should have won
    assert(winners.load() == 1);
    assert(ctrl.isTerminationRequested());

    std::cout << "PASSED\n";
}

void test_termination_reason_strings() {
    std::cout << "Testing TerminationReason strings... ";

    TerminationRequest req;

    req.reason = TerminationReason::None;
    assert(req.reasonString() == "None");

    req.reason = TerminationReason::Completed;
    assert(req.reasonString() == "Completed");

    req.reason = TerminationReason::ExitSyscall;
    assert(req.reasonString() == "ExitSyscall");

    req.reason = TerminationReason::Error;
    assert(req.reasonString() == "Error");

    req.reason = TerminationReason::UserInterrupted;
    assert(req.reasonString() == "UserInterrupted");

    req.reason = TerminationReason::MaxCyclesReached;
    assert(req.reasonString() == "MaxCyclesReached");

    req.reason = TerminationReason::CheckpointRequested;
    assert(req.reasonString() == "CheckpointRequested");

    std::cout << "PASSED\n";
}

// =============================================================================
// TickSimulation Termination Tests
// =============================================================================

void test_simulation_single_unit_termination() {
    std::cout << "Testing single unit termination... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    // Use epoch_size=1 for precise termination (checked after each cycle)
    config.epoch_size = 1;

    TickSimulation sim(config);
    [[maybe_unused]] auto* counter = sim.createUnit<CountingUnit>("counter", 100);

    [[maybe_unused]] uint64_t executed = sim.runUntilTermination(1000);

    // Should have stopped at 100
    assert(counter->count() == 100);
    assert(executed == 100);
    assert(sim.wasTerminationRequested());

    [[maybe_unused]] const auto& req = sim.terminationRequest();
    assert(req.reason == TerminationReason::Completed);
    assert(req.cycle == 99);  // 0-indexed, terminated on tick 99
    assert(req.unit_name == "counter");

    std::cout << "PASSED\n";
}

void test_simulation_exit_syscall() {
    std::cout << "Testing exit syscall termination... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    config.epoch_size = 16;

    TickSimulation sim(config);
    sim.createUnit<ExitSyscallUnit>("exit_unit", 50, 42);

    sim.runUntilTermination(1000);

    assert(sim.wasTerminationRequested());
    [[maybe_unused]] const auto& req = sim.terminationRequest();
    assert(req.reason == TerminationReason::ExitSyscall);
    assert(req.exit_code == 42);
    assert(req.message == "Exit syscall");

    std::cout << "PASSED\n";
}

void test_simulation_error_termination() {
    std::cout << "Testing error termination... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    config.epoch_size = 16;

    TickSimulation sim(config);
    sim.createUnit<ErrorUnit>("error_unit", 75);

    sim.runUntilTermination(1000);

    assert(sim.wasTerminationRequested());
    [[maybe_unused]] const auto& req = sim.terminationRequest();
    assert(req.reason == TerminationReason::Error);
    assert(req.exit_code == 1);
    assert(req.message == "Simulated error condition");

    std::cout << "PASSED\n";
}

void test_simulation_max_cycles_termination() {
    std::cout << "Testing max cycles termination... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    config.epoch_size = 16;

    TickSimulation sim(config);
    sim.createUnit<InfiniteUnit>("infinite");

    [[maybe_unused]] uint64_t executed = sim.runUntilTermination(100);

    assert(executed == 100);
    assert(sim.wasTerminationRequested());
    [[maybe_unused]] const auto& req = sim.terminationRequest();
    assert(req.reason == TerminationReason::MaxCyclesReached);

    std::cout << "PASSED\n";
}

void test_simulation_external_termination() {
    std::cout << "Testing external termination request... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    config.epoch_size = 16;

    TickSimulation sim(config);
    sim.createUnit<InfiniteUnit>("infinite");
    sim.initialize();

    // Request termination externally
    sim.requestTermination(TerminationReason::UserInterrupted, 0, "User interrupt");

    // Should stop immediately
    [[maybe_unused]] uint64_t executed = sim.runUntilTermination(1000);
    assert(executed == 0);
    assert(sim.wasTerminationRequested());
    assert(sim.terminationRequest().reason == TerminationReason::UserInterrupted);

    std::cout << "PASSED\n";
}

void test_simulation_reset_termination() {
    std::cout << "Testing termination reset and rerun... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    // Use epoch_size=1 for precise termination (checked after each cycle)
    config.epoch_size = 1;

    TickSimulation sim(config);
    [[maybe_unused]] auto* counter = sim.createUnit<CountingUnit>("counter", 50);

    // First run
    sim.runUntilTermination(1000);
    assert(counter->count() == 50);
    assert(sim.wasTerminationRequested());
    assert(sim.terminationRequest().reason == TerminationReason::Completed);

    // Reset and rerun (note: counter keeps its state, so it will terminate immediately)
    sim.resetTermination();
    assert(!sim.wasTerminationRequested());

    // Since counter already completed, it will terminate immediately again
    sim.runUntilTermination(100);
    assert(sim.wasTerminationRequested());

    std::cout << "PASSED\n";
}

void test_simulation_multiple_units_first_wins() {
    std::cout << "Testing multiple units - first wins... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    config.epoch_size = 16;

    TickSimulation sim(config);

    // counter1 terminates at 30, counter2 at 50
    [[maybe_unused]] auto* counter1 = sim.createUnit<CountingUnit>("counter1", 30);
    [[maybe_unused]] auto* counter2 = sim.createUnit<CountingUnit>("counter2", 50);

    sim.runUntilTermination(1000);

    // counter1 should have terminated first
    assert(sim.wasTerminationRequested());
    [[maybe_unused]] const auto& req = sim.terminationRequest();
    assert(req.unit_name == "counter1");
    assert(counter1->count() >= 30);
    // counter2 may have run a bit more (within the epoch)
    assert(counter2->count() <= 30 + config.epoch_size);

    std::cout << "PASSED\n";
}

void test_simulation_parallel_termination() {
    std::cout << "Testing parallel execution with termination... ";

    TickSimulationConfig config;
    config.enable_parallel = true;
    config.num_threads = 4;
    // Use epoch_size=1 for precise termination (checked after each cycle)
    config.epoch_size = 1;

    TickSimulation sim(config);
    [[maybe_unused]] auto* counter = sim.createUnit<CountingUnit>("counter", 200);

    [[maybe_unused]] uint64_t executed = sim.runUntilTermination(10000);

    assert(counter->count() == 200);
    assert(executed == 200);
    assert(sim.wasTerminationRequested());
    assert(sim.terminationRequest().reason == TerminationReason::Completed);

    std::cout << "PASSED\n";
}

// =============================================================================
// Stop Token Integration Tests
// =============================================================================

void test_stop_token_reflects_termination() {
    std::cout << "Testing stop_token reflects termination... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    config.epoch_size = 1;

    TickSimulation sim(config);
    sim.createUnit<CountingUnit>("counter", 10);
    sim.initialize();

    // Before termination: stop not requested
    auto token = sim.get_stop_token();
    assert(!token.stop_requested());
    (void)token;

    sim.runUntilTermination(1000);

    // After termination: stop is requested
    assert(sim.wasTerminationRequested());
    assert(token.stop_requested());

    std::cout << "PASSED\n";
}

void test_stop_token_survives_reset() {
    std::cout << "Testing stop_token reset with new stop_source... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    config.epoch_size = 1;

    TickSimulation sim(config);
    sim.createUnit<CountingUnit>("counter", 10);

    // First run
    sim.runUntilTermination(1000);
    auto old_token = sim.get_stop_token();
    assert(old_token.stop_requested());
    (void)old_token;

    // Reset reconstructs stop_source
    sim.resetTermination();

    // New token from new stop_source should NOT be stopped
    auto new_token = sim.get_stop_token();
    assert(!new_token.stop_requested());
    (void)new_token;

    // Old token still reflects the old (destroyed) stop_source state
    // (stop_possible() becomes false since source is gone)

    std::cout << "PASSED\n";
}

void test_stop_token_external_request() {
    std::cout << "Testing stop_token with external termination... ";

    TickSimulationConfig config;
    config.enable_parallel = false;

    TickSimulation sim(config);
    sim.createUnit<InfiniteUnit>("infinite");
    sim.initialize();

    auto token = sim.get_stop_token();
    assert(!token.stop_requested());
    (void)token;

    // External termination triggers stop_source
    sim.requestTermination(TerminationReason::UserInterrupted, 0, "test");

    assert(token.stop_requested());
    assert(sim.wasTerminationRequested());

    std::cout << "PASSED\n";
}

void test_stop_token_parallel_termination() {
    std::cout << "Testing stop_token with parallel execution... ";

    TickSimulationConfig config;
    config.enable_parallel = true;
    config.num_threads = 4;
    config.epoch_size = 64;

    TickSimulation sim(config);
    // Use larger target to ensure we're actually running in parallel
    [[maybe_unused]] auto* counter = sim.createUnit<CountingUnit>("counter", 500);
    sim.createUnit<InfiniteUnit>("infinite1");
    sim.createUnit<InfiniteUnit>("infinite2");
    sim.createUnit<InfiniteUnit>("infinite3");

    sim.runUntilTermination(100000);

    assert(sim.wasTerminationRequested());
    assert(sim.terminationRequest().reason == TerminationReason::Completed);
    assert(sim.get_stop_token().stop_requested());

    std::cout << "PASSED\n";
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "=== Termination Tests ===\n\n";

    // TerminationController tests
    test_termination_controller_basic();
    test_termination_controller_first_wins();
    test_termination_controller_reset();
    test_termination_controller_concurrent();
    test_termination_reason_strings();

    std::cout << "\n";

    // TickSimulation termination tests
    test_simulation_single_unit_termination();
    test_simulation_exit_syscall();
    test_simulation_error_termination();
    test_simulation_max_cycles_termination();
    test_simulation_external_termination();
    test_simulation_reset_termination();
    test_simulation_multiple_units_first_wins();
    test_simulation_parallel_termination();

    std::cout << "\n";

    // Stop token integration tests
    test_stop_token_reflects_termination();
    test_stop_token_survives_reset();
    test_stop_token_external_request();
    test_stop_token_parallel_termination();

    std::cout << "\n=== All Termination Tests PASSED ===\n";
    return 0;
}
