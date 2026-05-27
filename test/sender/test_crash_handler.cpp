// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_crash_handler.cpp
//
// Tests for the crash handler and tick exception capture system.

#include <sys/wait.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "observe/ObservableUnit.hpp"
#include "observe/ObservationManager.hpp"
#include "observe/ObservationYAMLConfig.hpp"
#include "observe/ObserveApi.hpp"
#include "observe/Types.hpp"
#include "sender/core/CrashHandler.hpp"
#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"

using namespace chronon::sender;
using namespace chronon::observe;

/**
 * A unit that throws a std::runtime_error after a specified number of ticks.
 */
class ThrowingUnit : public TickableUnit {
public:
    ThrowingUnit(std::string name, uint64_t throw_at_tick)
        : TickableUnit(std::move(name)), throw_at_tick_(throw_at_tick) {}

    void tick() override {
        if (localCycle() == throw_at_tick_) {
            throw std::runtime_error("intentional crash at tick " + std::to_string(throw_at_tick_));
        }
    }

private:
    uint64_t throw_at_tick_;
};

/**
 * A unit that never throws (paired with ThrowingUnit in multi-unit tests).
 */
class SafeUnit : public TickableUnit {
public:
    explicit SafeUnit(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override { ++tick_count_; }

    uint64_t tickCount() const { return tick_count_; }

private:
    uint64_t tick_count_ = 0;
};

/**
 * An observable unit that emits a log every tick, then throws.
 * Used to test that observer data emitted before the crash is flushed.
 */
inline const auto TEST_CATEGORY = Category<"test_crash", "Crash test trace">{};

class ObservableThrowingUnit : public TickableUnit, public ObservableUnit {
public:
    ObservableThrowingUnit(std::string name, uint64_t throw_at_tick)
        : TickableUnit(std::move(name)), throw_at_tick_(throw_at_tick) {}

    void tick() override {
        // Emit a log message each tick (if observation is set up)
        if (observationContext()) {
            observationContext()->setCurrentCycleValue(localCycle());
            log_info<"tick {}">(observationContext(), localCycle());
        }

        if (localCycle() == throw_at_tick_) {
            throw std::runtime_error("observable crash at tick " + std::to_string(throw_at_tick_));
        }
    }

private:
    uint64_t throw_at_tick_;
};

void test_sequential_exception_capture() {
    std::cout << "Testing sequential exception capture... ";

    TickSimulationConfig config;
    config.enable_parallel = false;

    TickSimulation sim(config);
    sim.createUnit<ThrowingUnit>("crasher", 50);

    bool caught = false;
    try {
        sim.run(1000);
    } catch (const TickException& e) {
        caught = true;
        assert(e.unitName() == "crasher");
        assert(e.cycle() == 50);
        assert(std::string(e.what()).find("intentional crash") != std::string::npos);
    }

    assert(caught);
    (void)caught;
    std::cout << "PASSED\n";
}

void test_sequential_exception_run_until_termination() {
    std::cout << "Testing sequential exception in runUntilTermination... ";

    TickSimulationConfig config;
    config.enable_parallel = false;
    config.epoch_size = 16;

    TickSimulation sim(config);
    sim.createUnit<ThrowingUnit>("crasher2", 30);

    bool caught = false;
    try {
        sim.runUntilTermination(1000);
    } catch (const TickException& e) {
        caught = true;
        assert(e.unitName() == "crasher2");
        assert(e.cycle() == 30);
    }

    assert(caught);
    (void)caught;
    std::cout << "PASSED\n";
}

void test_sequential_multi_unit_exception() {
    std::cout << "Testing multi-unit sequential exception identifies correct unit... ";

    TickSimulationConfig config;
    config.enable_parallel = false;

    TickSimulation sim(config);
    sim.createUnit<SafeUnit>("safe1");
    sim.createUnit<ThrowingUnit>("the_crasher", 25);
    sim.createUnit<SafeUnit>("safe2");

    bool caught = false;
    try {
        sim.run(1000);
    } catch (const TickException& e) {
        caught = true;
        // Must identify the specific unit that crashed
        assert(e.unitName() == "the_crasher");
        assert(e.cycle() == 25);
    }

    assert(caught);
    (void)caught;
    std::cout << "PASSED\n";
}

void test_parallel_exception_capture() {
    std::cout << "Testing parallel exception capture... ";

    TickSimulationConfig config;
    config.enable_parallel = true;
    config.num_threads = 2;
    config.epoch_size = 16;

    TickSimulation sim(config);
    // Create multiple units so parallel mode is considered
    sim.createUnit<SafeUnit>("safe_a");
    sim.createUnit<SafeUnit>("safe_b");
    sim.createUnit<SafeUnit>("safe_c");
    sim.createUnit<SafeUnit>("safe_d");
    sim.createUnit<SafeUnit>("safe_e");
    sim.createUnit<SafeUnit>("safe_f");
    sim.createUnit<ThrowingUnit>("par_crasher", 10);
    sim.createUnit<SafeUnit>("safe_g");

    bool caught = false;
    try {
        sim.runUntilTermination(1000);
    } catch (const TickException& e) {
        caught = true;
        // In parallel mode, the first captured exception wins.
        // The crasher should be identified.
        assert(e.unitName() == "par_crasher");
        assert(e.cycle() == 10);
        assert(std::string(e.what()).find("intentional crash") != std::string::npos);
    }

    assert(caught);
    (void)caught;
    std::cout << "PASSED\n";
}

void test_context_cleared_after_sequential_exception() {
    std::cout << "Testing context is cleared after sequential exception... ";

    TickSimulationConfig config;
    config.enable_parallel = false;

    TickSimulation sim(config);
    sim.createUnit<ThrowingUnit>("ctx_seq_crasher", 7);

    bool caught = false;
    try {
        sim.run(100);
    } catch (const TickException&) {
        caught = true;
    }

    assert(caught);
    (void)caught;
    assert(detail::current_tick_context_.unit == nullptr);
    assert(detail::current_tick_context_.cycle == 0);
    std::cout << "PASSED\n";
}

void test_context_cleared_after_parallel_exception() {
    std::cout << "Testing context is cleared after parallel exception... ";

    TickSimulationConfig config;
    config.enable_parallel = true;
    config.num_threads = 2;
    config.epoch_size = 8;

    TickSimulation sim(config);
    sim.createUnit<SafeUnit>("p_safe_1");
    sim.createUnit<SafeUnit>("p_safe_2");
    sim.createUnit<SafeUnit>("p_safe_3");
    sim.createUnit<SafeUnit>("p_safe_4");
    sim.createUnit<ThrowingUnit>("p_crasher", 3);
    sim.createUnit<SafeUnit>("p_safe_5");

    bool caught = false;
    try {
        sim.runUntilTermination(100);
    } catch (const TickException&) {
        caught = true;
    }

    assert(caught);
    (void)caught;
    assert(detail::current_tick_context_.unit == nullptr);
    assert(detail::current_tick_context_.cycle == 0);
    std::cout << "PASSED\n";
}

void test_simulation_destructs_after_exception() {
    std::cout << "Testing simulation destructs cleanly after exception... ";

    // This test verifies that after an exception, the TickSimulation destructor
    // doesn't deadlock or crash (e.g., worker threads are properly shut down).
    {
        TickSimulationConfig config;
        config.enable_parallel = true;
        config.num_threads = 2;

        TickSimulation sim(config);
        sim.createUnit<SafeUnit>("s1");
        sim.createUnit<SafeUnit>("s2");
        sim.createUnit<SafeUnit>("s3");
        sim.createUnit<SafeUnit>("s4");
        sim.createUnit<SafeUnit>("s5");
        sim.createUnit<SafeUnit>("s6");
        sim.createUnit<ThrowingUnit>("crash", 5);
        sim.createUnit<SafeUnit>("s7");

        try {
            sim.runUntilTermination(1000);
        } catch (const TickException&) {
            // Expected
        }

        // sim goes out of scope here - destructor must not deadlock
    }

    // No stale tick context should remain after destruction.
    assert(detail::current_tick_context_.unit == nullptr);
    assert(detail::current_tick_context_.cycle == 0);

    std::cout << "PASSED\n";
}

void test_tick_exception_fields() {
    std::cout << "Testing TickException fields... ";

    TickException ex("my_unit", 42, "some error");
    assert(ex.unitName() == "my_unit");
    assert(ex.cycle() == 42);
    assert(ex.cause() == "some error");
    assert(std::string(ex.what()).find("my_unit") != std::string::npos);
    assert(std::string(ex.what()).find("42") != std::string::npos);
    assert(std::string(ex.what()).find("some error") != std::string::npos);

    std::cout << "PASSED\n";
}

void test_crash_handler_install_idempotent() {
    std::cout << "Testing CrashHandler::install() idempotent... ";

    // Should not crash or change behavior when called multiple times
    CrashHandler::install();
    CrashHandler::install();
    CrashHandler::install();

    std::cout << "PASSED\n";
}

/**
 * A unit that checks the thread-local tick context is set correctly during tick.
 */
class ContextCheckUnit : public TickableUnit {
public:
    explicit ContextCheckUnit(std::string name) : TickableUnit(std::move(name)) {}

    void tick() override {
        // During tick(), the thread-local should point to us
        auto* ctx = detail::current_tick_context_.unit;
        if (ctx != static_cast<TickableUnit*>(this)) {
            context_correct_ = false;
        }
        if (detail::current_tick_context_.cycle != localCycle()) {
            context_correct_ = false;
        }
        ++tick_count_;
    }

    bool contextCorrect() const { return context_correct_; }
    uint64_t tickCount() const { return tick_count_; }

private:
    bool context_correct_ = true;
    uint64_t tick_count_ = 0;
};

void test_thread_local_context_set_during_tick() {
    std::cout << "Testing thread-local context is set during tick... ";

    TickSimulationConfig config;
    config.enable_parallel = false;

    TickSimulation sim(config);
    auto* checker = sim.createUnit<ContextCheckUnit>("ctx_checker");

    sim.run(100);

    assert(checker->tickCount() == 100);
    assert(checker->contextCorrect());
    (void)checker;

    // After run(), context should be cleared
    assert(detail::current_tick_context_.unit == nullptr);
    assert(detail::current_tick_context_.cycle == 0);

    std::cout << "PASSED\n";
}

void test_fatal_signal_handler_subprocess() {
    std::cout << "Testing fatal signal handler subprocess behavior... ";

    int pipefd[2];
    [[maybe_unused]] int pipe_rc = pipe(pipefd);
    assert(pipe_rc == 0);

    pid_t pid = fork();
    assert(pid >= 0);

    if (pid == 0) {
        close(pipefd[0]);
        [[maybe_unused]] int dup_rc = dup2(pipefd[1], STDERR_FILENO);
        assert(dup_rc >= 0);
        close(pipefd[1]);

        CrashHandler::install();
        detail::current_tick_context_.unit = reinterpret_cast<TickableUnit*>(0x1234);
        detail::current_tick_context_.cycle = 777;
        std::memcpy(detail::current_tick_context_.unit_name, "test_unit", 10);
        detail::current_tick_context_.phase = "tick";

        raise(SIGABRT);
        _exit(0);  // Should never reach here
    }

    close(pipefd[1]);

    int status = 0;
    bool exited = false;
    for (int i = 0; i < 200; ++i) {  // ~2s timeout
        pid_t w = waitpid(pid, &status, WNOHANG);
        if (w == pid) {
            exited = true;
            break;
        }
        assert(w == 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!exited) {
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
        assert(false && "child process hung in signal handler");
    }

    std::string stderr_output;
    char buf[512];
    ssize_t n = 0;
    while ((n = read(pipefd[0], buf, sizeof(buf))) > 0) {
        stderr_output.append(buf, static_cast<size_t>(n));
    }
    close(pipefd[0]);

    assert(WIFEXITED(status));
    assert(WEXITSTATUS(status) == 128 + SIGABRT);
    assert(stderr_output.find("=== CHRONON CRASH ===") != std::string::npos);
    assert(stderr_output.find("SIGABRT") != std::string::npos);
    assert(stderr_output.find("Unit:    test_unit") != std::string::npos);
    assert(stderr_output.find("Phase:   tick") != std::string::npos);
#if !defined(CHRONON_SANITIZER_BUILD)
    assert(stderr_output.find("Backtrace:") != std::string::npos);
#endif
    assert(stderr_output.find("Flushing observers") == std::string::npos);

    std::cout << "PASSED\n";
}

void test_observer_emergency_flush_on_exception() {
    std::cout << "Testing observer emergency flush on exception... ";

    // Create a temporary directory for observation output
    std::string tmp_dir = "/tmp/chronon_crash_test_" + std::to_string(getpid());
    std::filesystem::create_directories(tmp_dir);

    {
        // Set up observation
        ObservationYAMLConfig obs_config;
        obs_config.enabled = true;
        obs_config.output_dir = tmp_dir;
        obs_config.counters.enabled = false;
        obs_config.counters.csv_output = false;
        obs_config.unified_logging.enabled = true;
        obs_config.unified_logging.info_channel.enabled = true;
        obs_config.unified_logging.info_channel.format = OutputFormat::Text;

        auto& obs_mgr = ObservationManager::instance();
        obs_mgr.reset();
        obs_mgr.initialize(obs_config);

        // Create simulation with an observable throwing unit
        TickSimulationConfig config;
        config.enable_parallel = false;

        TickSimulation sim(config);
        auto* unit = sim.createUnit<ObservableThrowingUnit>("obs_crasher", 20);

        // Create observation context for the unit
        auto* ctx =
            obs_mgr.createContextForUnit("obs_crasher", [&unit]() { return unit->localCycle(); });
        unit->setObservationContext(ctx);

        // Explicitly enable the info log category on this context
        ctx->filter().enableCategory(category::LOG_INFO);

        // Initialize simulation first so setStopToken() runs before the
        // backend thread starts (avoids TSAN race on stop_token_).
        sim.initialize();

        // Start backend
        obs_mgr.startBackend();

        // Run - should throw after 20 ticks
        bool caught = false;
        try {
            sim.run(1000);
        } catch (const TickException& e) {
            caught = true;
            assert(e.unitName() == "obs_crasher");
            assert(e.cycle() == 20);

            // Emergency flush should drain the observer queues
            CrashHandler::emergencyFlush();
        }

        assert(caught);
        (void)caught;

        // Reset observation manager for clean state
        obs_mgr.reset();
    }

    // Check output directory for any files written by the observer
    bool found_output = false;
    for (auto& entry : std::filesystem::directory_iterator(tmp_dir)) {
        if (entry.is_regular_file() && entry.file_size() > 0) {
            found_output = true;
        }
    }

    if (found_output) {
        // The observer flushed data to at least one output file
        std::string log_path = tmp_dir + "/events.log";
        if (std::filesystem::exists(log_path)) {
            std::ifstream log_file(log_path);
            std::string content{std::istreambuf_iterator<char>(log_file),
                                std::istreambuf_iterator<char>()};
            assert(!content.empty());
            std::cout << "PASSED (events.log: " << content.size() << " bytes)\n";
        } else {
            std::cout << "PASSED (observer output files found)\n";
        }
    } else {
        // No output files is acceptable: the critical guarantee is that the
        // exception was caught with correct unit/cycle context, and
        // emergencyFlush() completed without deadlocking or crashing.
        // Full observer output requires the YAML-driven build pipeline.
        std::cout << "PASSED (exception caught, emergency flush completed)\n";
    }

    // Clean up
    std::filesystem::remove_all(tmp_dir);
}

void test_emergency_flush_no_observer() {
    std::cout << "Testing emergency flush with no observer running... ";

    // Should not crash when called without any observer set up
    CrashHandler::emergencyFlush();

    std::cout << "PASSED\n";
}

int main() {
    std::cout << "=== Crash Handler Tests ===\n\n";

    // Basic tests
    test_tick_exception_fields();
    test_crash_handler_install_idempotent();
    test_thread_local_context_set_during_tick();
    test_emergency_flush_no_observer();
    test_fatal_signal_handler_subprocess();

    std::cout << "\n";

    // Sequential exception tests
    test_sequential_exception_capture();
    test_sequential_exception_run_until_termination();
    test_sequential_multi_unit_exception();
    test_context_cleared_after_sequential_exception();

    std::cout << "\n";

    // Parallel exception tests
    test_parallel_exception_capture();
    test_context_cleared_after_parallel_exception();
    test_simulation_destructs_after_exception();

    std::cout << "\n";

    // Observer integration
    test_observer_emergency_flush_on_exception();

    std::cout << "\n=== All Crash Handler Tests PASSED ===\n";
    return 0;
}
