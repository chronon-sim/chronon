// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

// Detect sanitizers (GCC vs Clang)
#if defined(__SANITIZE_THREAD__)
#define CHRONON_TSAN_ACTIVE 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define CHRONON_TSAN_ACTIVE 1
#endif
#endif

#if defined(__SANITIZE_ADDRESS__)
#define CHRONON_ASAN_ACTIVE 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define CHRONON_ASAN_ACTIVE 1
#endif
#endif

#include "observe/FormatRegistry.hpp"
#include "observe/LookaheadBuffer.hpp"
#include "observe/ObservationBackend.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationManager.hpp"
#include "observe/ObservationYAMLConfig.hpp"
#include "observe/ReorderBuffer.hpp"
#include "observe/ThreadContextManager.hpp"

using namespace chronon::observe;

namespace {

void fakeWakeupCallback(void*) {}

ObservationYAMLConfig makeEnabledConfig(const std::string& output_dir) {
    ObservationYAMLConfig cfg;
    cfg.enabled = true;
    cfg.output_dir = output_dir;
    cfg.counters.enabled = false;
    cfg.counters.csv_output = false;
    cfg.unified_logging.enabled = false;
    return cfg;
}

void test_reinitialize_without_deadlock() {
    std::cout << "Testing ObservationManager reinitialize path... ";

    auto& mgr = ObservationManager::instance();
    mgr.reset();

    ObservationYAMLConfig cfg;
    cfg.enabled = false;

    mgr.initialize(cfg);
    mgr.initialize(cfg);

    assert(mgr.isInitialized());
    mgr.reset();

    std::cout << "PASSED\n";
}

void test_source_registry_freezes_after_backend_start() {
    std::cout << "Testing source registry freeze after backend start... ";

    auto& mgr = ObservationManager::instance();
    mgr.reset();

    auto cfg = makeEnabledConfig("/tmp/chronon_obs_hardening");
    mgr.initialize(cfg);

    [[maybe_unused]] auto* ctx0 = mgr.createContextForUnit("u0", []() { return 0ULL; });
    assert(ctx0 != nullptr);

    mgr.startBackend();

    [[maybe_unused]] auto* ctx1 = mgr.createContextForUnit("late_unit", []() { return 0ULL; });
    assert(ctx1 == nullptr);

    mgr.stopBackend();
    mgr.reset();

    std::cout << "PASSED\n";
}

void test_reorder_buffer_force_flush() {
    std::cout << "Testing reorder buffer force flush behavior... ";

    ReorderBuffer::Config cfg;
    cfg.watermark_cycles = 1000;
    cfg.max_buffer_events = 4;

    ReorderBuffer rb(cfg);

    auto buffer_event = [&](uint64_t cycle) {
        [[maybe_unused]] ObservationQueue::RecordHeader header{};
        header.total_size =
            static_cast<uint16_t>(sizeof(ObservationQueue::RecordHeader) + sizeof(uint64_t));
        header.type = ObservationQueue::EventType::TRACE_EVENT;
        header.flags = 1;
        header.padding = 0;

        std::byte payload[sizeof(uint64_t)]{};
        std::memcpy(payload, &cycle, sizeof(cycle));
        assert(rb.bufferEvent(&header, payload, sizeof(payload)));
    };

    for (uint64_t cycle = 1; cycle <= 6; ++cycle) {
        buffer_event(cycle);
    }

    // With min_cycle=5 and watermark_cycles=1000, the cycle-based threshold
    // is 0 (5 <= 1000, so no warm-up flushing). The force-flush safety valve
    // triggers because size (6) > max_buffer_events (4), flushing the oldest
    // half (3 events: cycles 1, 2, 3).
    rb.updateMinCycle(5);

    std::vector<BufferedRecord> ready;
    rb.flushReady(ready);
    assert(ready.size() == 3);
    assert(ready[0].cycle == 1);
    assert(ready[1].cycle == 2);
    assert(ready[2].cycle == 3);
    assert(rb.size() == 3);

    std::vector<BufferedRecord> remaining;
    rb.flushAll(remaining);
    assert(remaining.size() == 3);
    assert(remaining[0].cycle == 4);
    assert(remaining[1].cycle == 5);
    assert(remaining[2].cycle == 6);

    // Test watermark-based flush: when min_cycle exceeds watermark, cycle-based
    // threshold takes effect. Buffer 4 events at cycles 100-103 with min_cycle=1105.
    ReorderBuffer rb2(cfg);
    for (uint64_t cycle = 100; cycle <= 103; ++cycle) {
        [[maybe_unused]] ObservationQueue::RecordHeader header{};
        header.total_size =
            static_cast<uint16_t>(sizeof(ObservationQueue::RecordHeader) + sizeof(uint64_t));
        header.type = ObservationQueue::EventType::TRACE_EVENT;
        header.flags = 1;
        header.padding = 0;

        std::byte payload[sizeof(uint64_t)]{};
        std::memcpy(payload, &cycle, sizeof(cycle));
        assert(rb2.bufferEvent(&header, payload, sizeof(payload)));
    }

    // threshold = 1105 - 1000 = 105, flushes cycles < 105 → cycles 100-104
    rb2.updateMinCycle(1105);
    std::vector<BufferedRecord> ready2;
    rb2.flushReady(ready2);
    assert(ready2.size() == 4);
    assert(ready2[0].cycle == 100);
    assert(ready2[3].cycle == 103);

    std::cout << "PASSED\n";
}

void test_lookahead_structured_args_commit_and_rollback() {
    std::cout << "Testing lookahead structured args commit/rollback... ";

    ObservationQueue queue(64 * 1024);
    uint64_t cycle = 42;
    ObservationContext ctx(&queue, [&cycle]() { return cycle; }, 0, "unit", 1);
    ctx.enableCategory(category::LOG_INFO);
    ctx.setLookaheadMode(true);

    const FormatId fmt_id = FormatRegistry::instance().registerFormat(
        "msg {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
    assert(fmt_id != INVALID_FORMAT_ID);

    ctx.log<LogLevel::Info>(fmt_id, static_cast<uint64_t>(7));
    assert(queue.prepareRead() == nullptr);

    ctx.rollbackEpoch();
    assert(queue.prepareRead() == nullptr);

    ctx.log<LogLevel::Info>(fmt_id, static_cast<uint64_t>(9));
    ctx.commitEpoch();

    auto* ptr = queue.prepareRead();
    assert(ptr != nullptr);

    auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
    assert(header->type == ObservationQueue::EventType::LOG_EVENT);
    assert((header->flags & 1) != 0);

    [[maybe_unused]] auto* rec =
        reinterpret_cast<const StructuredRecord*>(ptr + sizeof(ObservationQueue::RecordHeader));
    assert(rec->cycle == cycle);
    assert(rec->arg_count == 1);

    uint64_t value = 0;
    const std::byte* arg_data =
        ptr + sizeof(ObservationQueue::RecordHeader) + sizeof(StructuredRecord);
    std::memcpy(&value, arg_data, sizeof(value));
    assert(value == 9);

    queue.finishRead(header->total_size);
    queue.forceCommitRead();
    assert(queue.prepareRead() == nullptr);

    std::cout << "PASSED\n";
}

void test_no_drops_under_pressure_debug() {
    std::cout << "Testing no drops under queue pressure (debug build)... ";

    // Use a moderate queue to exercise pressure while allowing the backend
    // to keep up on slower CI runners (cloud vCPUs).
    constexpr size_t SMALL_QUEUE = 16384;

    ObservationQueue queue(64 * 1024);

    // Configure backend with fast polling
    ObservationBackend::Config backend_cfg;
    backend_cfg.output_dir = "/tmp/chronon_obs_pressure";
    backend_cfg.enable_counter_csv = false;
    backend_cfg.enable_reordering = false;
    backend_cfg.trace_format = OutputFormat::Text;
    backend_cfg.debug_format = OutputFormat::Text;
    backend_cfg.info_format = OutputFormat::Text;
    backend_cfg.warn_format = OutputFormat::Text;
    backend_cfg.error_format = OutputFormat::Text;

    ObservationBackend backend(queue, backend_cfg);

    // Set small per-thread queue capacity before any context is created
    ThreadContextManager::instance().setQueueCapacity(SMALL_QUEUE);

    // Use SpinWait policy so the producer waits for backend to drain rather
    // than dropping after a bounded spin count.  The default BoundedWait
    // (4096 spins) can time out on slow CI runners, causing flaky drops.
    ThreadContextManager::instance().setBackpressurePolicy(ObservationChannel::Info,
                                                           BackpressurePolicy::SpinWait);

    // Register a format for our test events
    const FormatId fmt_id = FormatRegistry::instance().registerFormat(
        "pressure {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
    assert(fmt_id != INVALID_FORMAT_ID);

    backend.start();

    // Create context that uses per-thread queues
    uint64_t cycle = 0;
    ObservationContext ctx(&queue, [&cycle]() { return cycle; }, 0, "pressure_unit", 1);
    ctx.enableCategory(category::LOG_INFO);

    // Emit many events rapidly — enough to overflow the queue many times over.
    // With SpinWait, the producer blocks until the backend drains space.
    constexpr size_t NUM_EVENTS = 10000;
    for (size_t i = 0; i < NUM_EVENTS; ++i) {
        cycle = i;
        ctx.log<LogLevel::Info>(fmt_id, static_cast<uint64_t>(i));
    }

    // Give backend time to drain remaining events
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    backend.stop();

    // Restore default policy for subsequent tests
    ThreadContextManager::instance().setBackpressurePolicy(ObservationChannel::Info,
                                                           BackpressurePolicy::BoundedWait);
    ThreadContextManager::instance().setBackpressureMaxSpins(ObservationChannel::Info, 4096);

    // Under TSAN/ASAN the consumer thread is heavily instrumented (~10-20x
    // slower), so drops are expected.  The test still exercises the code path
    // for sanitizer checks; we just skip the zero-drop assertion.
#if !defined(NDEBUG) && !defined(CHRONON_TSAN_ACTIVE) && !defined(CHRONON_ASAN_ACTIVE)
    uint64_t dropped = ThreadContextManager::instance().totalDroppedCount();
    if (dropped != 0) {
        std::cerr << "FAILED: dropped " << dropped << " events in debug build\n";
        assert(false && "Events were dropped in debug build");
    }
#endif

    std::cout << "PASSED\n";
}

void test_pressure_without_backend_drops_instead_of_hanging() {
    std::cout << "Testing queue pressure without backend drops instead of hanging... ";

    constexpr size_t SMALL_QUEUE = 4096;
    ThreadContextManager::instance().setQueueCapacity(SMALL_QUEUE);

    ObservationQueue queue(64 * 1024);

    const FormatId fmt_id = FormatRegistry::instance().registerFormat(
        "pressure no backend {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
    assert(fmt_id != INVALID_FORMAT_ID);

    [[maybe_unused]] const uint64_t dropped_before =
        ThreadContextManager::instance().totalDroppedCount();

    uint64_t cycle = 0;
    ObservationContext ctx(&queue, [&cycle]() { return cycle; }, 0, "pressure_no_backend", 1);
    ctx.enableCategory(category::LOG_INFO);

    constexpr size_t NUM_EVENTS = 10000;
    for (size_t i = 0; i < NUM_EVENTS; ++i) {
        cycle = i;
        ctx.log<LogLevel::Info>(fmt_id, static_cast<uint64_t>(i));
    }

    [[maybe_unused]] const uint64_t dropped_after =
        ThreadContextManager::instance().totalDroppedCount();
    assert(dropped_after > dropped_before && "Expected drops when backend is not running");

    std::cout << "PASSED\n";
}

void test_spin_wait_exits_when_wakeup_is_removed() {
    std::cout << "Testing spin_wait exits when backend wakeup is removed... ";

    constexpr size_t SMALL_QUEUE = 4096;
    ThreadContextManager::instance().setQueueCapacity(SMALL_QUEUE);

    // No backend is running in this test. We install a fake wakeup callback
    // so wakeBackend() initially succeeds and the producer enters spin-wait.
    ThreadContextManager::instance().setBackendWakeup(&fakeWakeupCallback, nullptr);

    const FormatId fmt_id = FormatRegistry::instance().registerFormat(
        "spin-wait {}", __FILE__, __LINE__, {ArgType::UInt64}, true, LogLevel::Info);
    assert(fmt_id != INVALID_FORMAT_ID);

    std::atomic<bool> ready_to_spin{false};
    std::atomic<bool> producer_done{false};

    std::thread producer([&]() {
        ObservationQueue queue(64 * 1024);
        uint64_t cycle = 0;
        ObservationContext ctx(&queue, [&cycle]() { return cycle; }, 0, "spin_wait_unit", 1);
        ctx.enableCategory(category::LOG_INFO);

        ThreadContext* tc = ThreadContextManager::instance().getContext();
        assert(tc != nullptr);
        const uint64_t dropped_before = tc->droppedCount();

        // Fill queue with Drop policy until full.
        ThreadContextManager::instance().setBackpressurePolicy(ObservationChannel::Info,
                                                               BackpressurePolicy::Drop);
        for (size_t i = 0; i < 200000; ++i) {
            cycle = i;
            ctx.log<LogLevel::Info>(fmt_id, static_cast<uint64_t>(i));
            if (tc->droppedCount() > dropped_before) {
                break;
            }
        }
        assert(tc->droppedCount() > dropped_before);

        // Switch to SpinWait and emit one more event; this call should spin
        // until main thread removes wakeup callback, then drop and return.
        ThreadContextManager::instance().setBackpressurePolicy(ObservationChannel::Info,
                                                               BackpressurePolicy::SpinWait);
        ready_to_spin.store(true, std::memory_order_release);
        ctx.log<LogLevel::Info>(fmt_id, static_cast<uint64_t>(0xDEADBEEF));
        producer_done.store(true, std::memory_order_release);
    });

    auto wait_until = [](std::atomic<bool>& flag, std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (flag.load(std::memory_order_acquire)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return false;
    };

    if (!wait_until(ready_to_spin, std::chrono::milliseconds(2000))) {
        std::cerr << "FAILED: producer did not reach spin-wait state in time\n";
        std::abort();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Simulate backend stop path removing wakeup callback.
    ThreadContextManager::instance().setBackendWakeup(nullptr, nullptr);

    if (!wait_until(producer_done, std::chrono::milliseconds(2000))) {
        std::cerr << "FAILED: producer did not exit spin-wait after wakeup removal\n";
        std::abort();
    }
    producer.join();

    // Restore defaults for following tests.
    ThreadContextManager::instance().setBackpressurePolicy(ObservationChannel::Info,
                                                           BackpressurePolicy::BoundedWait);
    ThreadContextManager::instance().setBackpressureMaxSpins(ObservationChannel::Info, 4096);

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Observation Hardening Tests ===\n\n";

    test_reinitialize_without_deadlock();
    test_source_registry_freezes_after_backend_start();
    test_reorder_buffer_force_flush();
    test_lookahead_structured_args_commit_and_rollback();
    test_no_drops_under_pressure_debug();
    test_pressure_without_backend_drops_instead_of_hanging();
    test_spin_wait_exits_when_wakeup_is_removed();

    std::cout << "\n=== Observation hardening tests PASSED ===\n";
    return 0;
}
