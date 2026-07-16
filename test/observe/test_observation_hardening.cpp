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
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

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

#include "PftraceTestDecoder.hpp"
#include "observe/CounterRegistry.hpp"
#include "observe/FormatRegistry.hpp"
#include "observe/LookaheadBuffer.hpp"
#include "observe/ObservationBackend.hpp"
#include "observe/ObservationContext.hpp"
#include "observe/ObservationManager.hpp"
#include "observe/ObservationYAMLConfig.hpp"
#include "observe/ReorderBuffer.hpp"
#include "observe/ThreadContextManager.hpp"

namespace chronon::observe {

struct ObservationBackendTestAccess {
    static size_t pipelineSliceNameCacheMaxEntries() {
        return ObservationBackend::PIPELINE_SLICE_NAME_CACHE_MAX_ENTRIES;
    }

    static const ObservationBackend::PipelineSliceNames& pipelineSliceNames(
        ObservationBackend& backend, uint64_t payload, bool hex_name) {
        return backend.pipelineSliceNames_(payload, hex_name);
    }

    static size_t pipelineSliceNameCacheSize(ObservationBackend& backend, bool hex_name) {
        return backend.pipeline_slice_name_cache_[hex_name ? 1 : 0].size();
    }
};

}  // namespace chronon::observe

using namespace chronon::observe;
using namespace pftrace_test;

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

void test_timeline_disabled_config_disables_producer() {
    std::cout << "Testing timeline disabled config disables producer... ";

    auto& mgr = ObservationManager::instance();
    mgr.reset();

    auto cfg = makeEnabledConfig("/tmp/chronon_obs_timeline_disabled");
    cfg.timeline.enabled = false;
    mgr.initialize(cfg);

    ObservationContext* ctx = mgr.createContextForUnit("timeline_disabled", []() { return 0ULL; });
    assert(ctx != nullptr);
    assert(!ctx->timelineEventsEnabled());

    ctx->enableCategory(category::TRACE);
    assert(!ctx->timelineEvent(category::TRACE, TimelineEventKind::Instant, /*track_id=*/1,
                               /*slot=*/0, /*name_id=*/0, /*payload=*/0, nullptr, 0));
    assert(ctx->observationStats().get<ObservationChannel::Trace>().emitted == 0);
    assert(ctx->observationStats().get<ObservationChannel::Trace>().dropped == 0);

    mgr.shutdown();
    mgr.reset();
    std::filesystem::remove_all(cfg.output_dir);

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
    backend_cfg.trace_text = true;
    backend_cfg.timeline_enabled = false;

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

// ---------------------------------------------------------------------------
// Timeline track re-declaration across backend restart
// ---------------------------------------------------------------------------

/// Minimal protobuf wire-format walk over a .pftrace: collects track UUIDs
/// declared by TrackDescriptors and referenced by TrackEvents.
struct TimelineTrackScan {
    std::set<uint64_t> declared;
    std::set<uint64_t> referenced;
    size_t events = 0;
};

uint64_t scanVarint(const std::vector<unsigned char>& buf, size_t& p) {
    uint64_t value = 0;
    int shift = 0;
    while (p < buf.size()) {
        unsigned char byte = buf[p++];
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    return value;
}

TimelineTrackScan scanTimelineTracks(const std::filesystem::path& path) {
    TimelineTrackScan scan;
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        return scan;
    }
    std::vector<unsigned char> buf{std::istreambuf_iterator<char>(in),
                                   std::istreambuf_iterator<char>()};

    // Trace.packet = 1; TracePacket.track_event = 11, .track_descriptor = 60;
    // TrackDescriptor.uuid = 1; TrackEvent.track_uuid = 11.
    size_t pos = 0;
    while (pos < buf.size()) {
        uint64_t tag = scanVarint(buf, pos);
        if (tag >> 3 != 1 || (tag & 0x7) != 2) break;  // Corrupt file: stop.
        uint64_t pkt_len = scanVarint(buf, pos);
        size_t pkt_end = pos + pkt_len;
        while (pos < pkt_end) {
            uint64_t ftag = scanVarint(buf, pos);
            uint64_t field = ftag >> 3;
            if ((ftag & 0x7) == 0) {
                scanVarint(buf, pos);
                continue;
            }
            uint64_t len = scanVarint(buf, pos);
            size_t body_end = pos + len;
            if (field == 60 || field == 11) {
                const uint64_t want_field = (field == 60) ? 1 : 11;
                auto& dest = (field == 60) ? scan.declared : scan.referenced;
                if (field == 11) scan.events++;
                while (pos < body_end) {
                    uint64_t stag = scanVarint(buf, pos);
                    if ((stag & 0x7) == 0) {
                        uint64_t value = scanVarint(buf, pos);
                        if (stag >> 3 == want_field) dest.insert(value);
                    } else {
                        uint64_t sub_len = scanVarint(buf, pos);
                        pos += sub_len;
                    }
                }
            }
            pos = body_end;
        }
        pos = pkt_end;
    }
    return scan;
}

/// Regression (PR #41 review): a restarted backend opens a fresh
/// timeline.pftrace, so it must re-declare its process/unit/counter tracks
/// instead of referencing cached UUIDs that only exist in the previous file.
void test_timeline_restart_redeclares_tracks() {
    std::cout << "Testing timeline track re-declaration across backend restart... ";

    const std::string out_dir = "/tmp/chronon_obs_timeline_restart";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(64 * 1024);

    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_enabled = true;
    // The raw wire scan below reads packets directly; keep them uncompressed
    // (compression round-trips are covered by test_perfetto_trace_writer).
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);

    const FormatId fmt_id = FormatRegistry::instance().registerFormat(
        "restart round {}", __FILE__, __LINE__, {ArgType::UInt64}, false, LogLevel::Info);

    uint64_t cycle = 1;
    std::filesystem::path last_timeline;
    for (uint64_t round = 0; round < 2; ++round) {
        backend.start();

        ObservationContext ctx(&queue, [&cycle]() { return cycle; }, 0, "restart_unit", 1);
        ctx.enableCategory(category::TRACE);
        ctx.trace(category::TRACE, fmt_id, round);
        ThreadContextManager::instance().flushAll();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        last_timeline = backend.outputDir() / "timeline.pftrace";
        backend.stop();
    }

    TimelineTrackScan scan = scanTimelineTracks(last_timeline);
    bool ok = scan.events > 0 && !scan.referenced.empty();
    for (uint64_t uuid : scan.referenced) {
        if (scan.declared.count(uuid) == 0) {
            ok = false;
        }
    }
    if (!ok) {
        std::cerr << "FAILED: second-run timeline at " << last_timeline << " has " << scan.events
                  << " events, " << scan.declared.size() << " declared tracks, "
                  << scan.referenced.size()
                  << " referenced tracks; every referenced track must be declared\n";
        std::abort();
    }

    std::cout << "PASSED\n";
}

void test_counter_group_does_not_collide_with_child_unit() {
    std::cout << "Testing counter group avoids child unit path collision... ";

    const std::string out_dir = "/tmp/chronon_counter_group_collision";
    std::filesystem::remove_all(out_dir);

    ObservationQueue queue(64 * 1024);
    ObservationBackend::Config cfg;
    cfg.output_dir = out_dir;
    cfg.enable_counter_csv = false;
    cfg.enable_reordering = false;
    cfg.timeline_compress = false;

    ObservationBackend backend(queue, cfg);
    backend.setSourceNameLookup([](uint16_t id) -> std::string_view {
        if (id == 1) return "core";
        if (id == 2) return "core.counters";
        return "";
    });
    backend.start();

    ObservationContext child_ctx(&queue, []() { return 1ULL; }, 0, "core.counters", 2);
    child_ctx.enableCategory(category::TRACE);
    const FormatId fmt =
        FormatRegistry::instance().registerFormat("child", __FILE__, __LINE__, {}, true);
    child_ctx.trace(category::TRACE, fmt);

    SimpleCounter counter;
    counter.increment(7);
    CounterRegistry registry;
    registry.registerCounter("core", makeCounterId(0), &counter, "ticks");
    registry.dumpFinalSnapshot(2, &queue, {});
    ThreadContextManager::instance().flushAll();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const std::filesystem::path timeline = backend.outputDir() / "timeline.pftrace";
    backend.stop();
    DecodedTrace trace = decodeFile(timeline);

    const DecodedTrack* core = nullptr;
    for (const auto& track : trace.tracks) {
        if (track.name == "core") {
            core = &track;
            break;
        }
    }
    assert(core != nullptr);

    std::vector<uint64_t> counter_group_uuids;
    for (const auto& track : trace.tracks) {
        if (track.name == "counters" && track.parent_uuid == core->uuid) {
            counter_group_uuids.push_back(track.uuid);
        }
    }
    assert(counter_group_uuids.size() == 2);

    size_t counter_tracks = 0;
    for (uint64_t group_uuid : counter_group_uuids) {
        for (const auto& track : trace.tracks) {
            if (track.parent_uuid == group_uuid && track.name == "ticks" && track.is_counter) {
                ++counter_tracks;
            }
        }
    }
    if (counter_tracks != 1) {
        std::cerr << "FAILED: expected one pull-model ticks counter track, got " << counter_tracks
                  << "\n";
        std::abort();
    }

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_periodic_counter_snapshot_is_metadata_indexed_batch() {
    std::cout << "Testing periodic counters use one metadata-indexed batch... ";

    ObservationQueue shared(64 * 1024);
    std::vector<std::unique_ptr<ObservationContext>> contexts;
    auto ctx =
        std::make_unique<ObservationContext>(&shared, []() { return 9ULL; }, 0, "batch_unit");
    ctx->setCounterOwnerId(0);
    const CounterId first = ctx->counters().addCounter("first");
    const CounterId second = ctx->counters().addCounter("second");
    ctx->counters().getUnchecked(first).increment(3);
    ctx->counters().getUnchecked(second).increment(7);
    auto* raw = ctx.get();
    contexts.push_back(std::move(ctx));

    CounterRegistry registry;
    registry.reregisterAll(contexts);
    CHECK(registry.snapshotPlans().size() == 1);
    CHECK(registry.snapshotPlans()[0].entries.size() == 2);

    ThreadContext producer(63, 64 * 1024);
    const size_t owner = 0;
    CHECK(registry.pushOwnerSnapshots(10, std::span<const size_t>(&owner, 1), producer));

    auto& queue = producer.queue();
    std::byte* ptr = queue.prepareRead();
    CHECK(ptr != nullptr);
    const auto* record = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
    CHECK(record->type == ObservationQueue::EventType::COUNTER_SNAPSHOT);
    CHECK((record->flags & COUNTER_SNAPSHOT_BATCH_FLAG) != 0);
    CHECK(record->total_size == sizeof(ObservationQueue::RecordHeader) +
                                    sizeof(CounterSnapshotBatchHeader) + 2 * sizeof(uint64_t));

    CounterSnapshotBatchHeader batch{};
    const std::byte* data = ptr + sizeof(ObservationQueue::RecordHeader);
    std::memcpy(&batch, data, sizeof(batch));
    CHECK(batch.cycle == 10);
    CHECK(batch.plan_id == 0);
    CHECK(batch.count == 2);
    uint64_t values[2]{};
    std::memcpy(values, data + sizeof(batch), sizeof(values));
    CHECK(values[0] == 3);
    CHECK(values[1] == 7);

    queue.finishRead(record->total_size);
    queue.forceCommitRead();
    CHECK(queue.prepareRead() == nullptr);
    CHECK(raw->counters().getUnchecked(first).get() == 0);
    CHECK(raw->counters().getUnchecked(second).get() == 0);

    std::cout << "PASSED\n";
}

void test_pivoted_csv_preserves_unowned_final_counter_columns() {
    std::cout << "Testing pivoted CSV preserves unowned final counter columns... ";

    const std::string out_dir = "/tmp/chronon_unowned_final_counter";
    std::filesystem::remove_all(out_dir);

    ObservationQueue shared(64 * 1024);
    std::vector<std::unique_ptr<ObservationContext>> contexts;

    auto owned = std::make_unique<ObservationContext>(&shared, []() { return 10ULL; }, 0, "owned");
    owned->setCounterOwnerId(0);
    const CounterId periodic = owned->counters().addCounter("periodic");
    owned->counters().getUnchecked(periodic).increment(3);
    contexts.push_back(std::move(owned));

    auto unowned =
        std::make_unique<ObservationContext>(&shared, []() { return 10ULL; }, 0, "unowned");
    unowned->enableCategory(category::LOG_INFO);
    const CounterId final_only = unowned->counters().addCounter("final_only");
    unowned->counters().getUnchecked(final_only).increment(7);
    contexts.push_back(std::move(unowned));

    CounterRegistry registry;
    registry.reregisterAll(contexts);
    CHECK(registry.snapshotPlans().size() == 1);
    CHECK(registry.counterColumns().size() == 4);

    ObservationBackend::Config config;
    config.output_dir = out_dir;
    config.enable_counter_csv = true;
    config.counter_csv_format = CounterCsvFormat::Pivoted;
    config.enable_reordering = false;
    config.timeline_enabled = false;

    ObservationBackend backend(shared, config);
    backend.setCounterColumns(registry.counterColumns());
    backend.setCounterSnapshotPlans(registry.snapshotPlans());
    backend.start();
    const auto csv_path = backend.outputDir() / "counters.csv";

    registry.dumpFinalSnapshot(10, &shared, contexts);
    backend.stop();

    std::ifstream csv(csv_path);
    std::string header;
    std::string row;
    CHECK(static_cast<bool>(std::getline(csv, header)));
    CHECK(static_cast<bool>(std::getline(csv, row)));
    CHECK(header.find("owned.periodic") != std::string::npos);
    CHECK(header.find("unowned.final_only") != std::string::npos);
    CHECK(header.find("unowned.obs_info_emitted") != std::string::npos);
    CHECK(header.find("unowned.obs_info_dropped") != std::string::npos);
    CHECK(row.starts_with("10,"));

    std::filesystem::remove_all(out_dir);
    std::cout << "PASSED\n";
}

void test_pipeline_slice_name_cache_is_bounded() {
    std::cout << "Testing pipeline slice name cache bound... ";

    ObservationQueue queue(1024);
    ObservationBackend backend(queue);
    const size_t max_entries = ObservationBackendTestAccess::pipelineSliceNameCacheMaxEntries();

    for (size_t i = 0; i < max_entries; ++i) {
        const auto& names = ObservationBackendTestAccess::pipelineSliceNames(backend, i, false);
        CHECK(!names.category.empty());
        CHECK(names.event_name.starts_with(std::to_string(i)));
    }
    CHECK(ObservationBackendTestAccess::pipelineSliceNameCacheSize(backend, false) == max_entries);

    const auto& overflow = ObservationBackendTestAccess::pipelineSliceNames(
        backend, static_cast<uint64_t>(max_entries), false);
    CHECK(overflow.event_name.starts_with(std::to_string(max_entries)));
    CHECK(ObservationBackendTestAccess::pipelineSliceNameCacheSize(backend, false) == max_entries);

    const auto& hex = ObservationBackendTestAccess::pipelineSliceNames(backend, 0x2a, true);
    CHECK(hex.event_name.starts_with("0x2a"));
    CHECK(ObservationBackendTestAccess::pipelineSliceNameCacheSize(backend, true) == 1);

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Observation Hardening Tests ===\n\n";

    test_reinitialize_without_deadlock();
    test_source_registry_freezes_after_backend_start();
    test_timeline_disabled_config_disables_producer();
    test_reorder_buffer_force_flush();
    test_lookahead_structured_args_commit_and_rollback();
    test_no_drops_under_pressure_debug();
    test_pressure_without_backend_drops_instead_of_hanging();
    test_spin_wait_exits_when_wakeup_is_removed();
    test_timeline_restart_redeclares_tracks();
    test_counter_group_does_not_collide_with_child_unit();
    test_periodic_counter_snapshot_is_metadata_indexed_batch();
    test_pivoted_csv_preserves_unowned_final_counter_columns();
    test_pipeline_slice_name_cache_is_bounded();

    std::cout << "\n=== Observation hardening tests PASSED ===\n";
    return 0;
}
