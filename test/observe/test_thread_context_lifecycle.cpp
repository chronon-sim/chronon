// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <latch>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../TestAssertions.hpp"
#include "observe/ObservationQueue.hpp"
#include "observe/ThreadContextManager.hpp"

using namespace chronon::observe;
using namespace std::chrono_literals;

namespace {

struct Record {
    ObservationQueue::RecordHeader header{sizeof(Record), ObservationQueue::EventType::LOG_EVENT, 0,
                                          0};
    uint64_t value;
};

void writeRecord(ThreadContext* ctx, uint64_t value) {
    CHECK(ctx != nullptr);
    Record record{};
    record.value = value;
    auto* ptr = ctx->queue().prepareWrite(sizeof(record));
    CHECK(ptr != nullptr);
    std::memcpy(ptr, &record, sizeof(record));
    // Deliberately leave a sub-batch tail unpublished until thread exit.
    ctx->queue().finishAndCommitWrite(sizeof(record));
}

uint64_t readRecord(ThreadContext* ctx) {
    auto* ptr = ctx->queue().prepareRead();
    CHECK(ptr != nullptr);
    Record record{};
    std::memcpy(&record, ptr, sizeof(record));
    CHECK(record.header.total_size == sizeof(record));
    ctx->queue().finishRead(sizeof(record));
    return record.value;
}

template <typename Predicate>
void waitUntil(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!predicate()) {
        CHECK(std::chrono::steady_clock::now() < deadline);
        std::this_thread::yield();
    }
}

void testSequential() {
    auto& manager = ThreadContextManager::instance();
    ThreadContext* first = nullptr;
    const auto dropped_before = manager.totalDroppedCount();
    for (uint64_t i = 0; i < 256; ++i) {
        std::thread([&] {
            auto* ctx = manager.getContext();
            CHECK(ctx != nullptr && ctx == manager.getContext());
            CHECK(manager.activeThreadCount() == 1);
            if (!first) first = ctx;
            CHECK(ctx == first);
            ctx->incrementDropped();
        }).join();
        CHECK(manager.activeThreadCount() == 0);
        CHECK(manager.allocatedContextCount() == 1);
        CHECK(first->queue().isDrained());
    }
    CHECK(manager.totalDroppedCount() == dropped_before + 256);

    // Repeated small tails also exercise ring wrap and consumer cursor reuse.
    for (uint64_t i = 0; i < 1024; ++i) {
        std::thread([&] { writeRecord(manager.getContext(), i); }).join();
        CHECK(manager.activeThreadCount() == 0);
        CHECK(readRecord(first) == i);
        first->queue().forceCommitRead();
        CHECK(first->queue().isDrained());
        CHECK(manager.allocatedContextCount() == 1);
    }
}

void testConcurrentLimit() {
    auto& manager = ThreadContextManager::instance();
    constexpr size_t count = ThreadContextManager::MAX_THREADS;
    std::array<ThreadContext*, count> contexts{};
    std::latch acquired(count), release(1);
    std::vector<std::thread> producers;
    for (size_t i = 0; i < count; ++i) {
        producers.emplace_back([&, i] {
            contexts[i] = manager.getContext();
            CHECK(contexts[i] != nullptr);
            acquired.count_down();
            release.wait();
        });
    }
    acquired.wait();
    CHECK(manager.activeThreadCount() == count);
    for (size_t i = 0; i < count; ++i)
        for (size_t j = 0; j < i; ++j) CHECK(contexts[i] != contexts[j]);
    CHECK(manager.getContext() == nullptr);
    release.count_down();
    for (auto& producer : producers) producer.join();
    CHECK(manager.activeThreadCount() == 0);
    for (size_t i = 0; i < 128; ++i) {
        std::thread([&] { CHECK(manager.getContext() != nullptr); }).join();
    }
    CHECK(manager.activeThreadCount() == 0);
    CHECK(manager.allocatedContextCount() == count);
}

void testLateTlsDestruction() {
    auto& manager = ThreadContextManager::instance();
    struct LateLookup {
        bool& rejected;
        ~LateLookup() { rejected = ThreadContextManager::instance().getContext() == nullptr; }
    };
    for (int i = 0; i < 128; ++i) {
        bool rejected = false;
        std::thread([&] {
            // Construct first so this destructor runs after the pool's lease.
            thread_local LateLookup late{rejected};
            (void)late;
            CHECK(manager.getContext() != nullptr);
        }).join();
        CHECK(rejected);
        CHECK(manager.activeThreadCount() == 0);
        CHECK(manager.allocatedContextCount() == 1);
    }
}

void testUnreadAndReaderHandoff() {
    auto& manager = ThreadContextManager::instance();
    constexpr size_t count = ThreadContextManager::MAX_THREADS;
    std::array<ThreadContext*, count> contexts{};
    for (size_t i = 0; i < count; ++i) {
        std::thread([&, i] {
            contexts[i] = manager.getContext();
            writeRecord(contexts[i], i);
        }).join();
    }
    CHECK(manager.activeThreadCount() == 0);
    CHECK(manager.getContext() == nullptr);  // All slots still have unread tails.
    CHECK(readRecord(contexts[0]) == 0);
    CHECK(manager.getContext() == nullptr);  // finishRead alone is not acknowledgement.
    contexts[0]->queue().forceCommitRead();

    // Keep using the same consumer pointer while a new producer owns the slot.
    std::thread([&] {
        auto* reused = manager.getContext();
        CHECK(reused == contexts[0]);
        writeRecord(reused, 999);
    }).join();
    CHECK(readRecord(contexts[0]) == 999);
    contexts[0]->queue().forceCommitRead();
    for (size_t i = 1; i < count; ++i) {
        CHECK(readRecord(contexts[i]) == i);
        contexts[i]->queue().forceCommitRead();
    }
    CHECK(manager.allocatedContextCount() == count);
    std::thread([&] { CHECK(manager.getContext() != nullptr); }).join();
}

void testConcurrentConsumer() {
    auto& manager = ThreadContextManager::instance();
    constexpr size_t workers = 8, rounds = 64, per_worker = 32;
    std::vector<bool> seen(workers * rounds * per_worker, false);
    std::atomic<size_t> consumed{0};
    std::atomic<bool> stop{false};
    std::thread consumer([&] {
        while (!stop.load(std::memory_order_acquire)) {
            manager.forEachContext([&](ThreadContext* ctx) {
                while (ctx->queue().prepareRead()) {
                    const uint64_t value = readRecord(ctx);
                    CHECK(value < seen.size() && !seen[value]);
                    seen[value] = true;
                    ctx->queue().eagerCommitRead();
                    consumed.fetch_add(1, std::memory_order_release);
                }
            });
            std::this_thread::yield();
        }
    });
    for (size_t round = 0; round < rounds; ++round) {
        std::latch ready(workers);
        std::vector<std::thread> producers;
        for (size_t worker = 0; worker < workers; ++worker) {
            producers.emplace_back([&, round, worker] {
                auto* ctx = manager.getContext();
                CHECK(ctx != nullptr);
                ready.arrive_and_wait();
                for (size_t i = 0; i < per_worker; ++i)
                    writeRecord(ctx, (round * workers + worker) * per_worker + i);
            });
        }
        for (auto& producer : producers) producer.join();
        waitUntil([&] {
            return consumed.load(std::memory_order_acquire) == (round + 1) * workers * per_worker;
        });
        CHECK(manager.activeThreadCount() == 0);
        CHECK(manager.allocatedContextCount() == workers);
    }
    stop.store(true, std::memory_order_release);
    consumer.join();
    for (bool value : seen) CHECK(value);
}

void testWakeupRemoval() {
    auto& manager = ThreadContextManager::instance();
    struct Notification {
        std::latch entered{1}, resume{1};
        std::atomic<bool> completed{false};
    };
    auto notification = std::make_unique<Notification>();
    manager.setBackendWakeup(
        [](void* ptr) {
            CHECK(ptr != nullptr);
            auto& state = *static_cast<Notification*>(ptr);
            state.entered.count_down();
            state.resume.wait();
            state.completed.store(true, std::memory_order_release);
        },
        notification.get());
    std::thread producer([&] { CHECK(manager.getContext() != nullptr); });
    notification->entered.wait();  // The thread-exit notification is in flight.
    std::latch removing(1);
    std::thread remover([&] {
        removing.count_down();
        manager.setBackendWakeup(nullptr, nullptr);
        CHECK(notification->completed.load(std::memory_order_acquire));
    });
    removing.wait();
    notification->resume.count_down();
    remover.join();
    notification.reset();  // No callback may still reference the destroyed backend.
    producer.join();
    CHECK(!manager.wakeBackend());
    for (int i = 0; i < 80; ++i)
        std::thread([&] { CHECK(manager.getContext() != nullptr); }).join();
    CHECK(manager.activeThreadCount() == 0);
}

void testFlushDuringRetirement() {
    auto& manager = ThreadContextManager::instance();
    std::atomic<bool> stop{false};
    std::thread flusher([&] {
        while (!stop.load(std::memory_order_acquire)) {
            manager.flushAll();
            std::this_thread::yield();
        }
    });
    for (int i = 0; i < 256; ++i) {
        // No concurrent event writes: only the exit tail publication races
        // with a backend's final flush of otherwise quiescent producers.
        std::thread([&] { CHECK(manager.getContext() != nullptr); }).join();
    }
    stop.store(true, std::memory_order_release);
    flusher.join();
    CHECK(manager.activeThreadCount() == 0);
    CHECK(manager.allocatedContextCount() == 1);
}

}  // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    ThreadContextManager::instance().setQueueCapacity(4096);
    const std::string mode = argv[1];
    if (mode == "sequential")
        testSequential();
    else if (mode == "concurrent_limit")
        testConcurrentLimit();
    else if (mode == "late_tls")
        testLateTlsDestruction();
    else if (mode == "unread")
        testUnreadAndReaderHandoff();
    else if (mode == "consumer")
        testConcurrentConsumer();
    else if (mode == "wakeup")
        testWakeupRemoval();
    else if (mode == "flush_exit")
        testFlushDuringRetirement();
    else
        CHECK(false);
    std::cout << "Thread context lifecycle " << mode << ": PASSED\n";
}
