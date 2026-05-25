// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_spsc_queue.cpp
//
// Unit tests for SPSCQueue, ThreadContext, and ThreadContextManager

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include "observe/ObservationQueue.hpp"
#include "observe/SPSCQueue.hpp"
#include "observe/ThreadContext.hpp"
#include "observe/ThreadContextManager.hpp"

using namespace chronon::observe;

// ============================================================================
// SPSCQueue Tests
// ============================================================================

void test_basic_write_read() {
    std::cout << "Testing basic write/read... ";

    SPSCQueue queue(4096);

    // Write a simple message
    struct TestRecord {
        ObservationQueue::RecordHeader header;
        uint64_t data;
    };

    TestRecord record;
    record.header.total_size = sizeof(TestRecord);
    record.header.type = ObservationQueue::EventType::TRACE_EVENT;
    record.header.flags = 1;
    record.header.padding = 0;
    record.data = 0xDEADBEEF;

    auto* ptr = queue.prepareWrite(sizeof(TestRecord));
    assert(ptr != nullptr);
    std::memcpy(ptr, &record, sizeof(TestRecord));
    queue.finishAndCommitWrite(sizeof(TestRecord));

    // Force commit to make visible
    queue.forceCommitWrite();

    // Read it back
    auto* read_ptr = queue.prepareRead();
    assert(read_ptr != nullptr);

    auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(read_ptr);
    assert(header->total_size == sizeof(TestRecord));
    assert(header->type == ObservationQueue::EventType::TRACE_EVENT);

    auto* read_record = reinterpret_cast<const TestRecord*>(read_ptr);
    assert(read_record->data == 0xDEADBEEF);
    (void)read_record;  // Suppress unused warning in release

    queue.finishRead(header->total_size);
    queue.forceCommitRead();

    // Queue should be empty now
    assert(queue.prepareRead() == nullptr);

    std::cout << "PASSED\n";
}

void test_multiple_records() {
    std::cout << "Testing multiple records... ";

    SPSCQueue queue(4096);

    // Write 100 records
    for (int i = 0; i < 100; ++i) {
        struct Record {
            ObservationQueue::RecordHeader header;
            int value;
        };

        Record record;
        record.header.total_size = sizeof(Record);
        record.header.type = ObservationQueue::EventType::TRACE_EVENT;
        record.header.flags = 1;
        record.header.padding = 0;
        record.value = i;

        auto* ptr = queue.prepareWrite(sizeof(Record));
        assert(ptr != nullptr);
        std::memcpy(ptr, &record, sizeof(Record));
        queue.finishAndCommitWrite(sizeof(Record));
    }

    queue.forceCommitWrite();

    // Read all 100 records
    struct RecordRead {
        ObservationQueue::RecordHeader h;
        int v;
    };

    int expected = 0;
    while (auto* ptr = queue.prepareRead()) {
        auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
        auto* record = reinterpret_cast<const RecordRead*>(ptr);
        assert(record->v == expected);
        (void)record;  // Suppress unused warning
        queue.finishRead(header->total_size);
        ++expected;
    }

    queue.forceCommitRead();
    assert(expected == 100);
    (void)expected;  // Suppress unused warning

    std::cout << "PASSED\n";
}

void test_queue_full() {
    std::cout << "Testing queue full... ";

    // Small queue to test overflow
    SPSCQueue queue(256);  // Will be rounded to power of 2

    // Fill the queue
    size_t written = 0;
    while (true) {
        auto* wptr = queue.prepareWrite(32);
        if (!wptr) break;
        queue.finishAndCommitWrite(32);
        written += 32;
    }

    assert(written > 0);
    (void)written;  // Suppress warning
    queue.forceCommitWrite();

    // Should fail to write more
    assert(queue.prepareWrite(32) == nullptr);

    // Read some data
    queue.forceCommitRead();
    auto* rptr = queue.prepareRead();
    assert(rptr != nullptr);
    (void)rptr;  // Suppress warning
    queue.finishRead(32);
    queue.forceCommitRead();

    // Now should be able to write again
    auto* wptr2 = queue.prepareWrite(32);
    assert(wptr2 != nullptr);
    (void)wptr2;  // Suppress warning
    queue.finishAndCommitWrite(32);

    std::cout << "PASSED\n";
}

void test_producer_consumer() {
    std::cout << "Testing producer-consumer threading... ";

    SPSCQueue queue(64 * 1024);  // 64KB
    std::atomic<bool> done{false};
    std::atomic<int> consumed{0};
    constexpr int NUM_RECORDS = 10000;

    // Consumer thread
    std::thread consumer([&]() {
        int count = 0;
        while (true) {
            while (auto* ptr = queue.prepareRead()) {
                auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
                queue.finishRead(header->total_size);
                ++count;
            }
            queue.commitRead();

            if (done.load(std::memory_order_acquire)) {
                // Producer finished and called forceCommitWrite() before setting done.
                // The acquire fence ensures all committed writes are visible.
                // Drain any remaining records.
                while (auto* ptr = queue.prepareRead()) {
                    auto* header = reinterpret_cast<const ObservationQueue::RecordHeader*>(ptr);
                    queue.finishRead(header->total_size);
                    ++count;
                }
                break;
            }

            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
        consumed.store(count, std::memory_order_release);
    });

    // Producer thread (main)
    struct Record {
        ObservationQueue::RecordHeader header;
        int value;
    };

    for (int i = 0; i < NUM_RECORDS; ++i) {
        Record record;
        record.header.total_size = sizeof(Record);
        record.header.type = ObservationQueue::EventType::TRACE_EVENT;
        record.header.flags = 1;
        record.header.padding = 0;
        record.value = i;

        // Retry if queue is full
        while (true) {
            auto* ptr = queue.prepareWrite(sizeof(Record));
            if (ptr) {
                std::memcpy(ptr, &record, sizeof(Record));
                queue.finishAndCommitWrite(sizeof(Record));
                break;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }

    queue.forceCommitWrite();
    done.store(true, std::memory_order_release);
    consumer.join();

    assert(consumed.load() == NUM_RECORDS);

    std::cout << "PASSED\n";
}

// ============================================================================
// ThreadContext Tests
// ============================================================================

void test_thread_context_basic() {
    std::cout << "Testing ThreadContext basic... ";

    ThreadContext ctx(0, 4096);

    assert(ctx.id() == 0);
    assert(ctx.droppedCount() == 0);

    // Test SizeCacheVector
    SizeCacheVector& cache = ctx.sizeCache();
    cache.clear();
    assert(cache.empty());

    cache.push(10);
    cache.push(20);
    cache.push(30);
    assert(cache.size() == 3);

    // Pop in FIFO order
    assert(cache.pop() == 10);
    assert(cache.pop() == 20);
    assert(cache.pop() == 30);
    assert(cache.empty());

    std::cout << "PASSED\n";
}

void test_thread_context_dropped() {
    std::cout << "Testing ThreadContext dropped count... ";

    ThreadContext ctx(1, 256);  // Small queue

    ctx.incrementDropped();
    ctx.incrementDropped();
    ctx.incrementDropped();

    assert(ctx.droppedCount() == 3);

    std::cout << "PASSED\n";
}

// ============================================================================
// ThreadContextManager Tests
// ============================================================================

void test_context_manager_single_thread() {
    std::cout << "Testing ThreadContextManager single thread... ";

    // Get context for current thread
    ThreadContext* ctx = ThreadContextManager::instance().getContext();
    assert(ctx != nullptr);

    // Getting again should return same context
    ThreadContext* ctx2 = ThreadContextManager::instance().getContext();
    assert(ctx2 == ctx);
    (void)ctx;   // Suppress warning
    (void)ctx2;  // Suppress warning

    std::cout << "PASSED\n";
}

void test_context_manager_multiple_threads() {
    std::cout << "Testing ThreadContextManager multiple threads... ";

    std::vector<ThreadContext*> contexts;
    std::mutex mutex;

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&]() {
            ThreadContext* ctx = ThreadContextManager::instance().getContext();
            assert(ctx != nullptr);

            std::lock_guard<std::mutex> lock(mutex);
            contexts.push_back(ctx);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Each thread should have a different context
    for (size_t i = 0; i < contexts.size(); ++i) {
        for (size_t j = i + 1; j < contexts.size(); ++j) {
            assert(contexts[i] != contexts[j]);
        }
    }

    std::cout << "PASSED\n";
}

void test_context_manager_foreach() {
    std::cout << "Testing ThreadContextManager forEachContext... ";

    // Count active contexts
    size_t count = 0;
    ThreadContextManager::instance().forEachContext([&](ThreadContext* c) {
        assert(c != nullptr);
        (void)c;  // Suppress warning
        ++count;
    });

    assert(count >= 1);  // At least the main thread context
    assert(count == ThreadContextManager::instance().activeThreadCount());

    std::cout << "PASSED\n";
}

// ============================================================================
// Performance Benchmark
// ============================================================================

void benchmark_spsc_throughput() {
    std::cout << "Benchmarking SPSC queue throughput... ";

    SPSCQueue queue(1024 * 1024);  // 1MB
    constexpr int NUM_RECORDS = 1000000;
    constexpr size_t RECORD_SIZE = 40;  // Typical structured record size

    auto start = std::chrono::high_resolution_clock::now();

    // Write records
    for (int i = 0; i < NUM_RECORDS; ++i) {
        auto* wptr = queue.prepareWrite(RECORD_SIZE);
        if (!wptr) break;
        (void)wptr;  // Suppress warning
        queue.finishWrite(RECORD_SIZE);
        if (i % 100 == 0) {
            queue.commitWrite();
        }
    }
    queue.forceCommitWrite();

    // Read records
    int count = 0;
    while (auto* rptr = queue.prepareRead()) {
        (void)rptr;  // Suppress warning
        queue.finishRead(RECORD_SIZE);
        ++count;
        if (count % 100 == 0) {
            queue.commitRead();
        }
    }
    queue.forceCommitRead();

    auto end = std::chrono::high_resolution_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

    double ns_per_op = static_cast<double>(ns) / (NUM_RECORDS * 2);  // Write + read
    double ops_per_sec = (NUM_RECORDS * 2.0) / (ns / 1e9);

    std::cout << "PASSED (" << ns_per_op << " ns/op, " << ops_per_sec / 1e6 << " M ops/sec)\n";
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "\n=== SPSCQueue Tests ===\n";
    test_basic_write_read();
    test_multiple_records();
    test_queue_full();
    test_producer_consumer();

    std::cout << "\n=== ThreadContext Tests ===\n";
    test_thread_context_basic();
    test_thread_context_dropped();

    std::cout << "\n=== ThreadContextManager Tests ===\n";
    test_context_manager_single_thread();
    test_context_manager_multiple_threads();
    test_context_manager_foreach();

    std::cout << "\n=== Performance Benchmarks ===\n";
    benchmark_spsc_throughput();

    std::cout << "\n=== All tests passed! ===\n\n";
    return 0;
}
