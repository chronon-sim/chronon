// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

// bench_message_queue.cpp
//
// Real two-thread (dedicated producer + consumer) throughput benchmark for
// LockFreeMessageQueue, the SPSC ring the scheduler uses for cross-thread
// messaging. The consumer folds each value into a printed, in-order-asserted
// checksum, so the loop can't be elided like the single-thread SPSC micro-bench.
// Reports ns/item and M items/sec (one item = one push+pop handoff), best of N.
//
// Usage: bench_message_queue [num_items] [repeats]

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

#include "chronon/CpuPause.hpp"
#include "sender/port/MessageQueue.hpp"

using chronon::sender::LockFreeMessageQueue;

namespace {

template <size_t Bytes>
struct Payload {
    uint64_t seq;
    char pad[Bytes - sizeof(uint64_t)];
    Payload() : seq(0) { std::memset(pad, 0, sizeof(pad)); }
    explicit Payload(uint64_t s) : seq(s) { std::memset(pad, 0, sizeof(pad)); }
};

uint64_t seq_of(uint64_t v) { return v; }
template <size_t B>
uint64_t seq_of(const Payload<B>& p) {
    return p.seq;
}

// One full producer->consumer transfer of `n` items. Returns seconds elapsed;
// writes the consumer's checksum to chk (anti-elision + correctness sanity).
template <typename T>
double run_once(uint64_t n, uint64_t& chk) {
    LockFreeMessageQueue<T> q;
    std::atomic<bool> order_ok{true};

    auto t0 = std::chrono::steady_clock::now();

    std::thread consumer([&] {
        uint64_t got = 0;
        uint64_t sum = 0;
        uint64_t expected = 0;
        while (got < n) {
            if (auto v = q.tryPop(0)) {
                uint64_t s = seq_of(*v);
                if (s != expected) order_ok.store(false, std::memory_order_relaxed);
                ++expected;
                sum += s;
                ++got;
            } else {
                chronon::cpuPause();
            }
        }
        chk = sum;
    });

    for (uint64_t i = 0; i < n;) {
        if (q.tryPush(T(i), 0)) {
            ++i;
        } else {
            chronon::cpuPause();
        }
    }
    consumer.join();

    auto t1 = std::chrono::steady_clock::now();
    if (!order_ok.load(std::memory_order_relaxed)) {
        std::cerr << "ERROR: consumer observed out-of-order/lost items\n";
    }
    return std::chrono::duration<double>(t1 - t0).count();
}

template <typename T>
void bench(const char* label, uint64_t n, int repeats) {
    uint64_t chk = 0;
    double best = 1e30;
    run_once<T>(n / 10 + 1, chk);  // warmup
    for (int r = 0; r < repeats; ++r) {
        double s = run_once<T>(n, chk);
        if (s < best) best = s;
    }
    double ns_per_item = best * 1e9 / static_cast<double>(n);
    double mitems = static_cast<double>(n) / best / 1e6;
    std::printf("%-18s | %10.2f | %12.2f   [chk=%016llx]\n", label, ns_per_item, mitems,
                static_cast<unsigned long long>(chk));
}

}  // namespace

int main(int argc, char** argv) {
    const uint64_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 20000000ULL;
    const int repeats = argc > 2 ? std::atoi(argv[2]) : 3;

    std::cout << "=== LockFreeMessageQueue 2-thread throughput (producer+consumer) ===\n";
    std::cout << "items=" << n << "  repeats=" << repeats
              << "  ring_capacity=" << LockFreeMessageQueue<uint64_t>::CAPACITY
              << "  hw_concurrency=" << std::thread::hardware_concurrency() << "\n\n";
    std::cout << "payload            |  ns/item   |  M items/sec\n";
    std::cout << "-------------------+------------+-------------\n";

    bench<uint64_t>("uint64 (8B)", n, repeats);
    bench<Payload<64>>("struct (64B)", n, repeats);

    std::cout << "\n(One item = one cross-thread push+pop handoff.)\n";
    return 0;
}
