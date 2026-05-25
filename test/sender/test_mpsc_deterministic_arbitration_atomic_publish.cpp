// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// test_mpsc_deterministic_arbitration_atomic_publish.cpp
//
// Determinism regression test for MPSC InPorts under the lookahead scheduler
// (consumer-tick-driven arbitration; see docs/mpsc-atomic-publish.md).
//
// Topology: 3 producer TickableUnits, each on its own worker thread, each
// sending 2 messages/tick for 100 ticks, fanning into a single consumer
// TickableUnit whose MPSC InPort has user_capacity set to 4.
//
// Parameter sweep: num_workers in {1, 2, 3, 4, 6, 8} x 5 repeats each.
// num_workers=1 exercises the Sequential fallback: TickSimulation disables
// enable_parallel when num_workers < 2, so the lookahead path is replaced
// by the per-cycle sequential loop and arbitration runs via the main-thread
// arbitrateAllMPSCPorts_() path. Including it in the sweep pins the total
// cycle count to be identical with the parallel cases.
//
// Expectations:
//   - All 30 runs produce the same (arrive_cycle, producer_id, sequence) trace.
//   - Total cycle counts match across all runs.
//   - Every producer sends exactly 200 messages and all 600 are received.

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/Port.hpp"

using namespace chronon::sender;

namespace {

struct Msg {
    uint32_t producer_id;
    uint64_t send_cycle;
    uint64_t sequence_number;
};

struct TraceEntry {
    uint64_t arrive_cycle;
    uint32_t producer_id;
    uint64_t sequence_number;

    bool operator==(const TraceEntry& o) const {
        return arrive_cycle == o.arrive_cycle && producer_id == o.producer_id &&
               sequence_number == o.sequence_number;
    }
};

class Producer : public TickableUnit {
public:
    Producer(uint32_t id, uint64_t total_ticks)
        : TickableUnit("producer_" + std::to_string(id)), id_(id), total_ticks_(total_ticks) {}

    OutPort<Msg> out{this, "out"};

    void tick() override {
        if (ticks_ >= total_ticks_) return;
        const uint64_t c = localCycle();
        for (int i = 0; i < 2; ++i) {
            if (!out.canSend()) break;
            const bool ok = out.send(Msg{id_, c, seq_});
            if (!ok) break;
            ++seq_;
            ++sent_;
        }
        ++ticks_;
    }

    bool isCompleted() const override { return ticks_ >= total_ticks_; }
    uint64_t sent() const { return sent_; }

private:
    uint32_t id_;
    uint64_t total_ticks_;
    uint64_t ticks_ = 0;
    uint64_t seq_ = 0;
    uint64_t sent_ = 0;
};

class Consumer : public TickableUnit {
public:
    Consumer() : TickableUnit("consumer") {}

    InPort<Msg> in{this, "in", /*capacity=*/64};

    void tick() override {
        const uint64_t c = localCycle();
        while (auto m = in.tryReceive(c)) {
            trace_.push_back(TraceEntry{c, m->producer_id, m->sequence_number});
        }
    }

    const std::vector<TraceEntry>& trace() const { return trace_; }

private:
    std::vector<TraceEntry> trace_;
};

struct RunResult {
    std::vector<TraceEntry> trace;
    uint64_t total_cycles = 0;
    std::array<uint64_t, 3> sent_per_producer{};
    uint64_t total_received = 0;
};

RunResult runOnce(size_t num_workers, uint64_t producer_ticks, size_t user_cap) {
    TickSimulationConfig cfg;
    cfg.num_threads = num_workers;
    // Lookahead requires parallel execution; TickSimulation forces
    // enable_parallel=false when num_workers<2, which is the intended
    // sequential fallback we want to pin alongside the lookahead path.
    cfg.enable_parallel = (num_workers > 1);
    cfg.enable_lookahead = true;
    cfg.epoch_size = 64;

    TickSimulation sim(cfg);

    auto* p0 = sim.createUnit<Producer>(0u, producer_ticks);
    auto* p1 = sim.createUnit<Producer>(1u, producer_ticks);
    auto* p2 = sim.createUnit<Producer>(2u, producer_ticks);
    auto* cons = sim.createUnit<Consumer>();

    // Three producers -> one consumer InPort, delay=1.
    sim.connect(p0->out, cons->in, /*delay=*/1);
    sim.connect(p1->out, cons->in, /*delay=*/1);
    sim.connect(p2->out, cons->in, /*delay=*/1);

    sim.initialize();
    cons->in.setCapacity(user_cap);

    const uint64_t total = sim.run(producer_ticks + 64);

    RunResult r;
    r.trace = cons->trace();
    r.total_cycles = total;
    r.sent_per_producer = {p0->sent(), p1->sent(), p2->sent()};
    r.total_received = r.trace.size();
    return r;
}

std::string formatTraceHead(const std::vector<TraceEntry>& t, size_t n = 16) {
    std::ostringstream oss;
    size_t lim = std::min(n, t.size());
    for (size_t i = 0; i < lim; ++i) {
        oss << "  [" << i << "] ac=" << t[i].arrive_cycle << " pid=" << t[i].producer_id
            << " seq=" << t[i].sequence_number << "\n";
    }
    if (t.size() > lim) {
        oss << "  ... (+" << (t.size() - lim) << " more entries)\n";
    }
    return oss.str();
}

size_t firstDiffIndex(const std::vector<TraceEntry>& a, const std::vector<TraceEntry>& b) {
    size_t lim = std::min(a.size(), b.size());
    for (size_t i = 0; i < lim; ++i) {
        if (!(a[i] == b[i])) return i;
    }
    return lim;
}

void run_with_timeout(std::function<void()> body, std::chrono::seconds timeout, const char* label) {
    std::mutex m;
    std::condition_variable cv;
    bool done = false;
    std::exception_ptr eptr;

    std::thread worker([&]() {
        try {
            body();
        } catch (...) {
            eptr = std::current_exception();
        }
        {
            std::lock_guard<std::mutex> lk(m);
            done = true;
        }
        cv.notify_all();
    });

    bool finished = false;
    {
        std::unique_lock<std::mutex> lk(m);
        finished = cv.wait_for(lk, timeout, [&]() { return done; });
    }

    if (!finished) {
        std::cerr << "DEADLOCK: " << label << " did not complete within " << timeout.count()
                  << "s\n";
        worker.detach();
        std::abort();
    }
    worker.join();
    if (eptr) std::rethrow_exception(eptr);
}

}  // namespace

int main() {
    std::cout << "=== MPSC + Lookahead Deterministic Arbitration Test ===\n\n";

    constexpr uint64_t PRODUCER_TICKS = 100;
    constexpr size_t USER_CAP = 4;
    constexpr uint64_t MSGS_PER_PRODUCER = PRODUCER_TICKS * 2;

    const std::vector<size_t> worker_counts = {1, 2, 3, 4, 6, 8};
    constexpr int REPEATS = 5;

    std::vector<RunResult> results;
    results.reserve(worker_counts.size() * REPEATS);

    int run_idx = 0;
    int failures = 0;

    for (size_t nw : worker_counts) {
        for (int rep = 0; rep < REPEATS; ++rep) {
            RunResult r;
            run_with_timeout(
                [&]() { r = runOnce(nw, PRODUCER_TICKS, USER_CAP); }, std::chrono::seconds(30),
                ("runOnce nw=" + std::to_string(nw) + " rep=" + std::to_string(rep)).c_str());
            std::cout << "[run " << run_idx << "] num_workers=" << nw << " rep=" << rep
                      << " trace_len=" << r.trace.size() << " total_cycles=" << r.total_cycles
                      << " sent=[" << r.sent_per_producer[0] << "," << r.sent_per_producer[1] << ","
                      << r.sent_per_producer[2] << "]\n";
            results.push_back(std::move(r));
            ++run_idx;
        }
    }

    const RunResult& golden = results.front();

    // Check 1: no messages lost.
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& r = results[i];
        for (int p = 0; p < 3; ++p) {
            if (r.sent_per_producer[p] != MSGS_PER_PRODUCER) {
                std::cerr << "FAIL run " << i << ": producer " << p << " sent "
                          << r.sent_per_producer[p] << " expected " << MSGS_PER_PRODUCER << "\n";
                ++failures;
            }
        }
        if (r.total_received != 3 * MSGS_PER_PRODUCER) {
            std::cerr << "FAIL run " << i << ": received " << r.total_received << " expected "
                      << (3 * MSGS_PER_PRODUCER) << "\n";
            ++failures;
        }
    }

    // Check 2: identical cycle count.
    for (size_t i = 1; i < results.size(); ++i) {
        if (results[i].total_cycles != golden.total_cycles) {
            std::cerr << "FAIL run " << i << ": total_cycles=" << results[i].total_cycles
                      << " differs from golden=" << golden.total_cycles << "\n";
            ++failures;
        }
    }

    // Check 3: identical trace.
    for (size_t i = 1; i < results.size(); ++i) {
        if (results[i].trace != golden.trace) {
            size_t idx = firstDiffIndex(results[i].trace, golden.trace);
            std::cerr << "FAIL run " << i << ": trace diverges from golden at index " << idx
                      << " (golden.len=" << golden.trace.size()
                      << ", this.len=" << results[i].trace.size() << ")\n";
            std::cerr << "Golden head:\n" << formatTraceHead(golden.trace);
            std::cerr << "This run head:\n" << formatTraceHead(results[i].trace);
            ++failures;
            break;
        }
    }

    if (failures == 0) {
        std::cout << "\n=== MPSC + Lookahead determinism: ALL " << results.size()
                  << " RUNS MATCH ===\n";
        return 0;
    }
    std::cerr << "\n=== MPSC + Lookahead determinism: " << failures << " failure(s) observed ===\n";
    return 1;
}
