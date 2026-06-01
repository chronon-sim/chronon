// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

// test_lookahead_floor_progress.cpp
//
// Regression test for issue #24: the lookahead floor (lookahead_floor_) is now
// refreshed lazily on the slow-path spin-wait instead of on every cycle
// advance. This test exercises a deliberately *imbalanced* cluster layout with
// a *small* max_lookahead_cycles so the fast clusters repeatedly race ahead,
// jam against the synthetic max_lookahead ceiling, and must rely on the lazily
// refreshed floor to unblock.
//
// The invariants checked:
//   1. No deadlock — every configuration completes (the CTest 60s timeout
//      would catch a hang where the floor stops rising).
//   2. Every unit advances exactly TARGET cycles (a lagged floor only delays;
//      it never drops or duplicates cycles).
//   3. Message accounting on a connected producer->consumer pair is exact
//      (no loss, no duplicates) even while the floor is stale.
//
// Topology (all independent clusters except the one connected pair, so the
// fast clusters are gated *only* by the floor ceiling, not by data deps):
//   - 1 slow standalone counter (busy work per tick -> drags the global min)
//   - 6 fast standalone counters (race ahead, hit the ceiling)
//   - 1 producer -> 1 consumer pair, delay=1 (exercises a real data dep too)
//
// Sweep: num_workers in {1, 2, 4, 8} x max_lookahead in {4, 16} x 2 repeats.

#include <cstdint>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/Port.hpp"

using namespace chronon::sender;

// White-box accessor (befriended by TickSimulation) used by the direct unit
// tests of refreshLookaheadFloor_ below. It drives the private progress array
// and floor on the main thread, so those checks are fully deterministic.
namespace chronon::sender {
struct LookaheadFloorTestAccess {
    static size_t count(const TickSimulation& s) { return s.thread_progress_count_; }
    static void setProgress(TickSimulation& s, size_t c, uint64_t v) {
        s.thread_progress_array_[c].completed_cycle.store(v, std::memory_order_relaxed);
    }
    static void setFloor(TickSimulation& s, uint64_t v) {
        s.lookahead_floor_.store(v, std::memory_order_relaxed);
    }
    static uint64_t floor(const TickSimulation& s) {
        return s.lookahead_floor_.load(std::memory_order_relaxed);
    }
    static void refresh(TickSimulation& s) { s.refreshLookaheadFloor_(); }
};
}  // namespace chronon::sender

namespace {

// A plain per-cycle counter. After run(TARGET) it must have ticked TARGET times.
class Counter : public TickableUnit {
public:
    explicit Counter(uint32_t id) : TickableUnit("counter_" + std::to_string(id)) {}
    void tick() override { ++ticks_; }
    uint64_t ticks() const { return ticks_; }

private:
    uint64_t ticks_ = 0;
};

// A counter that burns some CPU each tick so its cluster genuinely lags behind
// the fast counters in wall-clock time. It becomes (approximately) the global
// minimum completed cycle, holding the floor down and forcing the fast clusters
// to pile up against the max_lookahead ceiling.
class SlowCounter : public TickableUnit {
public:
    explicit SlowCounter(uint32_t id) : TickableUnit("slow_counter_" + std::to_string(id)) {}
    void tick() override {
        // Busy work the optimizer cannot elide. ~tens of thousands of adds.
        volatile uint64_t acc = sink_;
        for (int i = 0; i < 20000; ++i) {
            acc += i;
        }
        sink_ = acc;
        ++ticks_;
    }
    uint64_t ticks() const { return ticks_; }

private:
    uint64_t ticks_ = 0;
    uint64_t sink_ = 0;
};

struct Msg {
    uint64_t seq;
};

class Producer : public TickableUnit {
public:
    // Stop sending one delay-cycle before the run ends so every message has a
    // cycle in which to be delivered (a message sent at cycle c with delay=1 is
    // received at c+1). This keeps the message accounting exact rather than
    // leaving one in-flight tail message — the point under test is the floor,
    // not the pipeline tail.
    explicit Producer(uint64_t last_send_cycle)
        : TickableUnit("producer"), last_send_cycle_(last_send_cycle) {}
    OutPort<Msg> out{this, "out"};
    void tick() override {
        if (localCycle() >= last_send_cycle_) return;
        if (!out.canSend()) return;
        if (out.send(Msg{seq_})) ++seq_;
    }
    uint64_t sent() const { return seq_; }

private:
    uint64_t last_send_cycle_;
    uint64_t seq_ = 0;
};

class Consumer : public TickableUnit {
public:
    Consumer() : TickableUnit("consumer") {}
    InPort<Msg> in{this, "in", /*capacity=*/64};
    void tick() override {
        const uint64_t c = localCycle();
        while (auto m = in.tryReceive(c)) {
            received_.push_back(m->seq);
        }
    }
    const std::vector<uint64_t>& received() const { return received_; }

private:
    std::vector<uint64_t> received_;
};

constexpr uint64_t TARGET = 1500;
constexpr uint32_t NUM_FAST = 6;

int run_trial(size_t nw, uint32_t max_lookahead, int rep) {
    TickSimulationConfig cfg;
    cfg.num_threads = nw;
    cfg.enable_parallel = (nw > 1);
    cfg.enable_lookahead = true;
    cfg.max_lookahead_cycles = max_lookahead;
    cfg.epoch_size = 64;

    TickSimulation sim(cfg);

    auto* slow = sim.createUnit<SlowCounter>(0u);
    std::vector<Counter*> fast;
    for (uint32_t i = 0; i < NUM_FAST; ++i) {
        fast.push_back(sim.createUnit<Counter>(i + 1));
    }
    auto* prod = sim.createUnit<Producer>(TARGET - 1);  // last delivery lands at cycle TARGET-1
    auto* cons = sim.createUnit<Consumer>();
    sim.connect(prod->out, cons->in, 1);

    sim.initialize();
    sim.run(TARGET);  // would hang (and trip the CTest timeout) if the floor stalled

    int failures = 0;
    const std::string tag = "nw=" + std::to_string(nw) +
                            " max_la=" + std::to_string(max_lookahead) +
                            " rep=" + std::to_string(rep);

    if (slow->ticks() != TARGET) {
        std::cerr << "FAIL " << tag << ": slow ticked " << slow->ticks() << " != " << TARGET
                  << "\n";
        ++failures;
    }
    for (uint32_t i = 0; i < NUM_FAST; ++i) {
        if (fast[i]->ticks() != TARGET) {
            std::cerr << "FAIL " << tag << ": fast[" << i << "] ticked " << fast[i]->ticks()
                      << " != " << TARGET << "\n";
            ++failures;
        }
    }

    // Message accounting: every sent message must be received exactly once.
    const auto& rx = cons->received();
    uint64_t sent = prod->sent();
    if (rx.size() != sent) {
        std::cerr << "FAIL " << tag << ": sent=" << sent << " received=" << rx.size() << "\n";
        ++failures;
    }
    std::set<uint64_t> unique(rx.begin(), rx.end());
    if (unique.size() != rx.size()) {
        std::cerr << "FAIL " << tag << ": duplicate messages (" << rx.size() << " received, "
                  << unique.size() << " unique)\n";
        ++failures;
    }
    for (uint64_t s = 0; s < sent; ++s) {
        if (unique.find(s) == unique.end()) {
            std::cerr << "FAIL " << tag << ": missing seq=" << s << "\n";
            ++failures;
            break;
        }
    }

    std::cout << (failures ? "FAIL " : "ok   ") << tag << " slow_ticks=" << slow->ticks()
              << " sent=" << sent << " received=" << rx.size() << "\n";
    return failures;
}

// --------------------------------------------------------------------------
// White-box unit tests of refreshLookaheadFloor_ itself.
//
// Drives the private progress array directly on the main thread (via the
// befriended LookaheadFloorTestAccess) so each case is deterministic. Verifies
// the function's full contract: it raises lookahead_floor_ to the global
// minimum completed cycle, never lowers it (monotone), and is idempotent.
// --------------------------------------------------------------------------
int test_refresh_semantics() {
    using TA = chronon::sender::LookaheadFloorTestAccess;
    int failures = 0;

    auto check = [&](bool cond, const std::string& what) {
        if (!cond) {
            std::cerr << "FAIL unit: " << what << "\n";
            ++failures;
        }
    };

    TickSimulationConfig cfg;
    cfg.num_threads = 4;
    cfg.enable_parallel = true;
    cfg.enable_lookahead = true;
    cfg.enable_dynamic_rebalance = false;  // keep the cluster layout fixed
    cfg.max_lookahead_cycles = 8;
    cfg.epoch_size = 64;

    TickSimulation sim(cfg);
    // Independent units -> one cluster each -> a multi-entry progress array.
    constexpr size_t kUnits = 8;
    for (uint32_t i = 0; i < kUnits; ++i) {
        sim.createUnit<Counter>(i);
    }
    sim.initialize();  // allocates the private thread_progress_array_

    const size_t n = TA::count(sim);
    std::cout << "unit: progress array has " << n << " cluster slot(s)\n";
    if (n < 2) {
        std::cerr << "FAIL unit: expected >= 2 clusters to exercise the global-min scan, got " << n
                  << "\n";
        return failures + 1;
    }

    auto setAll = [&](uint64_t v) {
        for (size_t c = 0; c < n; ++c) TA::setProgress(sim, c, v);
    };

    // Case A: floor rises to the global min when the min sits at index 0.
    for (size_t c = 0; c < n; ++c) TA::setProgress(sim, c, 10 * (c + 1));  // min = 10 at c=0
    TA::setFloor(sim, 0);
    TA::refresh(sim);
    check(TA::floor(sim) == 10, "A: floor should rise to global min (10)");

    // Case B: min located at the last index is found.
    for (size_t c = 0; c < n; ++c) TA::setProgress(sim, c, 1000 - 5 * c);  // min at last index
    const uint64_t min_last = 1000 - 5 * (n - 1);
    TA::setFloor(sim, 0);
    TA::refresh(sim);
    check(TA::floor(sim) == min_last, "B: floor should equal min at last index");

    // Case C: min located at a middle index is found (needs >= 3 clusters).
    if (n >= 3) {
        setAll(1000);
        TA::setProgress(sim, n / 2, 250);  // unique minimum in the middle
        TA::setFloor(sim, 0);
        TA::refresh(sim);
        check(TA::floor(sim) == 250, "C: floor should equal min at middle index");
    }

    // Case D: monotone — refresh must NEVER lower an already-higher floor.
    TA::setFloor(sim, 500);
    setAll(100);  // global min 100 < floor 500
    TA::refresh(sim);
    check(TA::floor(sim) == 500, "D: floor must not be lowered below current value");

    // Case E: floor rises only up to the global min, not beyond.
    TA::setFloor(sim, 100);
    setAll(900);
    TA::setProgress(sim, 0, 300);  // global min 300 (a lagging cluster)
    TA::refresh(sim);
    check(TA::floor(sim) == 300, "E: floor should rise exactly to the lagging cluster's cycle");

    // Case F: idempotent — a second refresh with unchanged state is a no-op.
    TA::refresh(sim);
    check(TA::floor(sim) == 300, "F: repeated refresh is idempotent");

    // Case G: all-equal progress collapses to that value.
    TA::setFloor(sim, 0);
    setAll(777);
    TA::refresh(sim);
    check(TA::floor(sim) == 777, "G: equal progress -> floor equals the shared value");

    // Case H: after the laggard advances, a refresh lifts the floor to the new min.
    TA::setProgress(sim, 0, 12345);  // every slot now 777 except this one higher -> min still 777
    TA::refresh(sim);
    check(TA::floor(sim) == 777, "H: floor stays at min while one cluster races ahead");
    setAll(2000);  // laggard caught up; new global min 2000
    TA::refresh(sim);
    check(TA::floor(sim) == 2000, "H: floor lifts once the slowest cluster advances");

    std::cout << (failures ? "FAIL " : "ok   ") << "unit: refreshLookaheadFloor_ semantics\n";
    return failures;
}

}  // namespace

int main() {
    std::cout << "=== Lookahead floor: lazy-refresh progress under imbalanced layout ===\n\n";

    int failures = 0;

    // Part 1: direct white-box unit tests of refreshLookaheadFloor_.
    std::cout << "--- refreshLookaheadFloor_ unit tests ---\n";
    failures += test_refresh_semantics();
    std::cout << "\n--- end-to-end imbalanced sweep ---\n";

    // Part 2: end-to-end progress under an imbalanced layout near the ceiling.
    const std::vector<size_t> worker_counts = {1, 2, 4, 8};
    const std::vector<uint32_t> lookaheads = {4, 16};
    constexpr int REPEATS = 2;

    for (size_t nw : worker_counts) {
        for (uint32_t la : lookaheads) {
            for (int rep = 0; rep < REPEATS; ++rep) {
                failures += run_trial(nw, la, rep);
            }
        }
    }

    if (failures == 0) {
        std::cout << "\n=== Lookahead floor progress: ALL PASSED ===\n";
        return 0;
    }
    std::cerr << "\n=== Lookahead floor progress: " << failures << " failure(s) ===\n";
    return 1;
}
