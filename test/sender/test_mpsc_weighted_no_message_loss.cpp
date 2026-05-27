// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

// test_mpsc_weighted_no_message_loss.cpp
//
// Verifies that the default weighted partitioning path (uniform-cost initial
// partition + dynamic rebalance) does not lose messages in an MPSC fan-in
// topology. Backpressure may prevent producers from sending at full rate —
// that is correct behavior. The invariant is: every successfully sent message
// must be received by the consumer.
//
// Topology: 3 producers → 1 consumer, delay=1, InPort capacity=64.
// Sweep: num_workers in {1, 2, 3, 4, 6, 8} × 3 repeats.

#include <array>
#include <cstdint>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/Port.hpp"

using namespace chronon::sender;

namespace {

struct Msg {
    uint32_t producer_id;
    uint64_t sequence_number;
};

class Producer : public TickableUnit {
public:
    Producer(uint32_t id, uint64_t total_ticks)
        : TickableUnit("producer_" + std::to_string(id)), id_(id), total_ticks_(total_ticks) {}

    OutPort<Msg> out{this, "out"};

    void tick() override {
        if (ticks_ >= total_ticks_) return;
        for (int i = 0; i < 2; ++i) {
            if (!out.canSend()) break;
            if (!out.send(Msg{id_, seq_})) break;
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
            if (m->producer_id < received_seqs_.size()) {
                received_seqs_[m->producer_id].push_back(m->sequence_number);
            }
            ++received_;
        }
    }

    uint64_t received() const { return received_; }
    const std::array<std::vector<uint64_t>, 3>& receivedSeqs() const { return received_seqs_; }

private:
    uint64_t received_ = 0;
    std::array<std::vector<uint64_t>, 3> received_seqs_;
};

}  // namespace

int main() {
    std::cout << "=== MPSC Weighted Path: No Message Loss ===\n\n";

    constexpr uint64_t PRODUCER_TICKS = 100;
    const std::vector<size_t> worker_counts = {1, 2, 3, 4, 6, 8};
    constexpr int REPEATS = 3;
    int failures = 0;

    for (size_t nw : worker_counts) {
        for (int rep = 0; rep < REPEATS; ++rep) {
            TickSimulationConfig cfg;
            cfg.num_threads = nw;
            cfg.enable_parallel = (nw > 1);
            cfg.enable_lookahead = true;
            cfg.epoch_size = 64;
            // Default: enable_weighted_partitioning = true (the path under test).

            TickSimulation sim(cfg);

            auto* p0 = sim.createUnit<Producer>(0u, PRODUCER_TICKS);
            auto* p1 = sim.createUnit<Producer>(1u, PRODUCER_TICKS);
            auto* p2 = sim.createUnit<Producer>(2u, PRODUCER_TICKS);
            auto* cons = sim.createUnit<Consumer>();

            sim.connect(p0->out, cons->in, 1);
            sim.connect(p1->out, cons->in, 1);
            sim.connect(p2->out, cons->in, 1);

            sim.initialize();
            sim.run(PRODUCER_TICKS + 64);

            std::array<uint64_t, 3> sent = {p0->sent(), p1->sent(), p2->sent()};
            uint64_t total_sent = sent[0] + sent[1] + sent[2];
            uint64_t total_received = cons->received();
            const auto& seqs = cons->receivedSeqs();

            std::cout << "nw=" << nw << " rep=" << rep << " sent=" << total_sent
                      << " received=" << total_received << " [" << sent[0] << "," << sent[1] << ","
                      << sent[2] << "]\n";

            if (total_received != total_sent) {
                std::cerr << "FAIL nw=" << nw << " rep=" << rep << ": sent=" << total_sent
                          << " received=" << total_received << " (delta "
                          << static_cast<int64_t>(total_received) - static_cast<int64_t>(total_sent)
                          << ")\n";
                ++failures;
            }

            for (uint32_t pid = 0; pid < 3; ++pid) {
                if (seqs[pid].size() != sent[pid]) {
                    std::cerr << "FAIL nw=" << nw << " rep=" << rep << " pid=" << pid
                              << ": sent=" << sent[pid] << " received_seqs=" << seqs[pid].size()
                              << "\n";
                    ++failures;
                    continue;
                }
                std::set<uint64_t> unique(seqs[pid].begin(), seqs[pid].end());
                if (unique.size() != seqs[pid].size()) {
                    std::cerr << "FAIL nw=" << nw << " rep=" << rep << " pid=" << pid
                              << ": duplicates (" << seqs[pid].size() << " received, "
                              << unique.size() << " unique)\n";
                    ++failures;
                }
                for (uint64_t s = 0; s < sent[pid]; ++s) {
                    if (unique.find(s) == unique.end()) {
                        std::cerr << "FAIL nw=" << nw << " rep=" << rep << " pid=" << pid
                                  << ": missing seq=" << s << "\n";
                        ++failures;
                        break;
                    }
                }
            }
        }
    }

    if (failures == 0) {
        std::cout << "\n=== No message loss: ALL PASSED ===\n";
        return 0;
    }
    std::cerr << "\n=== No message loss: " << failures << " failure(s) ===\n";
    return 1;
}
