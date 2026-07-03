// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "chronon/Chronon.hpp"
#include "observe/ObservationManager.hpp"
#include "observe/ObservationYAMLConfig.hpp"

using namespace chronon::sender;
namespace observe = chronon::observe;

namespace {

class SourceUnit : public TickableUnit {
public:
    explicit SourceUnit(std::string name, int value)
        : TickableUnit(std::move(name)), value_(value) {}

    OutPort<int> out{this, "out"};

    void tick() override {
        if (!sent_) {
            send_succeeded_ = out.send(value_);
            sent_ = true;
        }
    }

    bool sendSucceeded() const noexcept { return send_succeeded_; }

private:
    int value_ = 0;
    bool sent_ = false;
    bool send_succeeded_ = false;
};

class RelayUnit : public TickableUnit {
public:
    explicit RelayUnit(std::string name) : TickableUnit(std::move(name)) {}

    InPort<int> in{this, "in", 256};
    OutPort<int> out{this, "out"};

    void tick() override {
        while (in.tryReceive(localCycle()).has_value()) {
            // Drain input to avoid queue growth in tight-coupling links.
        }
    }
};

class SinkUnit : public TickableUnit {
public:
    explicit SinkUnit(std::string name) : TickableUnit(std::move(name)) {}

    InPort<int> in{this, "in", 8192};

    void tick() override {
        while (auto value = in.tryReceive(localCycle())) {
            received_.push_back(*value);
        }
    }

    const std::vector<int>& received() const noexcept { return received_; }

private:
    std::vector<int> received_;
};

class ManualUnit : public Unit {
public:
    explicit ManualUnit(std::string name) : Unit(std::move(name)) {}

    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
};

struct AllocationProbe {
    static inline bool fail_on_default = false;

    AllocationProbe() {
        if (fail_on_default) {
            throw std::runtime_error("unexpected staging ring allocation");
        }
    }
};

class IdleMPSCReceiverUnit : public TickableUnit {
public:
    explicit IdleMPSCReceiverUnit(std::string name) : TickableUnit(std::move(name)) {}

    InPort<int> in{this, "in", 8};

    void tick() override {}
};

class ZeroSlackFeedbackUnit : public TickableUnit {
public:
    explicit ZeroSlackFeedbackUnit(std::string name) : TickableUnit(std::move(name)) {}

    InPort<int> peer_in{this, "peer_in", 1};
    OutPort<int> peer_out{this, "peer_out", 1};
    OutPort<int> local_out{this, "local_out"};

    void tick() override {
        while (peer_in.tryReceive(localCycle()).has_value()) {
        }
        [[maybe_unused]] bool peer_sent = peer_out.send(static_cast<int>(localCycle()));
        [[maybe_unused]] bool local_sent = local_out.send(static_cast<int>(localCycle()));
        ++ticks_;
    }

    uint64_t ticks() const noexcept { return ticks_; }

private:
    uint64_t ticks_ = 0;
};

class FeedbackUnit : public TickableUnit {
public:
    explicit FeedbackUnit(std::string name, uint64_t seed)
        : TickableUnit(std::move(name)), seed_(seed) {}

    InPort<uint64_t> in{this, "in", 64};
    OutPort<uint64_t> out{this, "out", 1};

    void tick() override {
        while (auto value = in.tryReceive(localCycle())) {
            state_ ^= (*value + seed_) * 1000003ULL;
        }
        (void)out.send(state_ ^ localCycle());
    }

private:
    uint64_t seed_ = 0;
    uint64_t state_ = 1;
};

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::string readTextFile(const std::filesystem::path& path) {
    std::ifstream file(path);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

size_t countOccurrences(const std::string& text, const std::string& needle) {
    size_t count = 0;
    size_t pos = 0;
    while ((pos = text.find(needle, pos)) != std::string::npos) {
        ++count;
        pos += needle.size();
    }
    return count;
}

SourceUnit* createSmallNonTightFallbackGraph(TickSimulation& sim) {
    auto* src = sim.createUnit<SourceUnit>("src", 7);
    auto* relay0 = sim.createUnit<RelayUnit>("relay0");
    auto* relay1 = sim.createUnit<RelayUnit>("relay1");
    auto* relay2 = sim.createUnit<RelayUnit>("relay2");
    auto* relay3 = sim.createUnit<RelayUnit>("relay3");
    auto* relay4 = sim.createUnit<RelayUnit>("relay4");
    auto* relay5 = sim.createUnit<RelayUnit>("relay5");
    auto* relay6 = sim.createUnit<RelayUnit>("relay6");
    auto* sink = sim.createUnit<SinkUnit>("sink");

    // No tight edges: all connections have delay=1.
    sim.connect(src->out, relay0->in, 1);
    sim.connect(src->out, relay1->in, 1);
    sim.connect(src->out, relay2->in, 1);
    sim.connect(src->out, relay3->in, 1);
    sim.connect(src->out, relay4->in, 1);
    sim.connect(src->out, relay5->in, 1);
    sim.connect(src->out, relay6->in, 1);
    sim.connect(relay0->out, sink->in, 1);
    sim.connect(relay1->out, sink->in, 1);
    sim.connect(relay2->out, sink->in, 1);
    sim.connect(relay3->out, sink->in, 1);
    sim.connect(relay4->out, sink->in, 1);
    sim.connect(relay5->out, sink->in, 1);
    sim.connect(relay6->out, sink->in, 1);
    sim.connect(src->out, sink->in, 1);

    return src;
}

void createMixedDelayFeedbackGraph(TickSimulation& sim) {
    auto* a = sim.createUnit<FeedbackUnit>("A", 11);
    auto* b = sim.createUnit<FeedbackUnit>("B", 22);
    auto* c = sim.createUnit<FeedbackUnit>("C", 33);
    auto* d = sim.createUnit<FeedbackUnit>("D", 44);

    sim.connect(a->out, c->in, 1);
    sim.connect(b->out, c->in, 3);
    sim.connect(c->out, d->in, 1);
    sim.connect(d->out, a->in, 1);
    sim.connect(d->out, b->in, 1);
}

void test_tick_simulation_mpsc_delivery() {
    std::cout << "Testing TickSimulation MPSC delivery... ";

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    // Consumer-tick-driven MPSC arbitration drains staging inside each
    // receiver tick; end-of-epoch flush via arbitrateAllMPSCPorts_
    // handles any tail entries not picked up this epoch.
    config.epoch_size = 4;

    TickSimulation sim(config);

    auto* src0 = sim.createUnit<SourceUnit>("src0", 100);
    auto* src1 = sim.createUnit<SourceUnit>("src1", 200);

    // Build two tight-coupling clusters so sources are assigned to different
    // threads and share one MPSC destination.
    auto* a0 = sim.createUnit<RelayUnit>("a0");
    auto* b0 = sim.createUnit<RelayUnit>("b0");
    auto* a1 = sim.createUnit<RelayUnit>("a1");
    auto* sink = sim.createUnit<SinkUnit>("sink");

    // Tight (delay=0) connections for cluster shaping.
    sim.connect(src0->out, a0->in, 0);
    sim.connect(a0->out, b0->in, 0);
    sim.connect(src1->out, a1->in, 0);

    // Two producers to one destination port (MPSC).
    sim.connect(src0->out, sink->in, 1);
    sim.connect(src1->out, sink->in, 1);

    sim.initialize();
    sim.run(2);

    assert(src0->sendSucceeded());
    assert(src1->sendSucceeded());

    std::vector<int> received = sink->received();
    std::sort(received.begin(), received.end());
    assert(received.size() == 2);
    assert(received[0] == 100);
    assert(received[1] == 200);

    std::cout << "PASSED\n";
}

void test_mpsc_staging_tracks_large_user_capacity() {
    std::cout << "Testing MPSC staging tracks large user capacity... ";

    constexpr size_t kUserCapacity = 8192;

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", kUserCapacity};
    InPort<int> in{&cons, "in", kUserCapacity};
    auto* conn = out.connect(&in, 1);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    prod.setCycle(0);

    size_t sent = 0;
    for (size_t i = 0; i < kUserCapacity; ++i) {
        require(out.canSend(), "canSend() rejected before user capacity");
        require(out.send(static_cast<int>(i)), "send() rejected before user capacity");
        ++sent;
    }

    require(sent == kUserCapacity, "did not fill to declared user capacity");
    require(!out.canSend(), "canSend() accepted beyond user capacity");
    require(!out.send(99999), "send() accepted beyond user capacity");

    in.arbitrateMPSC();
    size_t received = 0;
    while (auto value = in.tryReceive(1)) {
        require(*value == static_cast<int>(received), "MPSC delivery order changed");
        ++received;
    }
    require(received == kUserCapacity,
            "downstream MPSC queue did not admit all same-cycle entries");

    std::cout << "PASSED\n";
}

void test_registered_mpsc_capacity_sizes_destination_queue() {
    std::cout << "Testing registered MPSC capacity sizes destination queue... ";

    constexpr size_t kRegisteredCapacity = 8192;

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", kRegisteredCapacity};
    InPort<int> in{&cons, "in"};
    auto* conn = out.connect(&in, 1);
    conn->configureRegisteredEdge(kRegisteredCapacity, std::nullopt);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    prod.setCycle(0);

    for (size_t i = 0; i < kRegisteredCapacity; ++i) {
        require(out.canSend(), "registered MPSC capacity rejected before edge capacity");
        require(out.send(static_cast<int>(i)), "registered MPSC send failed before edge capacity");
    }
    require(!out.canSend(), "registered MPSC capacity accepted beyond edge capacity");
    require(!out.send(99999), "registered MPSC send accepted beyond edge capacity");

    in.arbitrateMPSC();
    size_t received = 0;
    while (auto value = in.tryReceive(1)) {
        require(*value == static_cast<int>(received), "registered MPSC delivery order changed");
        ++received;
    }
    require(received == kRegisteredCapacity,
            "registered MPSC destination queue did not admit declared capacity");

    std::cout << "PASSED\n";
}

void test_mpsc_epoch_free_headroom_respects_user_capacity() {
    std::cout << "Testing MPSC epoch-free headroom respects user capacity... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", 1};
    InPort<int> in{&cons, "in", 1};
    auto* conn = out.connect(&in, 1);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    require(conn->crossThreadHeadroom() == 1,
            "MPSC headroom ignored the bounded staging admission capacity");
    require(conn->ensureEpochFreeHeadroom(8), "MPSC headroom rejected bounded zero-slack capacity");

    std::cout << "PASSED\n";
}

void test_mpsc_epoch_free_headroom_sizes_destination_queue() {
    std::cout << "Testing MPSC headroom sizes destination queue... ";

    constexpr size_t kSourceRate = 5000;

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", kSourceRate};
    InPort<int> in{&cons, "in"};
    auto* conn = out.connect(&in, 1);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    require(conn->crossThreadHeadroom() == 0,
            "high-rate MPSC edge unexpectedly had enough default destination headroom");
    require(conn->ensureEpochFreeHeadroom(2),
            "MPSC headroom did not grow destination and staging rings together");
    require(conn->crossThreadHeadroom() > 1,
            "MPSC headroom still insufficient after destination ring growth");

    prod.setCycle(0);
    for (size_t i = 0; i < kSourceRate; ++i) {
        require(out.canSend(), "high-rate MPSC edge rejected before source rate");
        require(out.send(static_cast<int>(i)), "high-rate MPSC send failed after headroom growth");
    }

    in.arbitrateMPSC();
    size_t received = 0;
    while (auto value = in.tryReceive(1)) {
        require(*value == static_cast<int>(received), "grown MPSC destination order changed");
        ++received;
    }
    require(received == kSourceRate, "grown MPSC destination did not admit source-rate burst");

    std::cout << "PASSED\n";
}

void test_zero_slack_feedback_falls_back_to_barrier() {
    std::cout << "Testing zero-slack feedback falls back to barrier... ";

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_epoch_free_lookahead = true;
    config.enable_weighted_partitioning = false;
    config.max_lookahead_cycles = 16;
    config.epoch_size = 4;

    TickSimulation sim(config);

    auto* a = sim.createUnit<ZeroSlackFeedbackUnit>("a");
    auto* a1 = sim.createUnit<RelayUnit>("a1");
    auto* a2 = sim.createUnit<RelayUnit>("a2");
    auto* b = sim.createUnit<ZeroSlackFeedbackUnit>("b");
    auto* b1 = sim.createUnit<RelayUnit>("b1");
    auto* b2 = sim.createUnit<RelayUnit>("b2");

    // Two tight intra-cluster chains force two size-3 clusters. With two
    // threads, the bidirectional delay-1/capacity-1 links must cross threads.
    sim.connect(a->local_out, a1->in, 0);
    sim.connect(a1->out, a2->in, 0);
    sim.connect(b->local_out, b1->in, 0);
    sim.connect(b1->out, b2->in, 0);
    sim.connect(a->peer_out, b->peer_in, 1);
    sim.connect(b->peer_out, a->peer_in, 1);

    sim.initialize();
    require(sim.useParallelExecution(), "zero-slack feedback test did not enter parallel mode");

    sim.run(8);

    require(sim.epochFreeRunCount() == 0,
            "zero-slack feedback cycle should veto epoch-free lookahead");
    require(a->ticks() == 8 && b->ticks() == 8,
            "barrier fallback did not execute zero-slack feedback units");

    std::cout << "PASSED\n";
}

void test_registered_capacity_only_uses_source_rate_for_headroom() {
    std::cout << "Testing registered capacity-only headroom uses source rate... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", 4};
    InPort<int> in{&cons, "in", 1};
    auto* conn = out.connect(&in, 1);
    conn->configureRegisteredEdge(/*capacity=*/1, /*rate=*/std::nullopt);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    require(conn->crossThreadHeadroom() == 0,
            "capacity-only edge was incorrectly treated as rate-1 headroom");
    require(!conn->ensureEpochFreeHeadroom(8),
            "capacity-only bounded edge accepted unproven epoch-free headroom");

    std::cout << "PASSED\n";
}

void test_registered_capacity_only_does_not_throttle_rate() {
    std::cout << "Testing registered capacity-only does not throttle rate... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", 4};
    InPort<int> in{&cons, "in", 8};
    auto* conn = out.connect(&in, 1);
    conn->configureRegisteredEdge(/*capacity=*/8, /*rate=*/std::nullopt);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    prod.setCycle(0);
    for (int value = 0; value < 4; ++value) {
        require(out.canSend(), "capacity-only edge throttled below source rate");
        require(out.send(value), "capacity-only edge blocked below source rate");
    }
    require(!out.canSend(), "source rate did not cap capacity-only edge");
    require(!out.send(4), "source rate allowed extra capacity-only send");

    in.arbitrateMPSC();
    for (int value = 0; value < 4; ++value) {
        auto received = in.tryReceive(1);
        require(received.has_value() && *received == value, "capacity-only edge delivery changed");
    }
    require(!in.tryReceive(1).has_value(), "capacity-only edge delivered extra data");

    std::cout << "PASSED\n";
}

void test_registered_edge_rate_throttles_mpsc_pushes() {
    std::cout << "Testing registered edge rate throttles MPSC pushes... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", 4};
    InPort<int> in{&cons, "in", 8};
    auto* conn = out.connect(&in, 1);
    conn->configureRegisteredEdge(/*capacity=*/8, /*rate=*/1);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    prod.setCycle(0);
    require(out.canSend(), "registered rate rejected first MPSC send");
    require(out.send(1), "registered rate blocked first MPSC send");
    require(!out.canSend(), "registered rate allowed second same-cycle MPSC preflight");
    require(!out.send(2), "registered rate allowed second same-cycle MPSC send");

    in.arbitrateMPSC();
    auto first = in.tryReceive(1);
    require(first.has_value() && *first == 1, "MPSC registered-rate delivery changed");
    require(!in.tryReceive(1).has_value(), "MPSC registered-rate throttle delivered extra data");

    prod.setCycle(1);
    require(out.canSend(), "registered rate did not reset on producer cycle advance");
    require(out.send(3), "registered rate blocked next-cycle MPSC send");

    std::cout << "PASSED\n";
}

void test_registered_edge_rate_throttles_spsc_pushes() {
    std::cout << "Testing registered edge rate throttles SPSC pushes... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", 4};
    InPort<int> in{&cons, "in", 8};
    auto* conn = out.connect(&in, 1);
    conn->configureRegisteredEdge(/*capacity=*/8, /*rate=*/1);

    conn->optimizeForSPSC();

    prod.setCycle(0);
    require(out.canSend(), "registered rate rejected first SPSC send");
    require(out.send(1), "registered rate blocked first SPSC send");
    require(!out.canSend(), "registered rate allowed second same-cycle SPSC preflight");
    require(!out.send(2), "registered rate allowed second same-cycle SPSC send");

    auto first = in.tryReceive(1);
    require(first.has_value() && *first == 1, "SPSC registered-rate delivery changed");
    require(!in.tryReceive(1).has_value(), "SPSC registered-rate throttle delivered extra data");

    prod.setCycle(1);
    require(out.canSend(), "registered rate did not reset on producer cycle advance");
    require(out.send(3), "registered rate blocked next-cycle SPSC send");

    std::cout << "PASSED\n";
}

void test_bounded_mpsc_epoch_free_headroom_skips_resize() {
    std::cout << "Testing bounded MPSC headroom skips impossible resize... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<AllocationProbe> out{&prod, "out", 1};
    InPort<AllocationProbe> in{&cons, "in", 1};
    auto* conn = out.connect(&in, 1);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    AllocationProbe::fail_on_default = true;
    try {
        require(conn->ensureEpochFreeHeadroom(4096),
                "bounded MPSC headroom should not resize zero-slack capacity");
    } catch (...) {
        AllocationProbe::fail_on_default = false;
        throw;
    }
    AllocationProbe::fail_on_default = false;

    std::cout << "PASSED\n";
}

void test_spsc_epoch_free_headroom_respects_registered_capacity() {
    std::cout << "Testing SPSC epoch-free headroom respects registered capacity... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", 1};
    InPort<int> in{&cons, "in", 1};
    auto* conn = out.connect(&in, 1);

    conn->optimizeForSPSC();

    require(conn->crossThreadHeadroom() == 1,
            "SPSC headroom ignored the bounded registered edge capacity");
    require(conn->ensureEpochFreeHeadroom(8), "SPSC headroom rejected bounded zero-slack capacity");

    std::cout << "PASSED\n";
}

void test_idle_advance_drains_mpsc_staging() {
    std::cout << "Testing idle advance drains MPSC staging... ";

    ManualUnit prod("prod");
    IdleMPSCReceiverUnit cons("cons");

    OutPort<int> out{&prod, "out"};
    auto* conn = out.connect(&cons.in, 1);

    conn->optimizeForMPSC();
    const size_t queue_id = conn->registerProducerThread(/*thread_id=*/0);
    require(queue_id != SIZE_MAX, "MPSC producer registration failed");
    conn->setThreadQueueId(queue_id);
    require(conn->registerOnDestMPSC() != nullptr, "MPSC destination registration failed");

    prod.setCycle(0);
    require(out.send(42), "MPSC send failed before idle drain");
    require(cons.in.queuedMessageCount() == 0, "message bypassed MPSC staging unexpectedly");

    cons.advanceIdleTick(3);

    require(cons.localCycle() == 3, "idle advance did not update receiver cycle");
    require(cons.in.queuedMessageCount() == 1, "idle advance did not drain MPSC staging");
    auto value = cons.in.tryReceive(3);
    require(value.has_value(), "drained MPSC message was not visible after idle advance");
    require(*value == 42, "drained MPSC message payload changed");

    std::cout << "PASSED\n";
}

void test_lockfree_backpressure_contract() {
    std::cout << "Testing lock-free backpressure contract... ";

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out"};
    InPort<int> in{&cons, "in", 8};
    out.connect(&in, 1);

    in.useLockFreeQueue();
    prod.setCycle(0);

    [[maybe_unused]] size_t sent = 0;
    [[maybe_unused]] bool send_failed_while_can_send = false;
    for (size_t i = 0; i < 10000; ++i) {
        if (!out.canSend()) {
            break;
        }
        if (!out.send(static_cast<int>(i))) {
            send_failed_while_can_send = true;
            break;
        }
        sent++;
    }

    assert(sent > 0);
    assert(!send_failed_while_can_send);
    assert(!out.canSend());
    assert(in.queuedMessageCount() == sent);

    // Destination should now reject additional traffic until consumed.
    // Since LockFreeQueueAdapter::setCapacity now honors the declared
    // bound physically, this push actually fails at the ring level —
    // not just at the canSend() snapshot check.
    assert(!out.send(99999));

    // Drain one entry and cross the cycle boundary. The per-connection
    // "pushes this cycle" counter clears on a cycle advance, matching
    // what TickableUnit::executeTick does at each clock edge.
    [[maybe_unused]] auto v = in.tryReceive(1);
    assert(v.has_value());
    prod.setCycle(1);
    assert(out.canSend());

    std::cout << "PASSED\n";
}

void test_spsc_user_capacity_backpressures_across_cycles() {
    std::cout << "Testing SPSC user capacity backpressures across cycles... ";

    constexpr size_t kRate = 8;
    constexpr size_t kDepth = 16;

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", kRate};
    InPort<int> in{&cons, "in", kDepth};
    auto* conn = out.connect(&in, 1);
    conn->optimizeForSPSC();

    size_t value = 0;
    for (uint64_t cycle = 0; cycle < 2; ++cycle) {
        prod.setCycle(cycle);
        for (size_t lane = 0; lane < kRate; ++lane) {
            require(out.canSend(), "SPSC capacity rejected before destination depth");
            require(out.send(static_cast<int>(value++)),
                    "SPSC send failed before destination depth");
        }
        require(!out.canSend(), "SPSC rate allowed an extra same-cycle send");
    }

    require(in.queuedMessageCount() == kDepth, "SPSC queue did not fill to declared depth");

    prod.setCycle(2);
    require(!out.canSend(), "SPSC capacity allowed send beyond destination depth");
    require(!out.send(99999), "SPSC send succeeded beyond destination depth");
    require(in.queuedMessageCount() == kDepth, "failed SPSC send changed queue depth");

    auto first = in.tryReceive(2);
    require(first.has_value() && *first == 0, "SPSC drain changed delivery order");
    require(in.queuedMessageCount() == kDepth - 1, "SPSC drain did not free one slot");

    require(!out.canSend(), "SPSC capacity reopened after same-cycle drain");
    require(!out.send(99998), "SPSC send succeeded after same-cycle drain");
    require(in.queuedMessageCount() == kDepth - 1,
            "failed same-cycle SPSC refill changed queue depth");

    prod.setCycle(3);
    require(out.canSend(), "SPSC capacity did not reopen after next-cycle drain credit");
    require(out.send(static_cast<int>(value)), "SPSC send failed after next-cycle drain credit");
    require(in.queuedMessageCount() == kDepth, "SPSC queue did not refill to declared depth");

    std::cout << "PASSED\n";
}

void test_spsc_capacity_ignores_same_cycle_consumer_pop_interleaving() {
    std::cout << "Testing SPSC capacity ignores same-cycle consumer pop interleaving... ";

    constexpr size_t kRate = 2;
    constexpr size_t kDepth = 2;

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", kRate};
    InPort<int> in{&cons, "in", kDepth};
    auto* conn = out.connect(&in, 1);
    conn->optimizeForSPSC();

    prod.setCycle(0);
    require(out.send(1), "SPSC failed to fill first slot");
    require(out.send(2), "SPSC failed to fill second slot");
    require(in.queuedMessageCount() == kDepth, "SPSC queue did not fill to declared depth");

    auto first = in.tryReceive(1);
    require(first.has_value() && *first == 1, "SPSC drain changed delivery order");
    require(in.queuedMessageCount() == kDepth - 1, "SPSC drain did not remove one entry");

    prod.setCycle(1);
    require(!out.canSend(), "SPSC capacity used same-cycle consumer pop for admission");
    require(!out.send(3), "SPSC send succeeded after same-cycle consumer pop");

    prod.setCycle(2);
    require(out.canSend(), "SPSC capacity did not release prior-cycle consumer pop");
    require(out.send(3), "SPSC send failed after prior-cycle consumer pop");

    std::cout << "PASSED\n";
}

void test_spsc_capacity_receive_all_returns_prior_cycle_credit() {
    std::cout << "Testing SPSC capacity receiveAll returns prior-cycle credit... ";

    constexpr size_t kRate = 2;
    constexpr size_t kDepth = 2;

    ManualUnit prod("prod");
    ManualUnit cons("cons");

    OutPort<int> out{&prod, "out", kRate};
    InPort<int> in{&cons, "in", kDepth};
    auto* conn = out.connect(&in, 1);
    conn->optimizeForSPSC();

    prod.setCycle(0);
    require(out.send(1), "SPSC failed to fill first slot before receiveAll");
    require(out.send(2), "SPSC failed to fill second slot before receiveAll");

    std::vector<int> drained = in.receiveAll(1);
    require(drained.size() == kDepth, "receiveAll did not drain ready SPSC messages");

    prod.setCycle(1);
    require(!out.canSend(), "SPSC receiveAll pop reopened capacity in the same cycle");

    prod.setCycle(2);
    require(out.canSend(), "SPSC receiveAll pop did not release prior-cycle credit");
    require(out.send(3), "SPSC send failed after receiveAll prior-cycle credit");
    require(out.send(4), "SPSC second send failed after receiveAll prior-cycle credit");

    std::cout << "PASSED\n";
}

void test_small_non_tight_graph_parallel_fallback() {
    std::cout << "Testing small non-tight graph parallel fallback... ";

    TickSimulationConfig config;
    config.num_threads = 4;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_weighted_partitioning = false;
    config.epoch_size = 8;

    TickSimulation sim(config);
    auto* src = createSmallNonTightFallbackGraph(sim);

    sim.initialize();

    assert(!sim.hasTightConnectionsInGraph());
    assert(!sim.isParallelBeneficial());
    assert(!sim.useParallelExecution());

    sim.run(2);
    require(src->sendSucceeded(), "source send failed after sequential fallback");

    std::cout << "PASSED\n";
}

void test_parallel_fallback_warns_via_observe() {
    std::cout << "Testing parallel fallback emits observe warning... ";

    auto& obs_mgr = observe::ObservationManager::instance();
    obs_mgr.reset();

    const std::filesystem::path out_dir =
        std::filesystem::temp_directory_path() /
        ("chronon_parallel_fallback_warn_" + std::to_string(getpid()));
    std::filesystem::remove_all(out_dir);

    observe::ObservationYAMLConfig obs_config;
    obs_config.enabled = true;
    obs_config.output_dir = out_dir.string();
    obs_config.counters.enabled = false;
    obs_config.counters.csv_output = false;
    obs_config.timeline.enabled = false;
    obs_config.unified_logging.enabled = true;
    obs_config.unified_logging.info_channel.enabled = false;
    obs_config.unified_logging.warn_channel.enabled = true;
    obs_config.unified_logging.warn_channel.file = "warn.log";
    obs_mgr.initialize(obs_config);

    TickSimulationConfig config;
    config.num_threads = 4;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_weighted_partitioning = false;
    config.epoch_size = 8;

    std::filesystem::path warn_log;
    {
        TickSimulation sim(config);
        createSmallNonTightFallbackGraph(sim);

        sim.initialize();
        assert(!sim.useParallelExecution());

        obs_mgr.startBackend();
        warn_log = obs_mgr.backend()->outputDir() / "warn.log";
        obs_mgr.stopBackend();
    }

    const std::string content = readTextFile(warn_log);
    require(content.find("[ WARN] simulation: parallel execution requested but falling back to "
                         "sequential") != std::string::npos,
            "parallel fallback warning was not written through observe");
    require(content.find("units=9") != std::string::npos,
            "parallel fallback warning omitted unit count");
    require(content.find("num_threads=4") != std::string::npos,
            "parallel fallback warning omitted thread count");
    require(countOccurrences(content, "falling back to sequential") == 1,
            "parallel fallback warning emitted more than once");

    obs_mgr.reset();
    std::filesystem::remove_all(out_dir);

    std::cout << "PASSED\n";
}

void test_deprecated_epoch_fallback_warns_via_observe() {
    std::cout << "Testing deprecated epoch fallback emits observe warning... ";

    auto& obs_mgr = observe::ObservationManager::instance();
    obs_mgr.reset();

    const std::filesystem::path out_dir =
        std::filesystem::temp_directory_path() /
        ("chronon_deprecated_epoch_fallback_warn_" + std::to_string(getpid()));
    std::filesystem::remove_all(out_dir);

    observe::ObservationYAMLConfig obs_config;
    obs_config.enabled = true;
    obs_config.output_dir = out_dir.string();
    obs_config.counters.enabled = false;
    obs_config.counters.csv_output = false;
    obs_config.timeline.enabled = false;
    obs_config.unified_logging.enabled = true;
    obs_config.unified_logging.info_channel.enabled = false;
    obs_config.unified_logging.warn_channel.enabled = true;
    obs_config.unified_logging.warn_channel.file = "warn.log";
    obs_mgr.initialize(obs_config);

    TickSimulationConfig config;
    config.num_threads = 2;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_epoch_free_lookahead = false;
    config.max_lookahead_cycles = 8;
    config.epoch_size = 8;

    std::filesystem::path warn_log;
    {
        TickSimulation sim(config);
        createMixedDelayFeedbackGraph(sim);

        sim.initialize();
        require(sim.useParallelExecution(), "deprecated fallback test did not enter parallel mode");

        obs_mgr.startBackend();
        warn_log = obs_mgr.backend()->outputDir() / "warn.log";
        sim.run(8);
        obs_mgr.stopBackend();
    }

    const std::string content = readTextFile(warn_log);
    require(content.find("[ WARN] simulation: DEPRECATED: per-epoch lookahead fallback") !=
                std::string::npos,
            "deprecated epoch fallback warning was not written through observe");
    require(content.find("reason=enable_epoch_free_lookahead=false") != std::string::npos,
            "deprecated epoch fallback warning omitted veto reason");
    require(countOccurrences(content, "per-epoch lookahead fallback") == 1,
            "deprecated epoch fallback warning emitted more than once");

    obs_mgr.reset();
    std::filesystem::remove_all(out_dir);

    std::cout << "PASSED\n";
}

}  // namespace

int main() {
    std::cout << "=== Thread Queue Hardening Tests ===\n\n";

    test_tick_simulation_mpsc_delivery();
    test_mpsc_staging_tracks_large_user_capacity();
    test_registered_mpsc_capacity_sizes_destination_queue();
    test_mpsc_epoch_free_headroom_respects_user_capacity();
    test_mpsc_epoch_free_headroom_sizes_destination_queue();
    test_zero_slack_feedback_falls_back_to_barrier();
    test_registered_capacity_only_uses_source_rate_for_headroom();
    test_registered_capacity_only_does_not_throttle_rate();
    test_registered_edge_rate_throttles_mpsc_pushes();
    test_registered_edge_rate_throttles_spsc_pushes();
    test_bounded_mpsc_epoch_free_headroom_skips_resize();
    test_spsc_epoch_free_headroom_respects_registered_capacity();
    test_idle_advance_drains_mpsc_staging();
    test_lockfree_backpressure_contract();
    test_spsc_user_capacity_backpressures_across_cycles();
    test_spsc_capacity_ignores_same_cycle_consumer_pop_interleaving();
    test_spsc_capacity_receive_all_returns_prior_cycle_credit();
    test_small_non_tight_graph_parallel_fallback();
    test_parallel_fallback_warns_via_observe();
    test_deprecated_epoch_fallback_warns_via_observe();

    std::cout << "\n=== Thread queue hardening tests PASSED ===\n";
    return 0;
}
