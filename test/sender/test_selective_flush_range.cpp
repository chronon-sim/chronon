// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "EpochFreeDifferentialHarness.hpp"
#include "chronon/Chronon.hpp"

using namespace chronon;
using namespace chronon::sender::test;

namespace chronon::sender {

struct InPortSelectiveFlushTestAccess {
    template <typename T>
    static size_t activePredicateCount(const InPort<T>& port) noexcept {
        return port.selective_flush_state_.active_slot_count();
    }
};

}  // namespace chronon::sender

using chronon::sender::InPortSelectiveFlushTestAccess;

namespace {

[[noreturn]] void fail(const std::string& message) { throw std::runtime_error(message); }

void require(bool condition, const std::string& message) {
    if (!condition) fail(message);
}

class ManualUnit final : public Unit {
public:
    explicit ManualUnit(std::string name) : Unit(std::move(name)) {}

    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
};

struct TaggedMessage {
    uint64_t key = 0;
    uint64_t sequence = 0;
    uint64_t payload = 0;
    uint32_t source = 0;

    static uint64_t keyOf(const TaggedMessage& message) noexcept { return message.key; }
    static uint64_t sequenceOf(const TaggedMessage& message) noexcept { return message.sequence; }
};

constexpr uint64_t kMaxKey = std::numeric_limits<uint64_t>::max();

static_assert(FlushRange::youngerThan(uint64_t{0}).keeps(0));
static_assert(!FlushRange::youngerThan(uint64_t{0}).keeps(1));
static_assert(!FlushRange::atAndYounger(uint64_t{0}).keeps(0));
static_assert(FlushRange::olderThan(kMaxKey).keeps(kMaxKey));
static_assert(!FlushRange::olderThan(kMaxKey).keeps(kMaxKey - 1));
static_assert(!FlushRange::atAndOlder(kMaxKey).keeps(kMaxKey));
static_assert(FlushRange::outsideInclusive(uint64_t{0}, kMaxKey).keeps(0));
static_assert(FlushRange::outsideInclusive(uint64_t{0}, kMaxKey).keeps(kMaxKey));
static_assert(FlushRange::olderThan(uint64_t{2})
                  .intersectedWith(FlushRange::youngerThan(uint64_t{4}))
                  .keeps(3));
static_assert(!FlushRange::olderThan(uint64_t{2})
                   .intersectedWith(FlushRange::atAndYounger(uint64_t{2}))
                   .keeps(2));

std::vector<uint64_t> runBoundaryCase(PortPolicy policy, FlushRange range) {
    ManualUnit producer("producer");
    ManualUnit consumer("consumer");
    OutPort<TaggedMessage> out{&producer, "out", 8};
    InPort<TaggedMessage> in{&consumer, "in", 32, policy};
    out.connect(&in, 2);

    producer.setCycle(4);
    for (uint64_t key : {uint64_t{0}, uint64_t{1}, kMaxKey}) {
        require(out.send(TaggedMessage{.key = key}), "failed to seed boundary case");
    }

    consumer.setCycle(5);
    in.flush<&TaggedMessage::keyOf>(range);

    // The whole enqueue cycle is the deterministic boundary. A send stamped
    // with the flush cycle is post-flush even if wall-clock execution overlaps.
    producer.setCycle(5);
    require(out.send(TaggedMessage{.key = 7}), "failed to send post-flush boundary message");

    std::vector<uint64_t> keys;
    for (const auto& message : in.receiveAll(7)) keys.push_back(message.key);
    return keys;
}

void verifyFullWidthBoundariesAndPolicyParity() {
    struct Case {
        FlushRange range;
        std::vector<uint64_t> expected;
        const char* name;
    };
    const std::array cases{
        Case{FlushRange::youngerThan(uint64_t{0}), {0, 7}, "younger-than zero"},
        Case{FlushRange::atAndYounger(uint64_t{0}), {7}, "at-and-younger zero"},
        Case{FlushRange::olderThan(kMaxKey), {kMaxKey, 7}, "older-than max"},
        Case{FlushRange::atAndOlder(kMaxKey), {7}, "at-and-older max"},
        Case{FlushRange::outsideInclusive(uint64_t{1}, kMaxKey - 1), {1, 7}, "outside inclusive"},
    };

    for (const auto& test : cases) {
        const auto legacy = runBoundaryCase(PortPolicy::LegacyFastPath, test.range);
        const auto stage = runBoundaryCase(PortPolicy::StageSelective, test.range);
        require(legacy == test.expected, std::string(test.name) + " produced wrong keys");
        require(stage == legacy, std::string(test.name) + " differs by PortPolicy");
    }
}

std::vector<uint64_t> runOverlapCase(bool lower_first) {
    ManualUnit producer("producer");
    ManualUnit consumer("consumer");
    OutPort<TaggedMessage> out{&producer, "out", 8};
    InPort<TaggedMessage> in{&consumer, "in", 32};
    out.connect(&in, 2);

    producer.setCycle(4);
    for (uint64_t key = 0; key <= 6; ++key) {
        require(out.send(TaggedMessage{.key = key}), "failed to seed overlap case");
    }

    consumer.setCycle(5);
    if (lower_first) {
        in.flush<&TaggedMessage::keyOf>(FlushRange::olderThan(uint64_t{2}));
        in.resetSelectiveCancellation();
        in.flush<&TaggedMessage::keyOf>(FlushRange::youngerThan(uint64_t{4}));
    } else {
        in.flush<&TaggedMessage::keyOf>(FlushRange::youngerThan(uint64_t{4}));
        in.resetSelectiveCancellation();
        in.flush<&TaggedMessage::keyOf>(FlushRange::olderThan(uint64_t{2}));
    }
    producer.setCycle(5);
    require(out.send(TaggedMessage{.key = 0}), "failed to send post-flush overlap message");

    std::vector<uint64_t> keys;
    for (const auto& message : in.receiveAll(7)) keys.push_back(message.key);
    return keys;
}

void verifyOverlapIsMonotonicAndOrderIndependent() {
    const std::vector<uint64_t> expected{2, 3, 4, 0};
    require(runOverlapCase(true) == expected, "lower-first overlap changed keep intersection");
    require(runOverlapCase(false) == expected, "upper-first overlap changed keep intersection");
}

void verifyPredicateRetiresWhenDrainEmptiesQueue() {
    for (const PortPolicy policy : {PortPolicy::LegacyFastPath, PortPolicy::StageSelective}) {
        ManualUnit producer("producer");
        ManualUnit consumer("consumer");
        OutPort<TaggedMessage> out{&producer, "out", 4};
        InPort<TaggedMessage> in{&consumer, "in", 8, policy};
        out.connect(&in, 2);

        consumer.setCycle(5);
        in.flush<&TaggedMessage::keyOf>(FlushRange::youngerThan(uint64_t{50}));
        require(InPortSelectiveFlushTestAccess::activePredicateCount(in) == 1,
                "flush predicate was not installed");

        producer.setCycle(5);
        require(out.send(TaggedMessage{.key = 100}), "failed to enqueue single post-flush message");
        const auto single = in.tryReceive(7);
        require(single && single->key == 100, "single post-flush message did not survive");
        require(InPortSelectiveFlushTestAccess::activePredicateCount(in) == 0,
                "predicate survived when tryReceive drained the queue to empty");

        consumer.setCycle(10);
        in.flush<&TaggedMessage::keyOf>(FlushRange::youngerThan(uint64_t{50}));
        producer.setCycle(10);
        require(out.send(TaggedMessage{.key = 101}),
                "failed to enqueue receiveAll retirement message");
        const auto drained = in.receiveAll(12);
        require(drained.size() == 1 && drained.front().key == 101,
                "receiveAll lost the post-flush message");
        require(InPortSelectiveFlushTestAccess::activePredicateCount(in) == 0,
                "predicate survived when receiveAll drained the queue to empty");
    }
}

void verifyHeterogeneousDelayMpscRetirement() {
    ManualUnit slow("slow");
    ManualUnit fast("fast");
    ManualUnit consumer("consumer");
    OutPort<TaggedMessage> slow_out{&slow, "out", 4};
    OutPort<TaggedMessage> fast_out{&fast, "out", 4};
    InPort<TaggedMessage> in{&consumer, "in", 32};
    auto* slow_connection = slow_out.connect(&in, 4);
    auto* fast_connection = fast_out.connect(&in, 1);
    slow_connection->setConnId(10);
    fast_connection->setConnId(20);
    slow_connection->optimizeForMPSC();
    fast_connection->optimizeForMPSC();
    const size_t slow_lane = slow_connection->registerProducerThread(100);
    const size_t fast_lane = fast_connection->registerProducerThread(200);
    require(slow_lane != SIZE_MAX && fast_lane != SIZE_MAX && slow_lane != fast_lane,
            "failed to create heterogeneous MPSC lanes");
    slow_connection->setThreadQueueId(slow_lane);
    fast_connection->setThreadQueueId(fast_lane);
    require(slow_connection->registerOnDestMPSC() && fast_connection->registerOnDestMPSC(),
            "failed to register heterogeneous MPSC metadata");

    std::atomic<uint64_t> slow_progress{4};
    std::atomic<uint64_t> fast_progress{9};
    in.setProducerProgress({{&slow, &slow_progress}, {&fast, &fast_progress}});

    consumer.setCycle(5);
    in.flush<&TaggedMessage::keyOf>(FlushRange::youngerThan(uint64_t{50}));

    fast.setCycle(5);
    require(fast_out.send(TaggedMessage{.key = 100}), "failed to send fast post-flush message");
    fast.setCycle(8);
    require(fast_out.send(TaggedMessage{.key = 101}), "failed to seed future arrival frontier");

    const auto fast_post = in.tryReceive(6);
    require(fast_post && fast_post->key == 100,
            "post-flush short-delay message was canceled or reordered");

    // The visible head is now arrival 9, beyond the affected arrival bound 8.
    // The slow lane has not yet published its enqueue-cycle-4 payload, though,
    // so only its progress atomic can prove that this frontier is not stable.
    slow.setCycle(4);
    require(slow_out.send(TaggedMessage{.key = 99}), "failed to late-publish pre-flush message");
    slow_progress.store(5, std::memory_order_release);

    require(!in.tryReceive(8), "late pre-flush long-delay message escaped cancellation");
    const auto future_post = in.tryReceive(9);
    require(future_post && future_post->key == 101,
            "predicate did not retire after the stable arrival frontier");
}

uint64_t burn(uint64_t value, uint32_t iterations) noexcept {
    for (uint32_t i = 0; i < iterations; ++i) {
        value = value * 6364136223846793005ULL + 1442695040888963407ULL;
        value ^= value >> 29;
    }
    return value;
}

class TracedUnit : public TickableUnit {
public:
    TracedUnit(std::string name, uint32_t component, size_t reserve_events)
        : TickableUnit(std::move(name)), events_(component) {
        events_.reserve(reserve_events);
    }

    UnitEventLog& eventLog() noexcept { return events_; }

protected:
    void event(ModelEventKind kind, uint64_t value0 = 0, uint64_t value1 = 0) {
        events_.record(localCycle(), kind, value0, value1);
    }

private:
    UnitEventLog events_;
};

class RangeProducer final : public TracedUnit {
public:
    OutPort<TaggedMessage> out{this, "out", 1};

    RangeProducer(std::string name, uint32_t component, uint32_t source, uint32_t work,
                  size_t cycles)
        : TracedUnit(std::move(name), component, cycles * 3),
          source_(source),
          state_(0x9e3779b97f4a7c15ULL ^ source),
          work_(work) {}

    void tick() override {
        state_ = burn(state_ ^ localCycle(), work_);
        uint64_t key = (localCycle() * 7 + source_ * 5) % 17;
        if (localCycle() % 31 == 0) key = 0;
        if (localCycle() % 37 == 0) key = kMaxKey;
        const TaggedMessage message{
            .key = key, .sequence = sequence_, .payload = state_, .source = source_};
        const bool sent = out.send(message);
        event(ModelEventKind::SendResult, sequence_, sent);
        if (sent) ++sequence_;
        event(ModelEventKind::State, sequence_, state_);
    }

private:
    uint64_t sequence_ = 0;
    uint32_t source_ = 0;
    uint64_t state_ = 0;
    uint32_t work_ = 0;
};

class RangeReceiver final : public TracedUnit {
public:
    InPort<TaggedMessage> fanin{this, "fanin", 256};
    InPort<TaggedMessage> spsc{this, "spsc", 256};

    explicit RangeReceiver(size_t cycles) : TracedUnit("receiver", 4, cycles * 12) {}

    void tick() override {
        applyFlushes_();
        drain_(fanin, 0);
        drain_(spsc, 1);
        event(ModelEventKind::State, received_, state_);
    }

private:
    void applyFlushes_() {
        switch (localCycle()) {
            case 16:
                flushBoth_(FlushRange::youngerThan(uint64_t{5}), 1, 5);
                break;
            case 24:
                flushBoth_(FlushRange::atAndYounger(uint64_t{0}), 2, 0);
                break;
            case 32:
                flushBoth_(FlushRange::olderThan(uint64_t{12}), 3, 12);
                break;
            case 40:
                flushBoth_(FlushRange::atAndOlder(uint64_t{16}), 4, 16);
                break;
            case 48:
                fanin.flush<&TaggedMessage::keyOf>(
                    FlushRange::outsideInclusive(uint64_t{4}, uint64_t{10}));
                spsc.flush<&TaggedMessage::keyOf>(
                    FlushRange::outsideInclusive(uint64_t{4}, uint64_t{10}));
                event(ModelEventKind::Flush, 5, (uint64_t{4} << 32) | 10);
                break;
            case 56:
                // Same-cycle calls must intersect independent of call order;
                // the compatibility reset between them cannot erase state.
                fanin.flush<&TaggedMessage::keyOf>(FlushRange::olderThan(uint64_t{2}));
                fanin.resetSelectiveCancellation();
                fanin.flush<&TaggedMessage::keyOf>(FlushRange::youngerThan(uint64_t{12}));
                spsc.flush<&TaggedMessage::keyOf>(FlushRange::youngerThan(uint64_t{12}));
                spsc.resetSelectiveCancellation();
                spsc.flush<&TaggedMessage::keyOf>(FlushRange::olderThan(uint64_t{2}));
                event(ModelEventKind::Flush, 6, (uint64_t{2} << 32) | 12);
                break;
            case 58:
                // A second live predicate overlaps the cycle-56 scope.
                flushBoth_(FlushRange::atAndYounger(uint64_t{8}), 7, 8);
                break;
            default:
                break;
        }
    }

    void flushBoth_(FlushRange range, uint64_t kind, uint64_t boundary) {
        fanin.flush<&TaggedMessage::keyOf>(range);
        spsc.flush<&TaggedMessage::keyOf>(range);
        event(ModelEventKind::Flush, kind, boundary);
    }

    void drain_(InPort<TaggedMessage>& port, uint64_t port_id) {
        while (auto message = port.tryReceive(localCycle())) {
            const uint64_t identity = (static_cast<uint64_t>(message->source) << 56) |
                                      (message->sequence & 0x00ffffffffffffffULL);
            event(ModelEventKind::Receive, (port_id << 63) | identity, message->key);
            state_ = burn(state_ ^ message->payload ^ message->key ^ identity, 5);
            ++received_;
        }
    }

    uint64_t received_ = 0;
    uint64_t state_ = 0x123456789abcdef0ULL;
};

class LoadUnit final : public TracedUnit {
public:
    LoadUnit(std::string name, uint32_t component, uint32_t work, size_t cycles)
        : TracedUnit(std::move(name), component, cycles), work_(work) {}

    void tick() override {
        state_ = burn(state_ ^ localCycle(), work_);
        event(ModelEventKind::State, state_, work_);
    }

private:
    uint64_t state_ = 0xfeedfacecafebeefULL;
    uint32_t work_ = 0;
};

class FlushMatrixScenario {
public:
    FlushMatrixScenario(TickSimulation& simulation, size_t cycles) {
        producers_[0] = simulation.createUnit<RangeProducer>("producer_d1", 0, 0, 350, cycles);
        producers_[1] = simulation.createUnit<RangeProducer>("producer_d2", 1, 1, 7, cycles);
        producers_[2] = simulation.createUnit<RangeProducer>("producer_d4", 2, 2, 11, cycles);
        producers_[3] = simulation.createUnit<RangeProducer>("producer_spsc", 3, 3, 5, cycles);
        receiver_ = simulation.createUnit<RangeReceiver>(cycles);

        simulation.connect(producers_[0]->out, receiver_->fanin, 1);
        simulation.connect(producers_[1]->out, receiver_->fanin, 2);
        simulation.connect(producers_[2]->out, receiver_->fanin, 4);
        simulation.connect(producers_[3]->out, receiver_->spsc, 2);

        loads_[0] = simulation.createUnit<LoadUnit>("load_0", 5, 800, cycles);
        loads_[1] = simulation.createUnit<LoadUnit>("load_1", 6, 400, cycles);
        loads_[2] = simulation.createUnit<LoadUnit>("load_2", 7, 23, cycles);
        loads_[3] = simulation.createUnit<LoadUnit>("load_3", 8, 3, cycles);

        logs_ = {&producers_[0]->eventLog(), &producers_[1]->eventLog(), &producers_[2]->eventLog(),
                 &producers_[3]->eventLog(), &receiver_->eventLog(),     &loads_[0]->eventLog(),
                 &loads_[1]->eventLog(),     &loads_[2]->eventLog(),     &loads_[3]->eventLog()};
    }

    std::vector<std::string> componentNames() const {
        return {"producer_d1", "producer_d2", "producer_d4", "producer_spsc", "receiver",
                "load_0",      "load_1",      "load_2",      "load_3"};
    }

    std::vector<CanonicalEvent> canonicalEvents() const {
        return canonicalizeEvents(std::span<UnitEventLog* const>(logs_.data(), logs_.size()));
    }

private:
    std::array<RangeProducer*, 4> producers_{};
    RangeReceiver* receiver_ = nullptr;
    std::array<LoadUnit*, 4> loads_{};
    std::array<UnitEventLog*, 9> logs_{};
};

void verifyEpochFreeFlushMatrix() {
    constexpr uint64_t kCycles = 192;
    TickSimulationConfig base;
    base.max_lookahead_cycles = 8;
    base.enable_weighted_partitioning = true;
    base.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    base.initial_partition_sync_cost_ns = 0.0;
    base.rebalance_check_interval_cycles = 16;
    base.rebalance_imbalance_threshold = 1.01;
    base.rebalance_min_gain = 0.0;
    base.rebalance_cooldown_cycles = 0;

    std::vector<EpochFreeRunMode> modes;
    modes.push_back({.name = "sequential-reference",
                     .kind = EpochFreeRunKind::SequentialReference,
                     .num_threads = 1,
                     .migrations = {}});
    for (size_t workers = 2; workers <= 8; ++workers) {
        modes.push_back({.name = "epoch-free-static-" + std::to_string(workers),
                         .kind = EpochFreeRunKind::Static,
                         .num_threads = workers,
                         .migrations = {}});
    }
    modes.push_back({.name = "epoch-free-forced-8",
                     .kind = EpochFreeRunKind::ForcedMigration,
                     .num_threads = 8,
                     .migrations = {{.cycle = 31, .unit_name = "receiver"},
                                    {.cycle = 33, .unit_name = "receiver"},
                                    {.cycle = 37, .unit_name = "receiver"}}});
    modes.push_back({.name = "epoch-free-runtime-4",
                     .kind = EpochFreeRunKind::RuntimeRebalance,
                     .num_threads = 4,
                     .migrations = {}});

    auto artifacts =
        runEpochFreeMatrix(base, kCycles, std::span<const EpochFreeRunMode>(modes),
                           [=](TickSimulation& simulation, const EpochFreeRunMode&) {
                               return std::make_unique<FlushMatrixScenario>(simulation, kCycles);
                           });
    const auto comparison = compareMatrix(artifacts);
    require(comparison.equivalent, comparison.diagnostic);
    require(artifacts.size() == modes.size(), "flush matrix omitted a configured mode");

    for (const auto& artifact : artifacts) {
        if (artifact.mode_name == "sequential-reference") {
            require(artifact.epoch_free_runs == 0, "reference unexpectedly used epoch-free mode");
        } else {
            require(artifact.epoch_free_runs > 0,
                    artifact.mode_name + " fell back from epoch-free lookahead");
        }
        if (artifact.mode_name == "epoch-free-forced-8") {
            require(artifact.forced_migrations_applied == 3,
                    "forced mode did not migrate before, during, and after a live flush");
        }
        // Runtime rebalance is intentionally heuristic: retaining the current
        // ownership is valid when the sampled gain does not justify a move.
        // The forced mode above provides deterministic migration coverage.
        std::cout << "  " << artifact.mode_name << ": digest=" << artifact.digest
                  << " events=" << artifact.events.size()
                  << " rebalances=" << artifact.rebalance_count << '\n';
    }
}

}  // namespace

int main() {
    try {
        std::cout << "=== Selective FlushRange contract ===\n";
        verifyFullWidthBoundariesAndPolicyParity();
        verifyOverlapIsMonotonicAndOrderIndependent();
        verifyPredicateRetiresWhenDrainEmptiesQueue();
        verifyHeterogeneousDelayMpscRetirement();
        verifyEpochFreeFlushMatrix();
        std::cout << "Selective FlushRange tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
