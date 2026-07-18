// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

// Differential correctness contract for the epoch-free scheduler. The same
// model is rebuilt for a sequential reference, several static worker counts,
// deterministic safe-point migrations, and runtime-driven rebalance.

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "EpochFreeDifferentialHarness.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/Port.hpp"

using namespace chronon::sender;
using namespace chronon::sender::test;

namespace {

constexpr uint32_t kProducerA = 0;
constexpr uint32_t kProducerB = 1;
constexpr uint32_t kMerger = 2;
constexpr uint32_t kFeedback = 3;
constexpr uint32_t kFlushProducer = 4;
constexpr uint32_t kFlushConsumer = 5;
constexpr uint32_t kReliableProducer = 6;
constexpr uint32_t kBackpressureConsumer = 7;
constexpr uint32_t kLevelSource = 8;
constexpr uint32_t kSleeper = 9;
constexpr size_t kComponentCount = 10;

[[noreturn]] void fail(const std::string& message) { throw std::runtime_error(message); }

void require(bool condition, const std::string& message) {
    if (!condition) fail(message);
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

struct Packet {
    uint64_t sequence = 0;
    uint64_t payload = 0;
    uint32_t source = 0;
};

class PacketProducer final : public TracedUnit {
public:
    OutPort<Packet> out{this, "out", 1};

    PacketProducer(std::string name, uint32_t component, uint32_t source, uint64_t seed,
                   uint32_t work, size_t cycles)
        : TracedUnit(std::move(name), component, cycles * 3),
          source_(source),
          state_(seed),
          work_(work) {}

    void tick() override {
        state_ = burn(state_ ^ localCycle(), work_);
        const Packet packet{.sequence = next_sequence_, .payload = state_, .source = source_};
        const bool sent = out.send(packet);
        event(ModelEventKind::SendResult, next_sequence_, sent);
        if (sent) ++next_sequence_;
        event(ModelEventKind::State, next_sequence_, state_);
    }

private:
    uint32_t source_ = 0;
    uint64_t next_sequence_ = 0;
    uint64_t state_ = 0;
    uint32_t work_ = 0;
};

class PacketMerger final : public TracedUnit {
public:
    InPort<Packet> in{this, "in", 128};
    OutPort<Packet> out{this, "out", 1};

    explicit PacketMerger(size_t cycles) : TracedUnit("merger", kMerger, cycles * 8) {}

    void tick() override {
        uint64_t receive_order = 0;
        while (auto packet = in.tryReceive(localCycle())) {
            const uint64_t identity = (static_cast<uint64_t>(packet->source) << 56) |
                                      (packet->sequence & 0x00ffffffffffffffULL);
            event(ModelEventKind::Receive, identity, receive_order++);
            state_ = burn(state_ ^ packet->payload ^ identity, 7);
        }

        const Packet packet{.sequence = next_sequence_, .payload = state_, .source = 2};
        const bool sent = out.send(packet);
        event(ModelEventKind::SendResult, next_sequence_, sent);
        if (sent) ++next_sequence_;
        event(ModelEventKind::State, next_sequence_, state_);
    }

private:
    uint64_t next_sequence_ = 0;
    uint64_t state_ = 0x123456789abcdef0ULL;
};

class FeedbackUnit final : public TracedUnit {
public:
    InPort<Packet> in{this, "in", 64};
    OutPort<Packet> out{this, "out", 1};

    explicit FeedbackUnit(size_t cycles) : TracedUnit("feedback", kFeedback, cycles * 6) {}

    void tick() override {
        while (auto packet = in.tryReceive(localCycle())) {
            event(ModelEventKind::Receive, packet->sequence, packet->payload);
            state_ ^= burn(packet->payload + packet->sequence, 500);
        }
        const Packet response{.sequence = next_sequence_, .payload = state_, .source = 3};
        const bool sent = out.send(response);
        event(ModelEventKind::SendResult, next_sequence_, sent);
        if (sent) ++next_sequence_;
        event(ModelEventKind::State, next_sequence_, state_);
    }

private:
    uint64_t next_sequence_ = 0;
    uint64_t state_ = 0x0fedcba987654321ULL;
};

struct TaggedMessage {
    uint64_t key = 0;
    uint64_t payload = 0;

    static uint64_t keyOf(const TaggedMessage& message) noexcept { return message.key; }
};

class FlushProducer final : public TracedUnit {
public:
    OutPort<TaggedMessage> out{this, "out", 1};

    explicit FlushProducer(size_t cycles)
        : TracedUnit("flush_producer", kFlushProducer, cycles * 3) {}

    void tick() override {
        const TaggedMessage message{.key = next_key_, .payload = burn(next_key_ + 17, 11)};
        const bool sent = out.send(message);
        event(ModelEventKind::SendResult, next_key_, sent);
        if (sent) ++next_key_;
        event(ModelEventKind::State, next_key_, message.payload);
    }

private:
    uint64_t next_key_ = 0;
};

class FlushConsumer final : public TracedUnit {
public:
    InPort<TaggedMessage> in{this, "in", 64, PortPolicy::StageSelective};

    explicit FlushConsumer(size_t cycles)
        : TracedUnit("flush_consumer", kFlushConsumer, cycles * 5) {}

    void tick() override {
        if (localCycle() == 10) {
            in.flush<&TaggedMessage::keyOf>(FlushRange::outsideInclusive(uint64_t{4}, uint64_t{6}));
            event(ModelEventKind::Flush, 4, 6);
        }
        if (localCycle() == 12) {
            in.flush<&TaggedMessage::keyOf>(FlushRange::youngerThan(uint64_t{8}));
            event(ModelEventKind::Flush, 0, 8);
        }

        while (auto message = in.tryReceive(localCycle())) {
            event(ModelEventKind::Receive, message->key, message->payload);
            state_ = burn(state_ ^ message->payload ^ message->key, 5);
            ++received_;
        }
        event(ModelEventKind::State, received_, state_);
    }

private:
    uint64_t received_ = 0;
    uint64_t state_ = 0x55aa55aa55aa55aaULL;
};

class ReliableProducer final : public TracedUnit {
public:
    OutPort<uint64_t> out{this, "out", 1};

    explicit ReliableProducer(size_t cycles)
        : TracedUnit("reliable_producer", kReliableProducer, cycles * 4) {}

    void tick() override {
        if (!pending_) pending_ = next_value_;
        const bool sent = out.send(*pending_);
        event(ModelEventKind::SendResult, *pending_, sent);
        if (!sent) {
            event(ModelEventKind::Backpressure, *pending_);
        } else {
            ++next_value_;
            pending_.reset();
        }
        event(ModelEventKind::State, next_value_, pending_.has_value());
    }

private:
    uint64_t next_value_ = 0;
    std::optional<uint64_t> pending_;
};

class BackpressureConsumer final : public TracedUnit {
public:
    InPort<uint64_t> in{this, "in", 2};

    explicit BackpressureConsumer(size_t cycles)
        : TracedUnit("backpressure_consumer", kBackpressureConsumer, cycles * 4) {}

    void tick() override {
        if ((localCycle() & 3U) == 0) {
            while (auto value = in.tryReceive(localCycle())) {
                event(ModelEventKind::Receive, *value, received_);
                state_ = burn(state_ ^ *value, 3);
                ++received_;
            }
        }
        event(ModelEventKind::State, received_, state_);
    }

private:
    uint64_t received_ = 0;
    uint64_t state_ = 0x3141592653589793ULL;
};

struct LevelMessage {
    uint64_t generation = 0;
    bool asserted = false;
};

class LevelSource final : public TracedUnit {
public:
    OutPort<LevelMessage> out{this, "out", 1};

    explicit LevelSource(size_t cycles) : TracedUnit("level_source", kLevelSource, cycles * 3) {}

    void tick() override {
        if (localCycle() % 7 == 2) {
            const LevelMessage message{.generation = generation_, .asserted = asserted_};
            const bool sent = out.send(message);
            event(ModelEventKind::SendResult, generation_, sent);
            if (sent) {
                ++generation_;
                asserted_ = !asserted_;
            }
        }
        event(ModelEventKind::State, generation_, asserted_);
    }

private:
    uint64_t generation_ = 0;
    bool asserted_ = true;
};

class SleepingLevelConsumer final : public TracedUnit {
public:
    InPort<LevelMessage> in{this, "in", 16};

    explicit SleepingLevelConsumer(size_t cycles) : TracedUnit("sleeper", kSleeper, cycles) {
        enableActivityScheduling();
    }

    void tick() override {
        event(ModelEventKind::Wake, generation_, asserted_);
        while (auto message = in.tryReceive(localCycle())) {
            generation_ = message->generation;
            asserted_ = message->asserted;
            event(ModelEventKind::Signal, generation_, asserted_);
        }
        sleepForever();
    }

private:
    uint64_t generation_ = 0;
    bool asserted_ = false;
};

class EquivalenceScenario {
public:
    EquivalenceScenario(TickSimulation& simulation, size_t cycles) {
        producer_a_ =
            simulation.createUnit<PacketProducer>("producer_a", kProducerA, 0, 11, 600, cycles);
        producer_b_ =
            simulation.createUnit<PacketProducer>("producer_b", kProducerB, 1, 22, 19, cycles);
        merger_ = simulation.createUnit<PacketMerger>(cycles);
        feedback_ = simulation.createUnit<FeedbackUnit>(cycles);
        flush_producer_ = simulation.createUnit<FlushProducer>(cycles);
        flush_consumer_ = simulation.createUnit<FlushConsumer>(cycles);
        reliable_producer_ = simulation.createUnit<ReliableProducer>(cycles);
        backpressure_consumer_ = simulation.createUnit<BackpressureConsumer>(cycles);
        level_source_ = simulation.createUnit<LevelSource>(cycles);
        sleeper_ = simulation.createUnit<SleepingLevelConsumer>(cycles);

        simulation.connect(producer_a_->out, merger_->in, 1);
        simulation.connect(producer_b_->out, merger_->in, 4);
        simulation.connect(merger_->out, feedback_->in, 0);
        simulation.connect(feedback_->out, merger_->in, 2);
        simulation.connect(flush_producer_->out, flush_consumer_->in, 6);
        simulation.connect(reliable_producer_->out, backpressure_consumer_->in, 1);
        simulation.connect(level_source_->out, sleeper_->in, 3);

        logs_ = {&producer_a_->eventLog(),
                 &producer_b_->eventLog(),
                 &merger_->eventLog(),
                 &feedback_->eventLog(),
                 &flush_producer_->eventLog(),
                 &flush_consumer_->eventLog(),
                 &reliable_producer_->eventLog(),
                 &backpressure_consumer_->eventLog(),
                 &level_source_->eventLog(),
                 &sleeper_->eventLog()};
    }

    std::vector<std::string> componentNames() const {
        return {"producer_a",
                "producer_b",
                "merger",
                "feedback",
                "flush_producer",
                "flush_consumer",
                "reliable_producer",
                "backpressure_consumer",
                "level_source",
                "sleeper"};
    }

    std::vector<CanonicalEvent> canonicalEvents() const {
        return canonicalizeEvents(std::span<UnitEventLog* const>(logs_.data(), logs_.size()));
    }

private:
    PacketProducer* producer_a_ = nullptr;
    PacketProducer* producer_b_ = nullptr;
    PacketMerger* merger_ = nullptr;
    FeedbackUnit* feedback_ = nullptr;
    FlushProducer* flush_producer_ = nullptr;
    FlushConsumer* flush_consumer_ = nullptr;
    ReliableProducer* reliable_producer_ = nullptr;
    BackpressureConsumer* backpressure_consumer_ = nullptr;
    LevelSource* level_source_ = nullptr;
    SleepingLevelConsumer* sleeper_ = nullptr;
    std::array<UnitEventLog*, kComponentCount> logs_{};
};

void verifyFirstDivergenceDiagnostic() {
    RunArtifact reference;
    reference.mode_name = "reference";
    reference.component_names = {"router"};
    reference.executed_cycles = 32;
    reference.events.push_back(
        CanonicalEvent{.cycle = 17, .value0 = 41, .component = 0, .kind = ModelEventKind::Receive});

    RunArtifact candidate = reference;
    candidate.mode_name = "epoch-free-2";
    candidate.events.front().value0 = 42;

    const auto comparison = compareArtifacts(reference, candidate);
    require(!comparison.equivalent, "synthetic mismatch must be detected");
    require(comparison.first_divergent_cycle == 17,
            "diagnostic must identify the first divergent cycle");
    require(comparison.diagnostic.find("router") != std::string::npos,
            "diagnostic must identify the divergent component");
    require(comparison.diagnostic.find("expected=") != std::string::npos &&
                comparison.diagnostic.find("actual=") != std::string::npos,
            "diagnostic must include expected and actual events");
}

void verifyEpochFreeMatrix() {
    constexpr uint64_t kCycles = 4096;

    TickSimulationConfig base;
    base.max_lookahead_cycles = 16;
    base.enable_weighted_partitioning = true;
    base.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    base.initial_partition_sync_cost_ns = 0.0;
    base.rebalance_check_interval_cycles = 128;
    base.rebalance_imbalance_threshold = 1.01;
    base.rebalance_min_gain = 0.0;
    base.rebalance_cooldown_cycles = 0;

    const std::vector<EpochFreeRunMode> modes{
        {.name = "sequential-reference",
         .kind = EpochFreeRunKind::SequentialReference,
         .num_threads = 1,
         .migrations = {}},
        {.name = "epoch-free-static-2",
         .kind = EpochFreeRunKind::Static,
         .num_threads = 2,
         .migrations = {}},
        {.name = "epoch-free-static-3",
         .kind = EpochFreeRunKind::Static,
         .num_threads = 3,
         .migrations = {}},
        {.name = "epoch-free-static-4",
         .kind = EpochFreeRunKind::Static,
         .num_threads = 4,
         .migrations = {}},
        {.name = "epoch-free-forced-3",
         .kind = EpochFreeRunKind::ForcedMigration,
         .num_threads = 3,
         .migrations = {{.cycle = 11, .unit_name = "flush_consumer"},
                        {.cycle = 23, .unit_name = "merger"},
                        {.cycle = 37, .unit_name = "backpressure_consumer"},
                        {.cycle = 53, .unit_name = "sleeper"}}},
        {.name = "epoch-free-runtime-3",
         .kind = EpochFreeRunKind::RuntimeRebalance,
         .num_threads = 3,
         .migrations = {}},
    };

    auto artifacts =
        runEpochFreeMatrix(base, kCycles, std::span<const EpochFreeRunMode>(modes),
                           [=](TickSimulation& simulation, const EpochFreeRunMode&) {
                               return std::make_unique<EquivalenceScenario>(simulation, kCycles);
                           });

    const auto comparison = compareMatrix(artifacts);
    require(comparison.equivalent, comparison.diagnostic);
    require(artifacts.size() == modes.size(), "every matrix mode must produce an artifact");

    for (const auto& artifact : artifacts) {
        if (artifact.mode_name == "sequential-reference") {
            require(artifact.epoch_free_runs == 0,
                    "sequential reference must not report an epoch-free invocation");
        } else {
            require(artifact.epoch_free_runs > 0,
                    artifact.mode_name + " must execute only through epoch-free lookahead");
        }
        if (artifact.mode_name == "epoch-free-forced-3") {
            require(artifact.forced_migrations_applied == 4,
                    "forced mode must apply every declared migration");
            require(artifact.rebalance_count >= 4,
                    "forced migrations must update scheduler migration accounting");
        }
        if (artifact.mode_name == "epoch-free-runtime-3") {
            require(artifact.rebalance_count > 0,
                    "runtime mode must exercise an actual epoch-free rebalance");
        }
        std::cout << "  " << artifact.mode_name << ": digest=" << artifact.digest
                  << " events=" << artifact.events.size()
                  << " epoch_free_runs=" << artifact.epoch_free_runs
                  << " rebalances=" << artifact.rebalance_count << '\n';
    }
}

}  // namespace

int main() {
    try {
        std::cout << "=== Epoch-free scheduler equivalence ===\n";
        verifyFirstDivergenceDiagnostic();
        verifyEpochFreeMatrix();
        std::cout << "Epoch-free scheduler equivalence tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
