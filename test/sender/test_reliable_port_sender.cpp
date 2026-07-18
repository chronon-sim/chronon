// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "EpochFreeDifferentialHarness.hpp"
#include "chronon/Chronon.hpp"

namespace {

using chronon::FlushRange;
using chronon::InPort;
using chronon::OutPort;
using chronon::ReliablePortSender;
using chronon::TickableUnit;
using chronon::TickSimulation;
using chronon::TickSimulationConfig;
using chronon::Unit;
using chronon::sender::test::canonicalizeEvents;
using chronon::sender::test::compareMatrix;
using chronon::sender::test::EpochFreeRunKind;
using chronon::sender::test::EpochFreeRunMode;
using chronon::sender::test::ModelEventKind;
using chronon::sender::test::runEpochFreeMatrix;
using chronon::sender::test::UnitEventLog;

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

class ManualUnit final : public Unit {
public:
    explicit ManualUnit(std::string name) : Unit(std::move(name)) {}

    void setCycle(uint64_t cycle) { setLocalCycle(cycle); }
};

struct MoveOnlyMessage {
    MoveOnlyMessage() = default;
    explicit MoveOnlyMessage(int value_in) : value(value_in) {}

    MoveOnlyMessage(const MoveOnlyMessage&) = delete;
    MoveOnlyMessage& operator=(const MoveOnlyMessage&) = delete;

    MoveOnlyMessage(MoveOnlyMessage&& other) noexcept : value(std::exchange(other.value, -1)) {}
    MoveOnlyMessage& operator=(MoveOnlyMessage&& other) noexcept {
        value = std::exchange(other.value, -1);
        return *this;
    }

    int value = 0;
};

struct TaggedMessage {
    uint64_t key = 0;
    uint64_t payload = 0;
};

static_assert(alignof(ReliablePortSender<int>) >= 64);
static_assert(sizeof(ReliablePortSender<int>) % 64 == 0);
static_assert(ReliablePortSender<int>::pendingCapacity() == 1);

void testMoveOnlyPayloadIsRetainedExactlyOnce() {
    ManualUnit producer("producer");
    ManualUnit filler("filler");
    ManualUnit consumer("consumer");
    OutPort<MoveOnlyMessage> out{&producer, "out", 1};
    OutPort<MoveOnlyMessage> fill{&filler, "fill", 1};
    InPort<MoveOnlyMessage> in{&consumer, "in", 1};
    out.connect(&in, 1);
    fill.connect(&in, 1);
    ReliablePortSender reliable{out};

    producer.setCycle(0);
    filler.setCycle(0);
    require(fill.send(MoveOnlyMessage{90}), "failed to fill bounded destination");
    require(!reliable.send(MoveOnlyMessage{7}), "backpressured reliable send unexpectedly passed");
    require(reliable.pending() && reliable.pendingCount() == 1,
            "failed reliable send did not retain exactly one payload");
    require(reliable.pendingSinceCycle() == 0, "pending cycle was not retained");

    MoveOnlyMessage same_cycle_argument{99};
    require(!reliable.send(std::move(same_cycle_argument)),
            "reliable sender retried twice on one producer edge");
    require(same_cycle_argument.value == 99,
            "arguments were consumed while an older reliable payload was pending");

    auto filler_value = in.tryReceive(1);
    require(filler_value && filler_value->value == 90, "failed to drain destination filler");

    producer.setCycle(1);
    MoveOnlyMessage ignored_retry_argument{123};
    require(reliable.send(std::move(ignored_retry_argument)),
            "retained reliable payload did not retry after capacity returned");
    require(ignored_retry_argument.value == 123,
            "retry consumed the caller's replacement argument instead of retained state");
    require(!reliable.pending(), "successful retry did not release the pending slot");

    auto delivered = in.tryReceive(2);
    require(delivered && delivered->value == 7, "retry delivered the wrong move-only payload");
    require(!in.tryReceive(3), "retry duplicated a previously delivered payload");
}

void testBoundedOverflowAndDiagnosticsFailLoudly() {
    ManualUnit producer("diagnostic_producer");
    ManualUnit filler("diagnostic_filler");
    ManualUnit consumer("diagnostic_consumer");
    OutPort<int> out{&producer, "architectural_completion", 1};
    OutPort<int> fill{&filler, "fill", 1};
    InPort<int> in{&consumer, "bounded_sink", 1};
    out.connect(&in, 1);
    fill.connect(&in, 1);
    ReliablePortSender reliable{out};

    producer.setCycle(17);
    filler.setCycle(17);
    require(fill.send(1), "failed to fill diagnostic destination");
    require(!reliable.submit(2), "diagnostic request did not become pending");

    bool overflow_reported = false;
    try {
        (void)reliable.submit(3);
    } catch (const std::logic_error& error) {
        const std::string diagnostic = error.what();
        overflow_reported = diagnostic.find("diagnostic_producer") != std::string::npos &&
                            diagnostic.find("architectural_completion") != std::string::npos &&
                            diagnostic.find("payload_types") != std::string::npos &&
                            diagnostic.find("pending_since_cycle=17") != std::string::npos;
    }
    require(overflow_reported, "pending-slot overflow omitted unit/port/payload diagnostics");

    require(reliable.cancelPending(), "explicit cancellation did not release pending storage");
    bool empty_retry_reported = false;
    try {
        (void)reliable.retry();
    } catch (const std::logic_error& error) {
        empty_retry_reported =
            std::string(error.what()).find("without a pending payload") != std::string::npos;
    }
    require(empty_retry_reported, "retry without a request did not fail loudly");
}

void testFanoutRetryPublishesAllOrNone() {
    ManualUnit producer("fanout_producer");
    ManualUnit filler("fanout_filler");
    ManualUnit sink_a("sink_a");
    ManualUnit sink_b("sink_b");
    OutPort<int> out{&producer, "fanout", 1};
    OutPort<int> fill{&filler, "fill", 1};
    InPort<int> in_a{&sink_a, "in_a", 2};
    InPort<int> in_b{&sink_b, "in_b", 1};
    out.connect(&in_a, 1);
    out.connect(&in_b, 1);
    fill.connect(&in_b, 1);
    ReliablePortSender reliable{out};

    producer.setCycle(0);
    filler.setCycle(0);
    require(fill.send(5), "failed to fill one fanout destination");
    require(!reliable.send(42), "partially blocked fanout did not retain its payload");
    require(!in_a.tryReceive(1), "failed fanout became visible at an unblocked destination");
    auto filler_value = in_b.tryReceive(1);
    require(filler_value && *filler_value == 5, "failed to drain fanout blocker");

    producer.setCycle(1);
    require(reliable.send(999), "fanout retry did not publish after capacity returned");
    const auto a = in_a.tryReceive(2);
    const auto b = in_b.tryReceive(2);
    require(a && b && *a == 42 && *b == 42,
            "fanout retry did not publish its original payload atomically");
    require(!in_a.tryReceive(3) && !in_b.tryReceive(3), "fanout retry duplicated delivery");
}

void testMultiPortRetryUsesAtomicTransaction() {
    ManualUnit producer("transaction_producer");
    ManualUnit filler("transaction_filler");
    ManualUnit sink_a("transaction_sink_a");
    ManualUnit sink_b("transaction_sink_b");
    OutPort<int> out_a{&producer, "out_a", 1};
    OutPort<int> out_b{&producer, "out_b", 1};
    OutPort<int> fill{&filler, "fill", 1};
    InPort<int> in_a{&sink_a, "in_a", 1};
    InPort<int> in_b{&sink_b, "in_b", 1};
    out_a.connect(&in_a, 1);
    out_b.connect(&in_b, 1);
    fill.connect(&in_b, 1);
    ReliablePortSender reliable{out_a, out_b};

    producer.setCycle(0);
    filler.setCycle(0);
    require(fill.send(70), "failed to fill one transaction destination");
    require(!reliable.send(10, 20), "blocked multi-port publication did not become pending");
    require(!in_a.tryReceive(1), "failed multi-port request partially published");
    const auto filler_value = in_b.tryReceive(1);
    require(filler_value && *filler_value == 70, "failed to drain transaction blocker");

    producer.setCycle(1);
    require(reliable.send(100, 200), "multi-port retry did not commit");
    const auto a = in_a.tryReceive(2);
    const auto b = in_b.tryReceive(2);
    require(a && b && *a == 10 && *b == 20,
            "multi-port retry consumed replacement values or partially committed");
}

void testPendingFilterAndCancellation() {
    ManualUnit producer("filter_producer");
    ManualUnit filler("filter_filler");
    ManualUnit consumer("filter_consumer");
    OutPort<TaggedMessage> out{&producer, "out", 1};
    OutPort<TaggedMessage> fill{&filler, "fill", 1};
    InPort<TaggedMessage> in{&consumer, "in", 1};
    out.connect(&in, 1);
    fill.connect(&in, 1);
    ReliablePortSender reliable{out};

    producer.setCycle(0);
    filler.setCycle(0);
    require(fill.send(TaggedMessage{.key = 0, .payload = 90}),
            "failed to fill filtered destination");
    require(!reliable.send(TaggedMessage{.key = 9, .payload = 900}),
            "filtered message did not become pending");
    require(reliable.flushPending<&TaggedMessage::key>(FlushRange::youngerThan(uint64_t{5})),
            "selective pending flush did not cancel a younger payload");
    require(!reliable.pending(), "selective pending flush left storage occupied");

    const auto filler_value = in.tryReceive(1);
    require(filler_value && filler_value->payload == 90, "failed to drain filtered blocker");
    producer.setCycle(1);
    require(reliable.send(TaggedMessage{.key = 3, .payload = 300}),
            "kept reliable payload did not publish");
    const auto delivered = in.tryReceive(2);
    require(delivered && delivered->key == 3 && delivered->payload == 300,
            "canceled pending payload was delivered or replacement was lost");

    filler.setCycle(2);
    require(fill.send(TaggedMessage{.key = 0, .payload = 91}),
            "failed to refill filtered destination");
    producer.setCycle(2);
    require(!reliable.send(TaggedMessage{.key = 4, .payload = 400}),
            "predicate message did not become pending");
    require(!reliable.cancelPendingIf(
                [](const TaggedMessage& message) noexcept { return message.key > 10; }),
            "non-matching pending predicate canceled a payload");
    require(reliable.cancelPendingIf(
                [](const TaggedMessage& message) noexcept { return message.payload == 400; }),
            "matching pending predicate did not cancel a payload");

#if CHRONON_ENABLE_OUTPORT_CANCELLATION
    const auto second_filler = in.tryReceive(3);
    require(second_filler && second_filler->payload == 91,
            "failed to drain second filtered blocker");
    producer.setCycle(3);
    require(reliable.send(TaggedMessage{.key = 2, .payload = 200}),
            "failed to publish cancellation probe");
    reliable.cancelInFlight();
    require(!in.tryReceive(4), "cancelInFlight did not cancel already-published reliable data");
#endif
}

void testUnsupportedTopologyFailsLoudly() {
    ManualUnit producer_a("producer_a");
    ManualUnit producer_b("producer_b");
    ManualUnit sink_a("sink_a");
    ManualUnit sink_b("sink_b");
    OutPort<int> out_a{&producer_a, "out_a", 1};
    OutPort<int> out_b{&producer_b, "out_b", 1};
    InPort<int> in_a{&sink_a, "in_a", 1};
    InPort<int> in_b{&sink_b, "in_b", 1};
    out_a.connect(&in_a, 1);
    out_b.connect(&in_b, 1);
    ReliablePortSender reliable{out_a, out_b};

    bool rejected = false;
    try {
        (void)reliable.send(1, 2);
    } catch (const std::logic_error& error) {
        const std::string diagnostic = error.what();
        rejected = diagnostic.find("unsupported") != std::string::npos &&
                   diagnostic.find("out_a") != std::string::npos &&
                   diagnostic.find("out_b") != std::string::npos;
    }
    require(rejected, "cross-owner reliable transaction did not fail with topology diagnostics");
}

constexpr uint64_t kReliableMessages = 64;
constexpr uint64_t kPeerMessages = 40;
constexpr uint64_t kDifferentialCycles = 900;

struct StreamMessage {
    uint64_t sequence = 0;
    uint64_t request_cycle = 0;
    uint32_t source = 0;
};

uint64_t burn(uint64_t value, uint32_t iterations) noexcept {
    for (uint32_t i = 0; i < iterations; ++i) {
        value = value * 6364136223846793005ULL + 1442695040888963407ULL;
        value ^= value >> 29;
    }
    return value;
}

class ReliableProducer final : public TickableUnit {
public:
    ReliableProducer()
        : TickableUnit("reliable_producer"), reliable_(out_spsc, out_mpsc), events_(0) {
        events_.reserve(kDifferentialCycles * 4);
    }

    OutPort<StreamMessage> out_spsc{this, "out_spsc", 1};
    OutPort<StreamMessage> out_mpsc{this, "out_mpsc", 1};

    void tick() override {
        if (localCycle() == 53 && reliable_.pending()) pending_during_migration_cycle_ = true;
        if (next_sequence_ == kReliableMessages) return;

        if (reliable_.pending() && !cancel_exercised_) {
            const uint64_t canceled = next_sequence_;
            require(reliable_.cancelPendingIf(
                        [canceled](const StreamMessage& spsc, const StreamMessage& mpsc) noexcept {
                            return spsc.sequence == canceled && mpsc.sequence == canceled;
                        }),
                    "failed to selectively cancel retained multi-port payload");
            canceled_sequence_ = canceled;
            cancel_exercised_ = true;
            ++next_sequence_;
            events_.record(localCycle(), ModelEventKind::Flush, canceled);
            return;
        }

        const StreamMessage message{
            .sequence = next_sequence_, .request_cycle = localCycle(), .source = 0};
        const bool delivered = reliable_.send(message, message);
        events_.record(localCycle(), ModelEventKind::SendResult, next_sequence_, delivered);
        if (delivered) {
            ++next_sequence_;
        } else {
            ++backpressure_count_;
            events_.record(localCycle(), ModelEventKind::Backpressure, next_sequence_);
        }
        state_ = burn(state_ ^ next_sequence_, 1200);
        events_.record(localCycle(), ModelEventKind::State, next_sequence_, state_);
    }

    [[nodiscard]] uint64_t completed() const noexcept { return next_sequence_; }
    [[nodiscard]] uint64_t backpressureCount() const noexcept { return backpressure_count_; }
    [[nodiscard]] bool cancelExercised() const noexcept { return cancel_exercised_; }
    [[nodiscard]] uint64_t canceledSequence() const noexcept { return canceled_sequence_; }
    [[nodiscard]] bool pendingDuringMigrationCycle() const noexcept {
        return pending_during_migration_cycle_;
    }
    UnitEventLog& eventLog() noexcept { return events_; }

private:
    ReliablePortSender<StreamMessage, StreamMessage> reliable_;
    uint64_t next_sequence_ = 0;
    uint64_t backpressure_count_ = 0;
    uint64_t canceled_sequence_ = std::numeric_limits<uint64_t>::max();
    uint64_t state_ = 0x6a09e667f3bcc909ULL;
    bool cancel_exercised_ = false;
    bool pending_during_migration_cycle_ = false;
    UnitEventLog events_;
};

class PeerProducer final : public TickableUnit {
public:
    PeerProducer() : TickableUnit("peer_producer"), events_(1) {
        events_.reserve(kDifferentialCycles * 3);
    }

    OutPort<StreamMessage> out{this, "out", 1};

    void tick() override {
        if (next_sequence_ == kPeerMessages) return;
        const StreamMessage message{
            .sequence = next_sequence_, .request_cycle = localCycle(), .source = 1};
        const bool sent = out.send(message);
        events_.record(localCycle(), ModelEventKind::SendResult, next_sequence_, sent);
        if (sent) {
            ++next_sequence_;
        } else {
            events_.record(localCycle(), ModelEventKind::Backpressure, next_sequence_);
        }
        state_ = burn(state_ ^ next_sequence_, 90);
        events_.record(localCycle(), ModelEventKind::State, next_sequence_, state_);
    }

    [[nodiscard]] uint64_t sent() const noexcept { return next_sequence_; }
    UnitEventLog& eventLog() noexcept { return events_; }

private:
    uint64_t next_sequence_ = 0;
    uint64_t state_ = 0xbb67ae8584caa73bULL;
    UnitEventLog events_;
};

class ReliableSink final : public TickableUnit {
public:
    ReliableSink(std::string name, uint32_t component, bool accepts_peer)
        : TickableUnit(std::move(name)), accepts_peer_(accepts_peer), events_(component) {
        events_.reserve(kDifferentialCycles * 3);
        reliable_sequences_.reserve(kReliableMessages);
    }

    InPort<StreamMessage> in{this, "in", 4};

    void tick() override {
        size_t budget = (localCycle() % 4 == 0) ? 2 : 0;
        if (localCycle() >= 45 && localCycle() < 61) budget = 0;
        for (size_t slot = 0; slot < budget; ++slot) {
            auto message = in.tryReceive(localCycle());
            if (!message) break;
            const uint64_t identity = (uint64_t{message->source} << 32) | message->sequence;
            events_.record(localCycle(), ModelEventKind::Receive, identity, message->request_cycle);
            if (message->source == 0) {
                reliable_sequences_.push_back(message->sequence);
            } else {
                require(accepts_peer_, "peer payload reached the SPSC reliable sink");
                ++peer_received_;
            }
        }
        events_.record(localCycle(), ModelEventKind::State, reliable_sequences_.size(),
                       peer_received_);
    }

    [[nodiscard]] const std::vector<uint64_t>& reliableSequences() const noexcept {
        return reliable_sequences_;
    }
    [[nodiscard]] uint64_t peerReceived() const noexcept { return peer_received_; }
    UnitEventLog& eventLog() noexcept { return events_; }

private:
    bool accepts_peer_ = false;
    uint64_t peer_received_ = 0;
    std::vector<uint64_t> reliable_sequences_;
    UnitEventLog events_;
};

class ReliableScenario {
public:
    explicit ReliableScenario(TickSimulation& simulation) {
        producer_ = simulation.createUnit<ReliableProducer>();
        peer_ = simulation.createUnit<PeerProducer>();
        spsc_sink_ = simulation.createUnit<ReliableSink>("spsc_sink", 2, false);
        mpsc_sink_ = simulation.createUnit<ReliableSink>("mpsc_sink", 3, true);

        simulation.connect(producer_->out_spsc, spsc_sink_->in, 1);
        simulation.connect(producer_->out_mpsc, mpsc_sink_->in, 1);
        simulation.connect(peer_->out, mpsc_sink_->in, 1);
    }

    std::vector<std::string> componentNames() const {
        return {"reliable_producer", "peer_producer", "spsc_sink", "mpsc_sink"};
    }

    std::vector<chronon::sender::test::CanonicalEvent> canonicalEvents() const {
        require(producer_->completed() == kReliableMessages,
                "reliable producer did not complete its logical stream");
        require(peer_->sent() == kPeerMessages, "peer producer did not complete its stream");
        require(producer_->backpressureCount() != 0,
                "reliable matrix did not exercise retained backpressure");
        require(producer_->cancelExercised(),
                "reliable matrix did not exercise selective pending cancellation");
        require(producer_->pendingDuringMigrationCycle(),
                "forced migration boundary did not retain a reliable payload");
        require(mpsc_sink_->peerReceived() == kPeerMessages,
                "MPSC sink lost peer-producer messages");

        const auto& spsc = spsc_sink_->reliableSequences();
        const auto& mpsc = mpsc_sink_->reliableSequences();
        require(spsc == mpsc, "SPSC and MPSC reliable transaction streams diverged");
        require(spsc.size() + 1 == kReliableMessages,
                "reliable stream lost or duplicated more than the canceled request");

        size_t observed = 0;
        for (uint64_t sequence = 0; sequence < kReliableMessages; ++sequence) {
            if (sequence == producer_->canceledSequence()) continue;
            require(observed < spsc.size() && spsc[observed] == sequence,
                    "reliable stream contained a duplicate, loss, or reordering");
            ++observed;
        }

        std::array<UnitEventLog*, 4> logs{&producer_->eventLog(), &peer_->eventLog(),
                                          &spsc_sink_->eventLog(), &mpsc_sink_->eventLog()};
        return canonicalizeEvents(std::span<UnitEventLog* const>(logs.data(), logs.size()));
    }

private:
    ReliableProducer* producer_ = nullptr;
    PeerProducer* peer_ = nullptr;
    ReliableSink* spsc_sink_ = nullptr;
    ReliableSink* mpsc_sink_ = nullptr;
};

void testEpochFreeReliableDifferentialMatrix() {
    TickSimulationConfig base;
    base.max_lookahead_cycles = 8;
    base.enable_weighted_partitioning = true;
    base.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    base.initial_partition_sync_cost_ns = 0.0;
    base.rebalance_check_interval_cycles = 32;
    base.rebalance_imbalance_threshold = 1.01;
    base.rebalance_min_gain = 0.0;
    base.rebalance_cooldown_cycles = 0;

    std::vector<EpochFreeRunMode> modes{{.name = "sequential-reference",
                                         .kind = EpochFreeRunKind::SequentialReference,
                                         .num_threads = 1,
                                         .migrations = {}}};
    for (size_t workers = 2; workers <= 6; ++workers) {
        modes.push_back({.name = "epoch-free-static-" + std::to_string(workers),
                         .kind = EpochFreeRunKind::Static,
                         .num_threads = workers,
                         .migrations = {}});
    }
    modes.push_back({.name = "epoch-free-forced-4",
                     .kind = EpochFreeRunKind::ForcedMigration,
                     .num_threads = 4,
                     .migrations = {{.cycle = 53, .unit_name = "reliable_producer"},
                                    {.cycle = 117, .unit_name = "mpsc_sink"}}});
    modes.push_back({.name = "epoch-free-runtime-3",
                     .kind = EpochFreeRunKind::RuntimeRebalance,
                     .num_threads = 3,
                     .migrations = {}});

    const auto artifacts =
        runEpochFreeMatrix(base, kDifferentialCycles, std::span<const EpochFreeRunMode>(modes),
                           [](TickSimulation& simulation, const EpochFreeRunMode&) {
                               return std::make_unique<ReliableScenario>(simulation);
                           });
    const auto comparison = compareMatrix(artifacts);
    require(comparison.equivalent, comparison.diagnostic);

    for (const auto& artifact : artifacts) {
        if (artifact.mode_name == "sequential-reference") {
            require(artifact.epoch_free_runs == 0,
                    "sequential reliable oracle used epoch-free execution");
        } else {
            require(artifact.epoch_free_runs != 0,
                    artifact.mode_name + " fell back from epoch-free execution");
        }
        if (artifact.mode_name == "epoch-free-forced-4") {
            require(artifact.forced_migrations_applied == 2,
                    "reliable matrix did not apply both forced migrations");
        }
        std::cout << "  " << artifact.mode_name << ": digest=" << artifact.digest
                  << " events=" << artifact.events.size()
                  << " rebalances=" << artifact.rebalance_count << '\n';
    }
}

}  // namespace

int main() {
    try {
        testMoveOnlyPayloadIsRetainedExactlyOnce();
        testBoundedOverflowAndDiagnosticsFailLoudly();
        testFanoutRetryPublishesAllOrNone();
        testMultiPortRetryUsesAtomicTransaction();
        testPendingFilterAndCancellation();
        testUnsupportedTopologyFailsLoudly();
        testEpochFreeReliableDifferentialMatrix();
    } catch (const std::exception& error) {
        std::cerr << "Reliable port sender test failed: " << error.what() << '\n';
        return 1;
    }
    std::cout << "Reliable port sender tests passed\n";
    return 0;
}
