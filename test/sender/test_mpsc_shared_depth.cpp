// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

// Aggregate bounded-MPSC contract under the epoch-free scheduler. Burst
// producers use heterogeneous delays while a slow receiver exercises shared
// FIFO saturation, receiver filtering, selective flush, forced migration, and
// runtime rebalance. Every model-visible event must match the sequential oracle.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "EpochFreeDifferentialHarness.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/Port.hpp"

using namespace chronon::sender;
using namespace chronon::sender::test;

namespace {

constexpr size_t kProducerCount = 4;
constexpr size_t kSharedDepth = 8;
constexpr uint64_t kMessagesPerProducer = 48;
constexpr uint64_t kFlushCycle = 40;
constexpr uint64_t kDrainCycle = 240;
constexpr uint64_t kRunCycles = 800;
constexpr uint32_t kConsumerComponent = kProducerCount;

[[noreturn]] void fail(const std::string& message) { throw std::runtime_error(message); }

void require(bool condition, const std::string& message) {
    if (!condition) fail(message);
}

uint64_t burn(uint64_t value, uint32_t iterations) noexcept {
    for (uint32_t i = 0; i < iterations; ++i) {
        value = value * 6364136223846793005ULL + 1442695040888963407ULL;
        value ^= value >> 27;
    }
    return value;
}

struct Message {
    uint64_t sequence = 0;
    uint64_t send_cycle = 0;
    uint32_t producer = 0;
};

class BurstProducer final : public TickableUnit {
public:
    BurstProducer(uint32_t producer, uint32_t burn_iterations)
        : TickableUnit("producer_" + std::to_string(producer)),
          producer_(producer),
          burn_iterations_(burn_iterations),
          events_(producer) {
        events_.reserve(kMessagesPerProducer * 3);
    }

    OutPort<Message> out{this, "out", 1};

    void tick() override {
        if (next_sequence_ == kMessagesPerProducer) return;

        for (size_t slot = 0; slot < 1 && next_sequence_ < kMessagesPerProducer; ++slot) {
            const Message message{
                .sequence = next_sequence_, .send_cycle = localCycle(), .producer = producer_};
            const bool sent = out.send(message);
            events_.record(localCycle(), ModelEventKind::SendResult, next_sequence_, sent);
            if (!sent) break;
            ++next_sequence_;
        }
        state_ = burn(state_ ^ next_sequence_, burn_iterations_);
        events_.record(localCycle(), ModelEventKind::State, next_sequence_, state_);
    }

    [[nodiscard]] uint64_t sent() const noexcept { return next_sequence_; }
    UnitEventLog& eventLog() noexcept { return events_; }

private:
    uint32_t producer_ = 0;
    uint32_t burn_iterations_ = 0;
    uint64_t next_sequence_ = 0;
    uint64_t state_ = 0x9e3779b97f4a7c15ULL;
    UnitEventLog events_;
};

class SlowConsumer final : public TickableUnit {
public:
    SlowConsumer() : TickableUnit("consumer"), events_(kConsumerComponent) {
        events_.reserve(kRunCycles * 3);
    }

    InPort<Message> in{this, "in", kSharedDepth};

    void tick() override {
        const uint64_t cycle = localCycle();
        observeDepth_();

        if (cycle == kFlushCycle) {
            // Empty keep range: cancel every message enqueued before this cycle.
            // Same-cycle and later producer traffic remains post-flush.
            in.flush<&Message::sequence>(FlushRange::atAndYounger(uint64_t{0}));
            flush_installed_ = true;
            events_.record(cycle, ModelEventKind::Flush, kFlushCycle, 0);
        }

        const size_t receive_budget =
            cycle >= kDrainCycle ? kSharedDepth : (cycle % 3 == 0 ? 1 : 0);
        for (size_t slot = 0; slot < receive_budget; ++slot) {
            auto filter = [this](const Message& message) noexcept {
                const bool accept = (message.sequence % 7) != 0;
                if (!accept) ++filtered_count_;
                return accept;
            };
            auto message = in.tryReceiveFiltered(cycle, filter);
            if (!message) break;
            if (flush_installed_ && message->send_cycle < kFlushCycle) {
                fail("selective flush leaked a pre-flush message");
            }
            const uint64_t identity = (uint64_t{message->producer} << 32) | message->sequence;
            events_.record(cycle, ModelEventKind::Receive, identity, message->send_cycle);
            ++received_count_;
        }

        observeDepth_();
        state_ = burn(state_ ^ received_count_ ^ filtered_count_, 200);
        events_.record(cycle, ModelEventKind::State, in.queuedMessageCount(), filtered_count_);
    }

    void validateFinalState() const {
        require(max_shared_depth_ == kSharedDepth,
                "slow consumer never saturated the aggregate shared FIFO");
        require(in.sharedFifoHighWatermark() == kSharedDepth,
                "shared FIFO high-water mark differs from its declared depth");
        require(in.queuedMessageCount() == 0, "shared FIFO did not drain by the end of the run");
        require(in.transportPendingMessageCount() == 0,
                "private ingress lanes did not drain by the end of the run");
        require(filtered_count_ != 0, "receiver filter did not reject any ready message");
        require(received_count_ != 0, "receiver did not accept any message");
    }

    UnitEventLog& eventLog() noexcept { return events_; }

private:
    void observeDepth_() {
        const size_t depth = in.queuedMessageCount();
        if (depth > kSharedDepth) {
            fail("aggregate shared FIFO exceeded its declared depth");
        }
        max_shared_depth_ = std::max(max_shared_depth_, depth);
    }

    bool flush_installed_ = false;
    size_t max_shared_depth_ = 0;
    uint64_t received_count_ = 0;
    uint64_t filtered_count_ = 0;
    uint64_t state_ = 0x243f6a8885a308d3ULL;
    UnitEventLog events_;
};

class SharedDepthScenario {
public:
    explicit SharedDepthScenario(TickSimulation& simulation) {
        for (uint32_t producer = 0; producer < kProducerCount; ++producer) {
            // Producer zero is intentionally heavier so runtime rebalance has
            // a stable, measurable reason to move work.
            const uint32_t work = producer == 0 ? 2400 : 80;
            producers_[producer] = simulation.createUnit<BurstProducer>(producer, work);
        }
        consumer_ = simulation.createUnit<SlowConsumer>();

        constexpr std::array<uint32_t, kProducerCount> delays{1, 2, 3, 4};
        for (size_t producer = 0; producer < kProducerCount; ++producer) {
            simulation.connect(producers_[producer]->out, consumer_->in, delays[producer]);
        }
    }

    std::vector<std::string> componentNames() const {
        return {"producer_0", "producer_1", "producer_2", "producer_3", "consumer"};
    }

    std::vector<CanonicalEvent> canonicalEvents() const {
        for (const auto* producer : producers_) {
            require(producer->sent() == kMessagesPerProducer,
                    producer->name() + " did not eventually send its complete burst");
        }
        consumer_->validateFinalState();

        std::array<UnitEventLog*, kProducerCount + 1> logs{};
        for (size_t producer = 0; producer < kProducerCount; ++producer) {
            logs[producer] = &producers_[producer]->eventLog();
        }
        logs.back() = &consumer_->eventLog();
        return canonicalizeEvents(std::span<UnitEventLog* const>(logs.data(), logs.size()));
    }

private:
    std::array<BurstProducer*, kProducerCount> producers_{};
    SlowConsumer* consumer_ = nullptr;
};

void verifySharedDepthMatrix() {
    TickSimulationConfig base;
    base.max_lookahead_cycles = 8;
    base.enable_weighted_partitioning = true;
    base.partition_solver = TickSimulationConfig::PartitionSolverType::Weighted;
    base.initial_partition_sync_cost_ns = 0.0;
    base.rebalance_check_interval_cycles = 64;
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
        {.name = "epoch-free-static-5",
         .kind = EpochFreeRunKind::Static,
         .num_threads = 5,
         .migrations = {}},
        {.name = "epoch-free-static-6",
         .kind = EpochFreeRunKind::Static,
         .num_threads = 6,
         .migrations = {}},
        {.name = "epoch-free-static-7",
         .kind = EpochFreeRunKind::Static,
         .num_threads = 7,
         .migrations = {}},
        {.name = "epoch-free-static-8",
         .kind = EpochFreeRunKind::Static,
         .num_threads = 8,
         .migrations = {}},
        {.name = "epoch-free-forced-4",
         .kind = EpochFreeRunKind::ForcedMigration,
         .num_threads = 4,
         .migrations = {{.cycle = 73, .unit_name = "consumer"},
                        {.cycle = 157, .unit_name = "producer_2"}}},
        {.name = "epoch-free-runtime-4",
         .kind = EpochFreeRunKind::RuntimeRebalance,
         .num_threads = 4,
         .migrations = {}},
    };

    const auto artifacts =
        runEpochFreeMatrix(base, kRunCycles, std::span<const EpochFreeRunMode>(modes),
                           [](TickSimulation& simulation, const EpochFreeRunMode&) {
                               return std::make_unique<SharedDepthScenario>(simulation);
                           });
    const auto comparison = compareMatrix(artifacts);
    require(comparison.equivalent, comparison.diagnostic);

    for (const auto& artifact : artifacts) {
        if (artifact.mode_name == "sequential-reference") {
            require(artifact.epoch_free_runs == 0,
                    "sequential oracle unexpectedly used epoch-free execution");
        } else {
            require(artifact.epoch_free_runs != 0,
                    artifact.mode_name + " fell back from epoch-free execution");
        }
        if (artifact.mode_name == "epoch-free-forced-4") {
            require(artifact.forced_migrations_applied == 2,
                    "forced-migration mode did not apply both moves");
        }
        if (artifact.mode_name == "epoch-free-runtime-4") {
#ifndef CHRONON_SANITIZER_BUILD
            require(artifact.rebalance_count != 0,
                    "runtime mode did not exercise an epoch-free rebalance");
#endif
        }
        std::cout << "  " << artifact.mode_name << ": digest=" << artifact.digest
                  << " events=" << artifact.events.size()
                  << " rebalances=" << artifact.rebalance_count << '\n';
    }
}

}  // namespace

int main() {
    try {
        std::cout << "=== Bounded MPSC shared-depth contract ===\n";
        verifySharedDepthMatrix();
        std::cout << "Bounded MPSC shared-depth tests passed.\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
