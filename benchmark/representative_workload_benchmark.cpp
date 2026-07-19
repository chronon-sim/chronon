// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "RepresentativeWorkload.hpp"
#include "sender/core/TickSimulation.hpp"
#include "sender/core/TickableUnit.hpp"
#include "sender/port/InPort.hpp"
#include "sender/port/OutPort.hpp"

namespace chronon::benchmark {
namespace {

using sender::InPort;
using sender::OutPort;
using sender::PlatformMetrics;
using sender::TickableUnit;
using sender::TickSimulation;
using sender::TickSimulationConfig;

template <size_t Bytes>
struct Payload {
    static_assert(Bytes >= 8 && Bytes % 8 == 0);
    std::array<uint64_t, Bytes / 8> words{};
};

static_assert(sizeof(Payload<8>) == 8);
static_assert(sizeof(Payload<16>) == 16);
static_assert(sizeof(Payload<32>) == 32);
static_assert(sizeof(Payload<64>) == 64);
static_assert(sizeof(Payload<144>) == 144);
static_assert(sizeof(Payload<256>) == 256);

struct alignas(64) UnitCounters {
    uint64_t ticks = 0;
    uint64_t work_iterations = 0;
    uint64_t offered = 0;
    uint64_t suppressed_offers = 0;
    uint64_t send_attempts = 0;
    uint64_t blocked_attempts = 0;
    uint64_t publications = 0;
    uint64_t sent_deliveries = 0;
    uint64_t sent_bytes = 0;
    uint64_t received = 0;
    uint64_t received_bytes = 0;
    uint64_t receive_checksum = 0;
};
static_assert(sizeof(UnitCounters) % 64 == 0);

struct Totals : UnitCounters {
    uint64_t queued = 0;
    uint64_t pending_publications = 0;
    uint64_t state_digest = 0;
};

[[nodiscard]] UnitCounters subtract(const Totals& after, const Totals& before) {
    UnitCounters result;
    result.ticks = after.ticks - before.ticks;
    result.work_iterations = after.work_iterations - before.work_iterations;
    result.offered = after.offered - before.offered;
    result.suppressed_offers = after.suppressed_offers - before.suppressed_offers;
    result.send_attempts = after.send_attempts - before.send_attempts;
    result.blocked_attempts = after.blocked_attempts - before.blocked_attempts;
    result.publications = after.publications - before.publications;
    result.sent_deliveries = after.sent_deliveries - before.sent_deliveries;
    result.sent_bytes = after.sent_bytes - before.sent_bytes;
    result.received = after.received - before.received;
    result.received_bytes = after.received_bytes - before.received_bytes;
    return result;
}

template <size_t Bytes>
struct TxChannel {
    explicit TxChannel(const ChannelSpec* channel, TickableUnit* owner)
        : spec(channel),
          port(std::make_unique<OutPort<Payload<Bytes>>>(owner, "tx" + std::to_string(channel->id),
                                                         1)) {}

    const ChannelSpec* spec;
    std::unique_ptr<OutPort<Payload<Bytes>>> port;
    std::optional<Payload<Bytes>> pending;
    uint64_t next_sequence = 0;
};

class alignas(64) WorkloadUnit final : public TickableUnit {
public:
    WorkloadUnit(uint32_t id, const Scenario* scenario)
        : TickableUnit("bench_unit_" + std::to_string(id)),
          id_(id),
          scenario_(scenario),
          spec_(&scenario->units.at(id)),
          in8_(this, "rx8", inputCapacity_<8>()),
          in16_(this, "rx16", inputCapacity_<16>()),
          in32_(this, "rx32", inputCapacity_<32>()),
          in64_(this, "rx64", inputCapacity_<64>()),
          in144_(this, "rx144", inputCapacity_<144>()),
          in256_(this, "rx256", inputCapacity_<256>()),
          scratch_(std::max<size_t>(8, spec_->working_set_bytes / sizeof(uint64_t))) {
        state_ = splitMix64(scenario_->config.seed ^ id_);
        for (size_t i = 0; i < scratch_.size(); ++i) {
            scratch_[i] = randomWord(scenario_->config.seed, 0x53435241544348ULL,
                                     static_cast<uint64_t>(id_) * scratch_.size() + i);
        }
        for (const auto& channel : scenario_->channels) {
            if (channel.source != id_) continue;
            switch (channel.payload) {
                case PayloadClass::Bytes8:
                    tx8_.emplace_back(&channel, this);
                    break;
                case PayloadClass::Bytes16:
                    tx16_.emplace_back(&channel, this);
                    break;
                case PayloadClass::Bytes32:
                    tx32_.emplace_back(&channel, this);
                    break;
                case PayloadClass::Bytes64:
                    tx64_.emplace_back(&channel, this);
                    break;
                case PayloadClass::Bytes144:
                    tx144_.emplace_back(&channel, this);
                    break;
                case PayloadClass::Bytes256:
                    tx256_.emplace_back(&channel, this);
                    break;
            }
        }
    }

    void tick() override {
        drainInputs_();
        runWork_();
        processTx_(tx8_);
        processTx_(tx16_);
        processTx_(tx32_);
        processTx_(tx64_);
        processTx_(tx144_);
        processTx_(tx256_);
        ++counters_.ticks;
    }

    void connectChannels(TickSimulation& simulation, const std::vector<WorkloadUnit*>& units) {
        connectTx_(simulation, units, tx8_);
        connectTx_(simulation, units, tx16_);
        connectTx_(simulation, units, tx32_);
        connectTx_(simulation, units, tx64_);
        connectTx_(simulation, units, tx144_);
        connectTx_(simulation, units, tx256_);
    }

    [[nodiscard]] const UnitCounters& counters() const noexcept { return counters_; }

    [[nodiscard]] uint64_t queued() const {
        return queuedOn_(in8_) + queuedOn_(in16_) + queuedOn_(in32_) + queuedOn_(in64_) +
               queuedOn_(in144_) + queuedOn_(in256_);
    }

    [[nodiscard]] uint64_t pendingPublications() const noexcept {
        return pendingCount_(tx8_) + pendingCount_(tx16_) + pendingCount_(tx32_) +
               pendingCount_(tx64_) + pendingCount_(tx144_) + pendingCount_(tx256_);
    }

    [[nodiscard]] uint64_t digest() const noexcept {
        uint64_t digest = splitMix64(state_ ^ counters_.receive_checksum ^ id_);
        digest = splitMix64(digest ^ counters_.sent_deliveries ^ counters_.received);
        digest = splitMix64(digest ^ counters_.blocked_attempts ^ queued());
        for (size_t i = 0; i < scratch_.size(); i += std::max<size_t>(1, scratch_.size() / 64)) {
            digest = splitMix64(digest ^ scratch_[i] ^ i);
        }
        foldTxDigest_(digest, tx8_);
        foldTxDigest_(digest, tx16_);
        foldTxDigest_(digest, tx32_);
        foldTxDigest_(digest, tx64_);
        foldTxDigest_(digest, tx144_);
        foldTxDigest_(digest, tx256_);
        return digest;
    }

    [[nodiscard]] double modeledCost() const {
        const double sum =
            std::accumulate(spec_->work_schedule.begin(), spec_->work_schedule.end(), 0.0);
        return 20.0 + 2.0 * sum / static_cast<double>(spec_->work_schedule.size());
    }

private:
    template <size_t Bytes>
    [[nodiscard]] static uint64_t queuedOn_(const InPort<Payload<Bytes>>& port) noexcept {
        uint64_t queued = port.queuedMessageCount();
        // A bounded MPSC port stages newly published entries in per-connection
        // lanes before its receiver-owned aggregate FIFO. size() intentionally
        // reports only that FIFO, so conservation must include the lanes too.
        if (port.isMultiProducerMode() &&
            port.configuredCapacity() != InPort<Payload<Bytes>>::UNLIMITED_CAPACITY) {
            queued += port.transportPendingMessageCount();
        }
        return queued;
    }

    template <size_t Bytes>
    [[nodiscard]] size_t inputCapacity_() const noexcept {
        (void)Bytes;
        return scenario_->config.queue_capacity == 0 ? InPort<Payload<Bytes>>::UNLIMITED_CAPACITY
                                                     : scenario_->config.queue_capacity;
    }

    template <size_t Bytes>
    [[nodiscard]] InPort<Payload<Bytes>>& input_() noexcept {
        if constexpr (Bytes == 8) return in8_;
        if constexpr (Bytes == 16) return in16_;
        if constexpr (Bytes == 32) return in32_;
        if constexpr (Bytes == 64) return in64_;
        if constexpr (Bytes == 144) return in144_;
        if constexpr (Bytes == 256) return in256_;
    }

    template <size_t Bytes>
    static Payload<Bytes> makePayload_(uint64_t seed, uint32_t source, uint32_t channel,
                                       uint64_t sequence, uint64_t cycle) noexcept {
        Payload<Bytes> payload;
        const uint64_t identity = splitMix64(seed ^ (static_cast<uint64_t>(source) << 32) ^
                                             channel ^ splitMix64(sequence));
        for (size_t i = 0; i < payload.words.size(); ++i) {
            payload.words[i] = splitMix64(identity ^ (static_cast<uint64_t>(i) << 48) ^ cycle);
        }
        return payload;
    }

    template <size_t Bytes>
    void processTx_(std::vector<TxChannel<Bytes>>& channels) {
        const uint64_t cycle = localCycle();
        for (auto& channel : channels) {
            if (channel.spec->scheduled(cycle, scenario_->config.send_period)) {
                if (channel.pending) {
                    ++counters_.suppressed_offers;
                } else {
                    channel.pending =
                        makePayload_<Bytes>(scenario_->config.seed, id_, channel.spec->id,
                                            channel.next_sequence, cycle);
                    ++counters_.offered;
                }
            }
            if (!channel.pending) continue;
            ++counters_.send_attempts;
            if (!channel.port->send(*channel.pending)) {
                ++counters_.blocked_attempts;
                continue;
            }
            const uint64_t fanout = channel.spec->destinations.size();
            ++counters_.publications;
            counters_.sent_deliveries += fanout;
            counters_.sent_bytes += fanout * Bytes;
            ++channel.next_sequence;
            channel.pending.reset();
        }
    }

    template <size_t Bytes>
    [[nodiscard]] bool receiveOne_(InPort<Payload<Bytes>>& port) {
        auto payload = port.tryReceive(localCycle());
        if (!payload) return false;
        uint64_t checksum = counters_.receive_checksum;
        for (size_t i = 0; i < payload->words.size(); ++i) {
            checksum = splitMix64(checksum ^ payload->words[i] ^ (i * 0x9e3779b9ULL));
        }
        counters_.receive_checksum = checksum;
        ++counters_.received;
        counters_.received_bytes += Bytes;
        return true;
    }

    [[nodiscard]] bool receiveType_(uint32_t type) {
        switch (type) {
            case 0:
                return receiveOne_(in8_);
            case 1:
                return receiveOne_(in16_);
            case 2:
                return receiveOne_(in32_);
            case 3:
                return receiveOne_(in64_);
            case 4:
                return receiveOne_(in144_);
            default:
                return receiveOne_(in256_);
        }
    }

    void drainInputs_() {
        constexpr uint32_t kPayloadTypes = PAYLOAD_BYTES.size();
        for (uint32_t drained = 0; drained < spec_->drain_limit; ++drained) {
            const uint32_t first =
                static_cast<uint32_t>((localCycle() + id_ + drained) % kPayloadTypes);
            bool found = false;
            for (uint32_t offset = 0; offset < kPayloadTypes; ++offset) {
                if (receiveType_((first + offset) % kPayloadTypes)) {
                    found = true;
                    break;
                }
            }
            if (!found) break;
        }
    }

    void runWork_() noexcept {
        const uint32_t work = spec_->work_schedule[localCycle() % spec_->work_schedule.size()];
        const size_t mask = scratch_.size() - 1;
        uint64_t state = state_;
        for (uint32_t iteration = 0; iteration < work; ++iteration) {
            const size_t index = static_cast<size_t>(state ^ (state >> 23U) ^ iteration) & mask;
            const uint64_t value = scratch_[index];
            state = std::rotl(state + value + 0x9e3779b97f4a7c15ULL, 17) * 0xbf58476d1ce4e5b9ULL;
            scratch_[index] = state ^ std::rotl(value, 11);
        }
        state_ = state;
        counters_.work_iterations += work;
    }

    template <size_t Bytes>
    void connectTx_(TickSimulation& simulation, const std::vector<WorkloadUnit*>& units,
                    std::vector<TxChannel<Bytes>>& channels) {
        for (auto& channel : channels) {
            for (uint32_t destination : channel.spec->destinations) {
                simulation.connect(*channel.port, units[destination]->input_<Bytes>(),
                                   channel.spec->delay);
            }
        }
    }

    template <size_t Bytes>
    static uint64_t pendingCount_(const std::vector<TxChannel<Bytes>>& channels) noexcept {
        return std::count_if(channels.begin(), channels.end(),
                             [](const auto& channel) { return channel.pending.has_value(); });
    }

    template <size_t Bytes>
    static void foldTxDigest_(uint64_t& digest,
                              const std::vector<TxChannel<Bytes>>& channels) noexcept {
        for (const auto& channel : channels) {
            digest = splitMix64(digest ^ channel.next_sequence ^ channel.spec->id);
            if (channel.pending) digest = splitMix64(digest ^ channel.pending->words.front());
        }
    }

    uint32_t id_;
    const Scenario* scenario_;
    const UnitSpec* spec_;
    InPort<Payload<8>> in8_;
    InPort<Payload<16>> in16_;
    InPort<Payload<32>> in32_;
    InPort<Payload<64>> in64_;
    InPort<Payload<144>> in144_;
    InPort<Payload<256>> in256_;
    std::vector<TxChannel<8>> tx8_;
    std::vector<TxChannel<16>> tx16_;
    std::vector<TxChannel<32>> tx32_;
    std::vector<TxChannel<64>> tx64_;
    std::vector<TxChannel<144>> tx144_;
    std::vector<TxChannel<256>> tx256_;
    std::vector<uint64_t> scratch_;
    uint64_t state_ = 0;
    UnitCounters counters_;
};

[[nodiscard]] Totals collectTotals(const std::vector<WorkloadUnit*>& units) {
    Totals totals;
    for (const auto* unit : units) {
        const auto& c = unit->counters();
        totals.ticks += c.ticks;
        totals.work_iterations += c.work_iterations;
        totals.offered += c.offered;
        totals.suppressed_offers += c.suppressed_offers;
        totals.send_attempts += c.send_attempts;
        totals.blocked_attempts += c.blocked_attempts;
        totals.publications += c.publications;
        totals.sent_deliveries += c.sent_deliveries;
        totals.sent_bytes += c.sent_bytes;
        totals.received += c.received;
        totals.received_bytes += c.received_bytes;
        totals.receive_checksum ^= c.receive_checksum;
        totals.queued += unit->queued();
        totals.pending_publications += unit->pendingPublications();
        totals.state_digest = splitMix64(totals.state_digest ^ unit->digest());
    }
    return totals;
}

struct RunResult {
    size_t workers = 1;
    double setup_seconds = 0.0;
    double warmup_seconds = 0.0;
    double wall_seconds = 0.0;
    UnitCounters measured;
    Totals final;
    bool parallel = false;
    bool epoch_free = false;
    size_t transparent_broadcast_connections = 0;
    uint64_t transport_overflows = 0;
};

struct RunOptions {
    uint64_t warmup_cycles = 8192;
    uint64_t measured_cycles = 50'000;
    bool dynamic_rebalance = false;
    bool precomputed_costs = true;
    uint32_t max_lookahead = 32;
};

[[nodiscard]] RunResult runOnce(const Scenario& scenario, size_t workers,
                                const RunOptions& options) {
    const auto setup_begin = std::chrono::steady_clock::now();
    TickSimulationConfig config;
    config.num_threads = workers;
    config.enable_parallel = workers > 1;
    config.enable_lookahead = true;
    config.enable_epoch_free_lookahead = true;
    config.enable_dynamic_rebalance = options.dynamic_rebalance;
    config.max_lookahead_cycles = options.max_lookahead;

    TickSimulation simulation(config);
    std::vector<WorkloadUnit*> units;
    units.reserve(scenario.units.size());
    for (uint32_t id = 0; id < scenario.units.size(); ++id) {
        units.push_back(simulation.createUnit<WorkloadUnit>(id, &scenario));
    }
    for (auto* unit : units) unit->connectChannels(simulation, units);
    if (options.precomputed_costs) {
        std::vector<double> costs;
        costs.reserve(units.size());
        for (const auto* unit : units) costs.push_back(unit->modeledCost());
        simulation.setPrecomputedUnitCosts(std::move(costs), PlatformMetrics{8.0});
    }
    simulation.initialize();
    const auto setup_end = std::chrono::steady_clock::now();

    const auto warmup_begin = std::chrono::steady_clock::now();
    simulation.run(options.warmup_cycles);
    const auto warmup_end = std::chrono::steady_clock::now();
    const Totals before = collectTotals(units);

    const auto measured_begin = std::chrono::steady_clock::now();
    simulation.run(options.measured_cycles);
    const auto measured_end = std::chrono::steady_clock::now();
    const Totals after = collectTotals(units);

    RunResult result;
    result.workers = workers;
    result.setup_seconds = std::chrono::duration<double>(setup_end - setup_begin).count();
    result.warmup_seconds = std::chrono::duration<double>(warmup_end - warmup_begin).count();
    result.wall_seconds = std::chrono::duration<double>(measured_end - measured_begin).count();
    result.measured = subtract(after, before);
    result.final = after;
    result.parallel = simulation.useParallelExecution();
    result.epoch_free = simulation.epochFreeRunCount() != 0;
    result.transparent_broadcast_connections = simulation.transparentBroadcastConnectionCount();
    result.transport_overflows = simulation.totalTransportOverflowEvents();

    const uint64_t expected_ticks = scenario.config.num_units * options.measured_cycles;
    if (result.measured.ticks != expected_ticks) {
        throw std::runtime_error("unit tick count does not match scenario dimensions");
    }
    if (result.final.sent_deliveries != result.final.received + result.final.queued) {
        throw std::runtime_error("message conservation failed: sent != received + queued");
    }
    if (result.transport_overflows != 0) {
        throw std::runtime_error("cross-thread transport overflowed during benchmark");
    }
    return result;
}

struct CliOptions {
    std::string profile = "nucleus";
    uint64_t seed = 0x4348524f4e4f4eULL;
    bool random_seed = false;
    uint64_t scenario_offset = 0;
    uint32_t scenario_count = 1;
    uint32_t repetitions = 5;
    std::vector<size_t> workers = {1, 2, 4, 8};
    RunOptions run;
    bool describe_only = false;
    bool verbose = false;
};

[[nodiscard]] uint64_t parseInteger(std::string_view value, std::string_view option) {
    size_t consumed = 0;
    const uint64_t result = std::stoull(std::string(value), &consumed, 0);
    if (consumed != value.size()) {
        throw std::invalid_argument("invalid value for " + std::string(option));
    }
    return result;
}

[[nodiscard]] std::vector<uint32_t> parseU32List(std::string_view text) {
    std::vector<uint32_t> result;
    std::stringstream stream{std::string(text)};
    std::string item;
    while (std::getline(stream, item, ',')) {
        result.push_back(static_cast<uint32_t>(parseInteger(item, "list")));
    }
    return result;
}

[[nodiscard]] std::vector<size_t> parseWorkers(std::string_view text) {
    const auto values = parseU32List(text);
    std::vector<size_t> workers(values.begin(), values.end());
    if (workers.empty() || std::find(workers.begin(), workers.end(), 0) != workers.end()) {
        throw std::invalid_argument("worker counts must be positive");
    }
    std::sort(workers.begin(), workers.end());
    workers.erase(std::unique(workers.begin(), workers.end()), workers.end());
    return workers;
}

[[nodiscard]] ScenarioConfig profileConfig(std::string_view profile, uint64_t seed) {
    ScenarioConfig config;
    config.seed = seed;
    if (profile == "nucleus") return config;
    if (profile == "scheduler") {
        config.channels_per_unit = 0;
        config.ensure_ring = false;
        config.median_work = 16;
        config.working_set_bytes = 4096;
        config.drain_limit = 1;
        return config;
    }
    if (profile == "port") {
        config.channels_per_unit = 3;
        config.median_work = 1;
        config.unit_sigma_milli = 100;
        config.cycle_sigma_milli = 0;
        config.send_probability_ppm = 450'000;
        config.drain_limit = 32;
        config.queue_capacity = 1024;
        return config;
    }
    if (profile == "hotspot") {
        config.channels_per_unit = 3;
        config.hotspot_probability_ppm = 750'000;
        config.send_probability_ppm = 300'000;
        config.queue_capacity = 128;
        config.drain_limit = 4;
        return config;
    }
    if (profile == "broadcast") {
        config.channels_per_unit = 1;
        config.active_source_count = 1;
        config.ensure_ring = false;
        config.broadcast_probability_ppm = 1'000'000;
        config.hotspot_probability_ppm = 0;
        config.max_fanout = 16;
        config.forced_delay = 1;
        config.zero_delay_probability_ppm = 0;
        config.send_probability_ppm = 250'000;
        config.queue_capacity = 0;
        config.drain_limit = 32;
        return config;
    }
    if (profile == "backpressure") {
        config.channels_per_unit = 3;
        config.send_probability_ppm = 500'000;
        config.burst_probability_ppm = 15'000;
        config.burst_length = 8;
        config.hotspot_probability_ppm = 500'000;
        config.queue_capacity = 64;
        config.drain_limit = 2;
        return config;
    }
    if (profile == "saturation") {
        config.channels_per_unit = 3;
        config.send_probability_ppm = 700'000;
        config.burst_probability_ppm = 30'000;
        config.burst_length = 8;
        config.hotspot_probability_ppm = 600'000;
        config.queue_capacity = 32;
        config.drain_limit = 1;
        return config;
    }
    if (profile == "random") {
        config.num_units = 32 + bounded(randomWord(seed, 1, 0), 97);
        // Keep one ring channel for connectivity and at least one independently
        // generated channel for topology, hotspot, and fan-out coverage.
        config.channels_per_unit = 2 + bounded(randomWord(seed, 1, 1), 3);
        config.max_fanout = 2 + bounded(randomWord(seed, 1, 2), 7);
        config.send_probability_ppm = 50'000 + bounded(randomWord(seed, 1, 3), 600'001);
        config.hotspot_probability_ppm = bounded(randomWord(seed, 1, 4), 700'001);
        config.broadcast_probability_ppm = bounded(randomWord(seed, 1, 5), 600'001);
        config.zero_delay_probability_ppm = bounded(randomWord(seed, 1, 6), 150'001);
        constexpr std::array<uint32_t, 4> capacities = {32, 128, 512, 0};
        config.queue_capacity = capacities[bounded(randomWord(seed, 1, 7), capacities.size())];
        config.median_work = 4 + bounded(randomWord(seed, 1, 8), 61);
        config.unit_sigma_milli = 250 + bounded(randomWord(seed, 1, 9), 651);
        config.cycle_sigma_milli = 50 + bounded(randomWord(seed, 1, 10), 351);
        config.drain_limit = 1 + bounded(randomWord(seed, 1, 11), 16);
        return config;
    }
    throw std::invalid_argument("unknown profile: " + std::string(profile));
}

void printHelp(const char* program) {
    std::cout << "Usage: " << program << " [options]\n\n"
              << "Profiles: nucleus, scheduler, port, hotspot, broadcast, backpressure, "
                 "saturation, random\n"
              << "Core options:\n"
              << "  --profile NAME              scenario profile (default nucleus)\n"
              << "  --seed N | --random-seed    reproducible or newly sampled base seed\n"
              << "  --scenario-offset N         first derived scenario index (default 0)\n"
              << "  --scenario-count N          independent derived scenarios (default 1)\n"
              << "  --threads 1,2,4,8           worker sweep; CPU affinity is external\n"
              << "  --cycles N --warmup N       measured and warmup cycles\n"
              << "  --repetitions N             interleaved repetitions (default 5)\n"
              << "  --rebalance                 enable runtime dynamic rebalance\n"
              << "  --no-precomputed-costs      start from uniform unit costs\n"
              << "  --describe-only             generate/validate without running\n"
              << "Scenario overrides:\n"
              << "  --units N --channels N --max-fanout N --send-ppm N --burst-ppm N\n"
              << "  --active-sources N --fixed-delay N\n"
              << "  --hotspot-ppm N --broadcast-ppm N --zero-delay-ppm N\n"
              << "  --queue-capacity N --drain-limit N --work N --unit-sigma N\n"
              << "  --cycle-sigma N --working-set N --payload-weights a,b,c,d,e,f\n"
              << "Other: --max-lookahead N --verbose --quick --help\n";
}

struct ParsedOptions {
    CliOptions cli;
    ScenarioConfig scenario;
};

[[nodiscard]] ParsedOptions parseCommandLine(int argc, char** argv) {
    CliOptions cli;
    for (int i = 1; i < argc; ++i) {
        const std::string_view option = argv[i];
        if (option == "--profile" && i + 1 < argc)
            cli.profile = argv[++i];
        else if (option == "--seed" && i + 1 < argc)
            cli.seed = parseInteger(argv[++i], option);
        else if (option == "--random-seed")
            cli.random_seed = true;
    }
    if (cli.random_seed) {
        std::random_device device;
        cli.seed = (static_cast<uint64_t>(device()) << 32) ^ device();
    }
    ScenarioConfig scenario = profileConfig(cli.profile, cli.seed);

    auto requireValue = [&](int& index, std::string_view option) -> std::string_view {
        if (index + 1 >= argc)
            throw std::invalid_argument("missing value for " + std::string(option));
        return argv[++index];
    };
    for (int i = 1; i < argc; ++i) {
        const std::string_view option = argv[i];
        if (option == "--help") {
            printHelp(argv[0]);
            std::exit(0);
        } else if (option == "--profile" || option == "--seed") {
            ++i;
        } else if (option == "--random-seed") {
        } else if (option == "--scenario-offset") {
            cli.scenario_offset = parseInteger(requireValue(i, option), option);
        } else if (option == "--scenario-count") {
            cli.scenario_count = parseInteger(requireValue(i, option), option);
        } else if (option == "--threads") {
            cli.workers = parseWorkers(requireValue(i, option));
        } else if (option == "--cycles") {
            cli.run.measured_cycles = parseInteger(requireValue(i, option), option);
        } else if (option == "--warmup") {
            cli.run.warmup_cycles = parseInteger(requireValue(i, option), option);
        } else if (option == "--repetitions") {
            cli.repetitions = parseInteger(requireValue(i, option), option);
        } else if (option == "--max-lookahead") {
            cli.run.max_lookahead = parseInteger(requireValue(i, option), option);
        } else if (option == "--rebalance") {
            cli.run.dynamic_rebalance = true;
        } else if (option == "--no-precomputed-costs") {
            cli.run.precomputed_costs = false;
        } else if (option == "--describe-only") {
            cli.describe_only = true;
        } else if (option == "--verbose") {
            cli.verbose = true;
        } else if (option == "--quick") {
            cli.run.warmup_cycles = 256;
            cli.run.measured_cycles = 2'000;
            cli.repetitions = 1;
        } else if (option == "--units") {
            scenario.num_units = parseInteger(requireValue(i, option), option);
        } else if (option == "--channels") {
            scenario.channels_per_unit = parseInteger(requireValue(i, option), option);
        } else if (option == "--active-sources") {
            scenario.active_source_count = parseInteger(requireValue(i, option), option);
        } else if (option == "--fixed-delay") {
            scenario.forced_delay = parseInteger(requireValue(i, option), option);
        } else if (option == "--max-fanout") {
            scenario.max_fanout = parseInteger(requireValue(i, option), option);
        } else if (option == "--send-ppm") {
            scenario.send_probability_ppm = parseInteger(requireValue(i, option), option);
        } else if (option == "--burst-ppm") {
            scenario.burst_probability_ppm = parseInteger(requireValue(i, option), option);
        } else if (option == "--hotspot-ppm") {
            scenario.hotspot_probability_ppm = parseInteger(requireValue(i, option), option);
        } else if (option == "--broadcast-ppm") {
            scenario.broadcast_probability_ppm = parseInteger(requireValue(i, option), option);
        } else if (option == "--zero-delay-ppm") {
            scenario.zero_delay_probability_ppm = parseInteger(requireValue(i, option), option);
        } else if (option == "--queue-capacity") {
            scenario.queue_capacity = parseInteger(requireValue(i, option), option);
        } else if (option == "--drain-limit") {
            scenario.drain_limit = parseInteger(requireValue(i, option), option);
        } else if (option == "--work") {
            scenario.median_work = parseInteger(requireValue(i, option), option);
        } else if (option == "--unit-sigma") {
            scenario.unit_sigma_milli = parseInteger(requireValue(i, option), option);
        } else if (option == "--cycle-sigma") {
            scenario.cycle_sigma_milli = parseInteger(requireValue(i, option), option);
        } else if (option == "--working-set") {
            scenario.working_set_bytes = parseInteger(requireValue(i, option), option);
        } else if (option == "--payload-weights") {
            const auto weights = parseU32List(requireValue(i, option));
            if (weights.size() != scenario.payload_weights.size()) {
                throw std::invalid_argument(
                    "--payload-weights requires six comma-separated values");
            }
            std::copy(weights.begin(), weights.end(), scenario.payload_weights.begin());
        } else {
            throw std::invalid_argument("unknown option: " + std::string(option));
        }
    }
    if (cli.scenario_count == 0 || cli.repetitions == 0 || cli.run.measured_cycles == 0) {
        throw std::invalid_argument(
            "scenario count, repetitions, and measured cycles must be positive");
    }
    return {std::move(cli), std::move(scenario)};
}

void printScenario(const Scenario& scenario, std::string_view profile, uint64_t scenario_index) {
    uint64_t edges = 0;
    for (const auto& channel : scenario.channels) edges += channel.destinations.size();
    const double period = scenario.config.send_period;
    std::cout << "\nscenario profile=" << profile << " version=" << REPRESENTATIVE_WORKLOAD_VERSION
              << " index=" << scenario_index << " seed=" << scenario.config.seed
              << " fingerprint=0x" << std::hex << scenario.fingerprint << std::dec << '\n';
    std::cout << "  units=" << scenario.units.size() << " channels=" << scenario.channels.size()
              << " edges=" << edges << " zero-delay=" << scenario.summary.zero_delay_channels
              << " broadcasts=" << scenario.summary.broadcast_channels
              << " max-in/out=" << scenario.summary.max_indegree << '/'
              << scenario.summary.max_outdegree << '\n';
    std::cout << "  work median=" << scenario.config.median_work
              << " unit-sigma=" << scenario.config.unit_sigma_milli / 1000.0
              << " cycle-sigma=" << scenario.config.cycle_sigma_milli / 1000.0
              << " z-cap=" << scenario.config.normal_cap_milli / 1000.0
              << " working-set-base=" << scenario.config.working_set_bytes << "B\n";
    std::cout << "  offered publications/cycle=" << scenario.summary.scheduled_slots / period
              << " deliveries/cycle=" << scenario.summary.scheduled_deliveries / period
              << " payload-KiB/cycle=" << scenario.summary.scheduled_payload_bytes / period / 1024.0
              << " queue-capacity=" << scenario.config.queue_capacity << '\n';
    std::cout << "  payload weights [8,16,32,64,144,256]=";
    for (size_t i = 0; i < scenario.config.payload_weights.size(); ++i) {
        if (i != 0) std::cout << ',';
        std::cout << scenario.config.payload_weights[i];
    }
    std::cout << '\n';
}

void printScenarioOverrides(const ScenarioConfig& config, const ScenarioConfig& profile) {
    const auto scalar = [&](std::string_view option, auto value, auto default_value) {
        if (value != default_value) std::cout << ' ' << option << ' ' << value;
    };
    scalar("--units", config.num_units, profile.num_units);
    scalar("--channels", config.channels_per_unit, profile.channels_per_unit);
    scalar("--active-sources", config.active_source_count, profile.active_source_count);
    scalar("--fixed-delay", config.forced_delay, profile.forced_delay);
    scalar("--max-fanout", config.max_fanout, profile.max_fanout);
    scalar("--send-ppm", config.send_probability_ppm, profile.send_probability_ppm);
    scalar("--burst-ppm", config.burst_probability_ppm, profile.burst_probability_ppm);
    scalar("--hotspot-ppm", config.hotspot_probability_ppm, profile.hotspot_probability_ppm);
    scalar("--broadcast-ppm", config.broadcast_probability_ppm, profile.broadcast_probability_ppm);
    scalar("--zero-delay-ppm", config.zero_delay_probability_ppm,
           profile.zero_delay_probability_ppm);
    scalar("--queue-capacity", config.queue_capacity, profile.queue_capacity);
    scalar("--drain-limit", config.drain_limit, profile.drain_limit);
    scalar("--work", config.median_work, profile.median_work);
    scalar("--unit-sigma", config.unit_sigma_milli, profile.unit_sigma_milli);
    scalar("--cycle-sigma", config.cycle_sigma_milli, profile.cycle_sigma_milli);
    scalar("--working-set", config.working_set_bytes, profile.working_set_bytes);
    if (config.payload_weights != profile.payload_weights) {
        std::cout << " --payload-weights ";
        for (size_t i = 0; i < config.payload_weights.size(); ++i) {
            if (i != 0) std::cout << ',';
            std::cout << config.payload_weights[i];
        }
    }
}

[[nodiscard]] double percentile(std::vector<double> values, double quantile) {
    std::sort(values.begin(), values.end());
    if (values.size() == 1) return values.front();
    const double position = quantile * static_cast<double>(values.size() - 1);
    const size_t lower = static_cast<size_t>(position);
    const size_t upper = std::min(values.size() - 1, lower + 1);
    const double fraction = position - static_cast<double>(lower);
    return values[lower] * (1.0 - fraction) + values[upper] * fraction;
}

[[nodiscard]] double coefficientOfVariation(const std::vector<double>& values) {
    const double mean = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
    if (values.size() < 2 || mean == 0.0) return 0.0;
    double squared = 0.0;
    for (double value : values) squared += (value - mean) * (value - mean);
    return std::sqrt(squared / static_cast<double>(values.size() - 1)) / mean;
}

void validateEquivalent(const RunResult& reference, const RunResult& candidate) {
    if (reference.final.state_digest != candidate.final.state_digest ||
        reference.final.sent_deliveries != candidate.final.sent_deliveries ||
        reference.final.received != candidate.final.received ||
        reference.final.queued != candidate.final.queued ||
        reference.final.pending_publications != candidate.final.pending_publications ||
        reference.measured.work_iterations != candidate.measured.work_iterations) {
        throw std::runtime_error("determinism check failed across repetitions/worker counts");
    }
}

void printResults(const std::vector<size_t>& workers,
                  const std::vector<std::vector<RunResult>>& results, uint32_t num_units,
                  uint64_t cycles) {
    std::cout << "\nworkers | median(s) | p10..p90(s)     | CV     | Munit-tick/s | Mmsg/s | GiB/s "
                 "| blocked | speedup | mode\n";
    std::cout << "--------+-----------+-----------------+--------+--------------+--------+-------+-"
                 "--------+---------+------\n";
    double baseline = 0.0;
    for (size_t index = 0; index < workers.size(); ++index) {
        std::vector<double> walls;
        for (const auto& result : results[index]) walls.push_back(result.wall_seconds);
        const double median = percentile(walls, 0.5);
        const double p10 = percentile(walls, 0.1);
        const double p90 = percentile(walls, 0.9);
        if (workers[index] == 1 || baseline == 0.0) baseline = median;
        const auto& counters = results[index].front().measured;
        const double mticks = static_cast<double>(num_units) * cycles / median / 1e6;
        const double messages = static_cast<double>(counters.received) / median / 1e6;
        const double gib =
            static_cast<double>(counters.received_bytes) / median / (1024.0 * 1024.0 * 1024.0);
        const double blocked = counters.send_attempts == 0
                                   ? 0.0
                                   : 100.0 * counters.blocked_attempts / counters.send_attempts;
        const auto& first = results[index].front();
        const char* mode = !first.parallel ? "seq" : (first.epoch_free ? "epoch" : "barrier");
        std::printf(
            "%7zu | %9.4f | %7.4f..%-7.4f | %6.2f%% | %12.2f | %6.2f | %5.2f | %6.2f%% | %7.2fx | "
            "%s\n",
            workers[index], median, p10, p90, 100.0 * coefficientOfVariation(walls), mticks,
            messages, gib, blocked, baseline / median, mode);
    }
    const auto& representative = results.front().front();
    std::cout << "  digest=0x" << std::hex << representative.final.state_digest << std::dec
              << " sent=" << representative.final.sent_deliveries
              << " received=" << representative.final.received
              << " queued=" << representative.final.queued
              << " pending-publishes=" << representative.final.pending_publications
              << " transparent-broadcast-edges=" << representative.transparent_broadcast_connections
              << '\n';
}

void shuffleOrder(std::vector<size_t>& order, uint64_t seed, uint32_t repetition) {
    for (size_t position = order.size(); position > 1; --position) {
        const size_t other =
            bounded(randomWord(seed, 0x494e5445524c4541ULL,
                               static_cast<uint64_t>(repetition) * 1024 + position),
                    position);
        std::swap(order[position - 1], order[other]);
    }
}

}  // namespace
}  // namespace chronon::benchmark

int main(int argc, char** argv) {
    using namespace chronon::benchmark;
    try {
        auto parsed = parseCommandLine(argc, argv);
        std::cout << "=== Chronon representative workload benchmark ===\n"
                  << "base-seed=" << parsed.cli.seed << " scenarios=" << parsed.cli.scenario_count
                  << " scenario-offset=" << parsed.cli.scenario_offset
                  << " repetitions=" << parsed.cli.repetitions
                  << " measured-cycles=" << parsed.cli.run.measured_cycles
                  << " warmup-cycles=" << parsed.cli.run.warmup_cycles
                  << " hw-concurrency=" << std::thread::hardware_concurrency() << '\n'
                  << "affinity is intentionally external; use taskset/hwloc to select homogeneous "
                     "physical cores\n";

        for (uint32_t scenario_index = 0; scenario_index < parsed.cli.scenario_count;
             ++scenario_index) {
            const uint64_t global_scenario_index =
                parsed.cli.scenario_offset + static_cast<uint64_t>(scenario_index);
            ScenarioConfig config = parsed.scenario;
            config.seed = global_scenario_index == 0
                              ? parsed.cli.seed
                              : splitMix64(parsed.cli.seed + global_scenario_index);
            const Scenario scenario = generateScenario(config);
            printScenario(scenario, parsed.cli.profile, global_scenario_index);
            std::cout << "  scenario-replay: " << argv[0] << " --profile " << parsed.cli.profile
                      << " --seed " << parsed.cli.seed << " --scenario-offset "
                      << global_scenario_index << " --scenario-count 1";
            printScenarioOverrides(parsed.scenario,
                                   profileConfig(parsed.cli.profile, parsed.cli.seed));
            std::cout << '\n';
            if (parsed.cli.describe_only) continue;

            std::vector<std::vector<RunResult>> results(parsed.cli.workers.size());
            std::optional<RunResult> reference;
            for (uint32_t repetition = 0; repetition < parsed.cli.repetitions; ++repetition) {
                std::vector<size_t> order(parsed.cli.workers.size());
                std::iota(order.begin(), order.end(), 0);
                shuffleOrder(order, config.seed, repetition);
                for (size_t worker_index : order) {
                    const size_t workers = parsed.cli.workers[worker_index];
                    if (parsed.cli.verbose) {
                        std::cout << "  run repetition=" << repetition << " workers=" << workers
                                  << std::flush;
                    }
                    RunResult result = runOnce(scenario, workers, parsed.cli.run);
                    if (parsed.cli.verbose) {
                        std::cout << " wall=" << result.wall_seconds << "s digest=0x" << std::hex
                                  << result.final.state_digest << std::dec << '\n';
                    }
                    if (reference)
                        validateEquivalent(*reference, result);
                    else
                        reference = result;
                    results[worker_index].push_back(std::move(result));
                }
            }
            printResults(parsed.cli.workers, results, config.num_units,
                         parsed.cli.run.measured_cycles);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ERROR: " << error.what() << '\n';
        return 2;
    }
}
