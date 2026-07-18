// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "sender/core/TickSimulation.hpp"

namespace chronon::sender {

/**
 * White-box seam for deterministic migration tests.
 *
 * The operation is legal only after initialize() and between completed run()
 * spans. No worker is alive at that point, so the test does not add a hook,
 * branch, lock, or callback to the production scheduler hot path.
 */
struct EpochFreeDifferentialTestAccess {
    static bool migrateAtRunBoundary(TickSimulation& simulation, Unit* unit, size_t target_thread) {
        return simulation.forceEpochFreeMigrationAtBoundary_(unit, target_thread);
    }

    static std::string vetoReason(const TickSimulation& simulation) {
        return simulation.epochFreeVetoReason_();
    }
};

namespace test {

enum class ModelEventKind : uint32_t {
    Tick = 0,
    SendResult,
    Receive,
    State,
    Flush,
    Backpressure,
    Wake,
    Signal,
};

struct CanonicalEvent {
    uint64_t cycle = 0;
    uint64_t value0 = 0;
    uint64_t value1 = 0;
    uint32_t component = 0;
    uint32_t sequence = 0;
    ModelEventKind kind = ModelEventKind::Tick;

    friend bool operator==(const CanonicalEvent&, const CanonicalEvent&) = default;
};

inline bool canonicalEventLess(const CanonicalEvent& lhs, const CanonicalEvent& rhs) noexcept {
    return std::tie(lhs.cycle, lhs.component, lhs.sequence, lhs.kind, lhs.value0, lhs.value1) <
           std::tie(rhs.cycle, rhs.component, rhs.sequence, rhs.kind, rhs.value0, rhs.value1);
}

/**
 * Single-writer event stream owned by one simulated Unit.
 *
 * A Unit is never ticked concurrently, including across cluster migration, so
 * recording needs neither atomics nor locks. Tests reserve expected storage up
 * front to keep vector growth out of their measured tick bodies as well.
 */
class UnitEventLog {
public:
    explicit UnitEventLog(uint32_t component) : component_(component) {}

    void reserve(size_t events) { events_.reserve(events); }

    void record(uint64_t cycle, ModelEventKind kind, uint64_t value0 = 0, uint64_t value1 = 0) {
        if (last_cycle_ != std::numeric_limits<uint64_t>::max() && cycle < last_cycle_) {
            throw std::logic_error("canonical Unit event cycle moved backward");
        }
        if (cycle != last_cycle_) {
            last_cycle_ = cycle;
            next_sequence_ = 0;
        }
        events_.push_back(CanonicalEvent{.cycle = cycle,
                                         .value0 = value0,
                                         .value1 = value1,
                                         .component = component_,
                                         .sequence = next_sequence_++,
                                         .kind = kind});
    }

    std::span<const CanonicalEvent> events() const noexcept { return events_; }

private:
    uint32_t component_ = 0;
    uint32_t next_sequence_ = 0;
    uint64_t last_cycle_ = std::numeric_limits<uint64_t>::max();
    std::vector<CanonicalEvent> events_;
};

inline std::vector<CanonicalEvent> canonicalizeEvents(std::span<UnitEventLog* const> logs) {
    size_t count = 0;
    for (const UnitEventLog* log : logs) {
        count += log->events().size();
    }

    std::vector<CanonicalEvent> events;
    events.reserve(count);
    for (const UnitEventLog* log : logs) {
        const auto source = log->events();
        events.insert(events.end(), source.begin(), source.end());
    }
    std::sort(events.begin(), events.end(), canonicalEventLess);
    return events;
}

struct ForcedMigration {
    uint64_t cycle = 0;
    std::string unit_name;
    /// SIZE_MAX means the next worker after the cluster's current owner.
    size_t target_thread = SIZE_MAX;
};

enum class EpochFreeRunKind : uint8_t {
    SequentialReference,
    Static,
    ForcedMigration,
    RuntimeRebalance,
};

struct EpochFreeRunMode {
    std::string name;
    EpochFreeRunKind kind = EpochFreeRunKind::Static;
    size_t num_threads = 2;
    std::vector<ForcedMigration> migrations;
};

struct RunArtifact {
    std::string mode_name;
    std::vector<std::string> component_names;
    std::vector<CanonicalEvent> events;
    uint64_t executed_cycles = 0;
    uint64_t epoch_free_runs = 0;
    uint64_t rebalance_count = 0;
    size_t forced_migrations_applied = 0;
    uint64_t digest = 0;
};

inline uint64_t canonicalDigest(const RunArtifact& artifact) noexcept {
    uint64_t hash = 0x6a09e667f3bcc909ULL;
    auto mix = [&hash](uint64_t value) {
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27;
        value *= 0x94d049bb133111ebULL;
        value ^= value >> 31;
        hash ^= value + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    };

    mix(artifact.executed_cycles);
    mix(artifact.component_names.size());
    for (const auto& name : artifact.component_names) {
        mix(name.size());
        for (unsigned char byte : name) mix(byte);
    }
    mix(artifact.events.size());
    for (const auto& event : artifact.events) {
        mix(event.cycle);
        mix(event.component);
        mix(event.sequence);
        mix(static_cast<uint32_t>(event.kind));
        mix(event.value0);
        mix(event.value1);
    }
    return hash;
}

inline const char* eventKindName(ModelEventKind kind) noexcept {
    switch (kind) {
        case ModelEventKind::Tick:
            return "tick";
        case ModelEventKind::SendResult:
            return "send-result";
        case ModelEventKind::Receive:
            return "receive";
        case ModelEventKind::State:
            return "state";
        case ModelEventKind::Flush:
            return "flush";
        case ModelEventKind::Backpressure:
            return "backpressure";
        case ModelEventKind::Wake:
            return "wake";
        case ModelEventKind::Signal:
            return "signal";
    }
    return "unknown";
}

inline std::string describeEvent(const RunArtifact& artifact, const CanonicalEvent& event) {
    std::ostringstream out;
    const std::string component = event.component < artifact.component_names.size()
                                      ? artifact.component_names[event.component]
                                      : "component#" + std::to_string(event.component);
    out << "{cycle=" << event.cycle << ", component=" << component
        << ", sequence=" << event.sequence << ", kind=" << eventKindName(event.kind)
        << ", value0=" << event.value0 << ", value1=" << event.value1 << '}';
    return out.str();
}

struct MatrixComparison {
    bool equivalent = true;
    uint64_t first_divergent_cycle = std::numeric_limits<uint64_t>::max();
    std::string diagnostic;
};

inline MatrixComparison compareArtifacts(const RunArtifact& reference,
                                         const RunArtifact& candidate) {
    if (reference.executed_cycles != candidate.executed_cycles) {
        return {false, std::min(reference.executed_cycles, candidate.executed_cycles),
                "mode=" + candidate.mode_name + " executed " +
                    std::to_string(candidate.executed_cycles) + " cycles; reference executed " +
                    std::to_string(reference.executed_cycles)};
    }
    if (reference.component_names != candidate.component_names) {
        return {false, 0,
                "mode=" + candidate.mode_name + " produced a different canonical component table"};
    }

    const size_t common = std::min(reference.events.size(), candidate.events.size());
    for (size_t i = 0; i < common; ++i) {
        if (reference.events[i] == candidate.events[i]) continue;
        const uint64_t cycle = std::min(reference.events[i].cycle, candidate.events[i].cycle);
        return {false, cycle,
                "mode=" + candidate.mode_name +
                    " first model-event divergence at index=" + std::to_string(i) +
                    " expected=" + describeEvent(reference, reference.events[i]) +
                    " actual=" + describeEvent(candidate, candidate.events[i])};
    }
    if (reference.events.size() != candidate.events.size()) {
        const bool candidate_short = candidate.events.size() < reference.events.size();
        const auto& event = candidate_short ? reference.events[common] : candidate.events[common];
        return {false, event.cycle,
                "mode=" + candidate.mode_name +
                    (candidate_short ? " ended before expected event " : " emitted extra event ") +
                    (candidate_short ? describeEvent(reference, event)
                                     : describeEvent(candidate, event))};
    }
    return {};
}

inline MatrixComparison compareMatrix(std::span<const RunArtifact> artifacts) {
    if (artifacts.empty()) {
        return {false, 0, "differential matrix is empty"};
    }
    for (size_t i = 1; i < artifacts.size(); ++i) {
        auto comparison = compareArtifacts(artifacts.front(), artifacts[i]);
        if (!comparison.equivalent) return comparison;
    }
    return {};
}

inline TickSimulationConfig configForMode(const TickSimulationConfig& base,
                                          const EpochFreeRunMode& mode) {
    TickSimulationConfig config = base;
    if (mode.kind == EpochFreeRunKind::SequentialReference) {
        config.num_threads = 1;
        config.enable_parallel = false;
        config.enable_lookahead = false;
        config.enable_dynamic_rebalance = false;
        return config;
    }

    if (mode.num_threads < 2) {
        throw std::invalid_argument("epoch-free differential mode requires at least two workers");
    }
    config.num_threads = mode.num_threads;
    config.enable_parallel = true;
    config.enable_lookahead = true;
    config.enable_epoch_free_lookahead = true;
    config.enable_dynamic_rebalance = mode.kind != EpochFreeRunKind::Static;

    if (mode.kind == EpochFreeRunKind::ForcedMigration) {
        // Keep the dynamic ownership driver but suppress wall-time-driven
        // placement. The harness applies the declared migrations only at
        // quiescent run boundaries.
        config.rebalance_check_interval_cycles = std::numeric_limits<uint64_t>::max();
        config.rebalance_imbalance_threshold = std::numeric_limits<double>::max();
        config.rebalance_min_gain = std::numeric_limits<double>::max();
        config.rebalance_cooldown_cycles = std::numeric_limits<uint64_t>::max();
    }
    return config;
}

/**
 * Run one freshly-built scenario in the sequential reference and each
 * requested epoch-free configuration.
 *
 * Factory signature:
 *   auto factory(TickSimulation&, const EpochFreeRunMode&)
 *
 * The returned scenario object must provide:
 *   std::vector<std::string> componentNames() const;
 *   std::vector<CanonicalEvent> canonicalEvents() const;
 */
template <typename Factory>
std::vector<RunArtifact> runEpochFreeMatrix(const TickSimulationConfig& base_config,
                                            uint64_t total_cycles,
                                            std::span<const EpochFreeRunMode> modes,
                                            Factory&& factory) {
    std::vector<RunArtifact> artifacts;
    artifacts.reserve(modes.size());

    for (const auto& mode : modes) {
        TickSimulation simulation(configForMode(base_config, mode));
        auto scenario = std::invoke(factory, simulation, mode);
        simulation.initialize();

        uint64_t completed = 0;
        size_t forced_migrations = 0;
        auto run_segment = [&](uint64_t cycles) {
            if (cycles == 0) return;
            const uint64_t epoch_free_before = simulation.epochFreeRunCount();
            const uint64_t executed = simulation.run(cycles);
            if (executed != cycles) {
                throw std::runtime_error("mode=" + mode.name + " stopped after " +
                                         std::to_string(executed) + " of " +
                                         std::to_string(cycles) + " requested cycles");
            }
            if (mode.kind != EpochFreeRunKind::SequentialReference &&
                simulation.epochFreeRunCount() == epoch_free_before) {
                throw std::runtime_error("mode=" + mode.name +
                                         " fell back from epoch-free lookahead: " +
                                         EpochFreeDifferentialTestAccess::vetoReason(simulation));
            }
            completed += executed;
        };

        for (const auto& migration : mode.migrations) {
            if (mode.kind != EpochFreeRunKind::ForcedMigration) {
                throw std::invalid_argument("forced migration attached to non-forced mode=" +
                                            mode.name);
            }
            if (migration.cycle < completed || migration.cycle > total_cycles) {
                throw std::invalid_argument("out-of-order forced migration in mode=" + mode.name);
            }
            run_segment(migration.cycle - completed);

            Unit* anchor = simulation.getUnit(migration.unit_name);
            const size_t source = simulation.assignedThread(anchor);
            if (!anchor || source == SIZE_MAX || mode.num_threads == 0) {
                throw std::runtime_error("cannot resolve migration anchor=" + migration.unit_name +
                                         " in mode=" + mode.name);
            }
            const size_t target = migration.target_thread == SIZE_MAX
                                      ? (source + 1) % mode.num_threads
                                      : migration.target_thread;
            if (!EpochFreeDifferentialTestAccess::migrateAtRunBoundary(simulation, anchor,
                                                                       target)) {
                throw std::runtime_error("rejected forced migration for anchor=" +
                                         migration.unit_name + " in mode=" + mode.name);
            }
            ++forced_migrations;
        }
        run_segment(total_cycles - completed);

        RunArtifact artifact;
        artifact.mode_name = mode.name;
        artifact.component_names = scenario->componentNames();
        artifact.events = scenario->canonicalEvents();
        std::sort(artifact.events.begin(), artifact.events.end(), canonicalEventLess);
        artifact.executed_cycles = completed;
        artifact.epoch_free_runs = simulation.epochFreeRunCount();
        artifact.rebalance_count = simulation.rebalanceCount();
        artifact.forced_migrations_applied = forced_migrations;
        artifact.digest = canonicalDigest(artifact);
        artifacts.push_back(std::move(artifact));
    }
    return artifacts;
}

}  // namespace test
}  // namespace chronon::sender
