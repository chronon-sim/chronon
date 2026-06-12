// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "CategoryPatternMatcher.hpp"
#include "Counter.hpp"
#include "CounterRegistry.hpp"
#include "ObservableUnit.hpp"
#include "ObservationBackend.hpp"
#include "ObservationContext.hpp"
#include "ObservationQueue.hpp"
#include "ObservationYAMLConfig.hpp"
#include "ObserveApi.hpp"
#include "SourceNameRegistry.hpp"
#include "Types.hpp"

namespace chronon::observe {

/**
 * @brief Central singleton coordinating observability setup.
 *
 * Initializes infrastructure from YAML configuration, creates and configures
 * per-unit ObservationContexts, resolves category patterns, and manages backend
 * lifecycle.
 *
 * @code
 *   auto& obs = ObservationManager::instance();
 *   obs.initialize(config);
 *   auto* ctx = obs.createContextForUnit("my_unit", cycle_provider);
 *   unit->setObservationContext(ctx);
 *   obs.startBackend();
 *   // ... run simulation ...
 *   obs.stopBackend();
 *   obs.shutdown();
 * @endcode
 */
class ObservationManager {
public:
    static ObservationManager& instance();

    /**
     * @brief Initialize from YAML config.
     *
     * Creates the shared queue and backend; does not start the backend yet.
     */
    void initialize(const ObservationYAMLConfig& config);

    bool isEnabled() const noexcept { return enabled_; }
    bool isInitialized() const noexcept { return initialized_; }

    /**
     * @brief Create an ObservationContext for a unit.
     *
     * The context is configured with global YAML settings, per-unit overrides,
     * and category temporal filters.
     *
     * @return Non-owning pointer; ObservationManager owns the context.
     */
    ObservationContext* createContextForUnit(const std::string& unit_name,
                                             std::function<uint64_t()> cycle_provider,
                                             uint32_t thread_id = 0);

    /**
     * @brief Re-register all counters from all contexts.
     *
     * PRECONDITION: all units have been created and their Counters initialized.
     * Counters are added after context creation, so this must be re-run to make
     * them visible for periodic dumps.
     */
    void reregisterAllCounters();

    void startBackend();
    /// Drains remaining events before stopping.
    void stopBackend();
    bool isBackendRunning() const noexcept;

    /// True when the backend writes a unified Perfetto timeline (timeline.pftrace).
    bool timelineEnabled() const noexcept;

    /**
     * @brief Hand recorded timeline streams to the backend for timeline.pftrace.
     *
     * The streams are written during stopBackend(), after the final event drain.
     * @return false when the backend is not running or has no timeline sink —
     *         the caller should fall back to standalone output.
     */
    bool submitTimeline(TimelineStreamData&& data);

    /// Shared queue used for counter snapshots and lookahead commits.
    ObservationQueue* sharedQueue() noexcept { return shared_queue_.get(); }

    ObservationBackend* backend() noexcept { return backend_.get(); }

    const std::string& outputDir() const noexcept { return config_.output_dir; }
    const ObservationYAMLConfig& config() const noexcept { return config_; }

    CounterRegistry& counterRegistry() noexcept { return counter_registry_; }

    /**
     * @brief Emit COUNTER_SNAPSHOT events for all registered counters at @p cycle.
     *
     * Uses the registration-based pull model: reads from registered counter
     * addresses rather than iterating context storage. Thread-safe.
     */
    void dumpCounterSnapshots(uint64_t cycle);

    /// @return Dump interval in cycles (0 = disabled).
    uint64_t periodicDumpCycles() const noexcept { return config_.counters.periodic_dump_cycles; }

    bool dumpOnShutdown() const noexcept { return config_.counters.dump_on_shutdown; }

    /**
     * @brief Register a unit name and obtain its source_id.
     * @return 1-based source ID (0 reserved for "unknown").
     */
    uint16_t registerSourceName(const std::string& name);

    /// Lock-free lookup; returns empty view if not found.
    std::string_view getSourceName(uint16_t source_id) const noexcept;

    /// Stops the backend and releases all resources; initialize() must be called again to reuse.
    void shutdown();

    /// Equivalent to shutdown() plus full state clear; intended for tests.
    void reset();

    void printReport(std::ostream& out) const;

    size_t contextCount() const noexcept;

private:
    ObservationManager() = default;
    ~ObservationManager();

    ObservationManager(const ObservationManager&) = delete;
    ObservationManager& operator=(const ObservationManager&) = delete;

    void applyConfigToContext(ObservationContext& ctx, const std::string& unit_name);
    void configureUnified(ObservationContext& ctx, const UnifiedLoggingConfig& unified_config);
    CategoryMask resolvePattern(const std::string& pattern);

    /// PRECONDITION: mutex_ held.
    void shutdownLocked_();

    bool enabled_ = false;
    bool initialized_ = false;
    ObservationYAMLConfig config_;

    std::unique_ptr<ObservationQueue> shared_queue_;
    std::unique_ptr<ObservationBackend> backend_;

    std::vector<std::unique_ptr<ObservationContext>> contexts_;

    CounterRegistry counter_registry_;
    SourceNameRegistry source_registry_;

    mutable std::mutex mutex_;
};

}  // namespace chronon::observe
