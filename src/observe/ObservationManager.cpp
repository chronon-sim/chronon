// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "ObservationManager.hpp"

#include <iostream>
#include <stdexcept>
#include <thread>

#include "ObserveApi.hpp"
#include "ThreadContextManager.hpp"

namespace chronon::observe {

ObservationManager& ObservationManager::instance() {
    static ObservationManager mgr;
    return mgr;
}

ObservationManager::ObservationManager() {
    // Construct the queue pool before registering this singleton's destructor.
    // The backend's final drain must finish before cached queues are destroyed,
    // including when the application relies on automatic process-exit cleanup.
    (void)ThreadContextManager::instance();
}

ObservationManager::~ObservationManager() {
    try {
        shutdown();
    } catch (...) {
        // The backend already reported the failure; destructors cannot propagate it.
    }
}

void ObservationManager::initialize(const ObservationYAMLConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_owner_)
        throw std::logic_error("observation backend belongs to a simulation session");
    initializeLocked_(config);
}

void ObservationManager::acquireSession(const ObservationYAMLConfig& config, const void* owner) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!owner || session_owner_ || initialized_ || initialized_simulation_count_ != 0)
        throw std::logic_error(
            "observation requires an exclusive simulation session; destroy the previous session "
            "first");
    try {
        initializeLocked_(config);
    } catch (...) {
        const auto failure = std::current_exception();
        try {
            shutdownLocked_();
        } catch (...) {
        }
        std::rethrow_exception(failure);
    }
    session_owner_ = owner;
}

void ObservationManager::releaseSession(const void* owner) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_owner_ != owner) return;
    session_owner_ = nullptr;
    shutdownLocked_();
}

void ObservationManager::registerSimulation(const void* simulation) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_owner_ && session_owner_ != simulation)
        throw std::logic_error("another simulation owns the process observation backend");
    ++initialized_simulation_count_;
}

void ObservationManager::unregisterSimulation() noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    --initialized_simulation_count_;
}

void ObservationManager::initializeLocked_(const ObservationYAMLConfig& config) {
    if (initialized_) {
        shutdownLocked_();
    }

    config_ = config;
    enabled_ = config.enabled;
    source_registry_.unfreeze();

    if (!enabled_) {
        initialized_ = true;
        return;
    }

    shared_queue_ = std::make_unique<ObservationQueue>(config.queue_capacity);

    ThreadContextManager::instance().setQueueCapacity(config.queue_capacity);
    ThreadContextManager::instance().setBackpressurePolicy(config.backpressure);
    ThreadContextManager::instance().setBackpressureMaxSpins(config.backpressure_max_spins);

    const auto& ul = config.unified_logging;
    auto applyChannelBP = [](ObservationChannel ch, const auto& channel_cfg) {
        if (channel_cfg.backpressure) {
            ThreadContextManager::instance().setBackpressurePolicy(ch, *channel_cfg.backpressure);
        }
        if (channel_cfg.backpressure_max_spins) {
            ThreadContextManager::instance().setBackpressureMaxSpins(
                ch, *channel_cfg.backpressure_max_spins);
        }
    };
    applyChannelBP(ObservationChannel::Trace, ul.trace_channel);
    applyChannelBP(ObservationChannel::Debug, ul.debug_channel);
    applyChannelBP(ObservationChannel::Info, ul.info_channel);
    applyChannelBP(ObservationChannel::Warn, ul.warn_channel);
    applyChannelBP(ObservationChannel::Error, ul.error_channel);

    ObservationBackend::Config backend_config;
    backend_config.output_dir = config.output_dir;
    backend_config.service_buffer_bytes = config.service_buffer_bytes;
    backend_config.enable_counter_csv = config.counters.csv_output;
    backend_config.counter_csv_format = config.counters.csv_format;

    backend_config.debug_file = ul.debug_channel.file;
    backend_config.info_file = ul.info_channel.file;
    backend_config.warn_file = ul.warn_channel.file;
    backend_config.error_file = ul.error_channel.file;

    backend_config.timeline_enabled = config.timeline.enabled;
    backend_config.timeline_file = config.timeline.file;
    backend_config.timeline_counters = config.timeline.counters;
    backend_config.timeline_compress = config.timeline.compress;

    backend_ = std::make_unique<ObservationBackend>(*shared_queue_, backend_config);

    initialized_ = true;
}

ObservationContext* ObservationManager::createContextForUnit(
    const std::string& unit_name, std::function<uint64_t()> cycle_provider, uint32_t thread_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_ || !enabled_) {
        return nullptr;
    }

    if (source_registry_.isFrozen()) {
        return nullptr;
    }

    uint16_t source_id = source_registry_.registerName(unit_name);

    auto ctx = std::make_unique<ObservationContext>(shared_queue_.get(), std::move(cycle_provider),
                                                    thread_id, unit_name, source_id);

    applyConfigToContext(*ctx, unit_name);
    ctx->registerAllCounters(&counter_registry_);

    ObservationContext* raw_ptr = ctx.get();
    contexts_.push_back(std::move(ctx));
    return raw_ptr;
}

void ObservationManager::applyConfigToContext(ObservationContext& ctx,
                                              const std::string& unit_name) {
    auto counters_config = config_.getCountersConfig(unit_name);
    auto unified_config = config_.getUnifiedLoggingConfig(unit_name);

    ctx.setCountersEnabled(counters_config.enabled);
    ctx.setTraceChannelEnabled(unified_config.enabled && unified_config.trace_channel.enabled);
    ctx.setTimelineEventsEnabled(config_.timeline.enabled);
    configureUnified(ctx, unified_config);
}

void ObservationManager::configureUnified(ObservationContext& ctx,
                                          const UnifiedLoggingConfig& unified_config) {
    if (!unified_config.enabled) {
        ctx.filter().disableCategory(category::ALL_LOGS);
        ctx.filter().disableCategory(category::TRACE);
        return;
    }

    struct ChannelCategoryMapping {
        bool enabled;
        CategoryMask mask;
    };
    const ChannelCategoryMapping channel_mappings[] = {
        {unified_config.debug_channel.enabled, category::LOG_DEBUG},
        {unified_config.info_channel.enabled, category::LOG_INFO},
        {unified_config.warn_channel.enabled, category::LOG_WARN},
        {unified_config.error_channel.enabled, category::LOG_ERROR},
        {unified_config.trace_channel.enabled, category::TRACE},
    };
    for (const auto& mapping : channel_mappings) {
        if (mapping.enabled) {
            ctx.filter().enableCategory(mapping.mask);
        } else {
            ctx.filter().disableCategory(mapping.mask);
        }
    }

    for (const auto& pattern : unified_config.categories) {
        if (!pattern.enabled) {
            continue;
        }

        CategoryMask mask = resolvePattern(pattern.pattern);
        if (mask == 0) {
            continue;
        }

        ctx.filter().enableCategory(mask);

        if (!pattern.temporal.empty()) {
            ObservationFilter::CategoryTemporalConfig temporal_config;

            for (const auto& filter : pattern.temporal) {
                if (filter.type == TemporalFilter::Type::RANGE) {
                    temporal_config.ranges.emplace_back(filter.range_start, filter.range_end);
                } else if (filter.type == TemporalFilter::Type::PERIODIC) {
                    temporal_config.periodic.push_back(
                        {filter.window, filter.period, filter.offset});
                }
            }

            ctx.filter().setCategoryTemporalConfig(mask, temporal_config);
        }
    }

    for (const auto& filter : unified_config.temporal) {
        if (filter.type == TemporalFilter::Type::RANGE) {
            ctx.filter().addCycleRange(filter.range_start, filter.range_end);
        } else if (filter.type == TemporalFilter::Type::PERIODIC) {
            ctx.filter().addPeriodicFilter(filter.window, filter.period, filter.offset);
        }
    }
}

CategoryMask ObservationManager::resolvePattern(const std::string& pattern) {
    return CategoryPatternMatcher::resolvePattern(pattern);
}

void ObservationManager::bindClockSource(ObservationContext& context, const ClockDomain& clock) {
    if (isBackendRunning()) throw std::logic_error("bind source clocks before backend start");
    if (context.isLookaheadMode())
        throw std::logic_error(
            "speculative observation epochs are unsupported with explicit hardware clocks");
    if (clock_sources_.size() <= context.sourceId()) clock_sources_.resize(context.sourceId() + 1);
    clock_sources_[context.sourceId()] = clock;
    // Unit::localCycle is owner-local; a thread override from another domain
    // must never substitute its edge index.
    context.useThreadCycleOverride(false);
}

void ObservationManager::configureClockObservation(const ClockDomain& reference, size_t lookahead) {
    clock_reference_ = reference;
    backend_->configureClocks(clock_sources_, reference, lookahead);
}

void ObservationManager::sampleClockOwner(size_t owner, SimTime exclusive_limit) {
    if (!periodicCounterSnapshotsEnabled()) return;
    const auto period = periodicDumpCycles();
    for (;;) {
        const auto next = nextPeriodicCounterCycle(owner, 0, period);
        if (next == UINT64_MAX) return;
        const auto time = clock_reference_->edge(next);
        if (time > exclusive_limit) return;
        auto* producer = periodicCounterProducer();
        if (!counter_registry_.pushOwnerSnapshots(next, std::span(&owner, 1), *producer, time))
            throw std::runtime_error("clock counter snapshot queue exhausted");
    }
}

uint64_t ObservationManager::clockSafeFrontier(uint64_t exclusive_ns) const {
    if (!periodicCounterSnapshotsEnabled()) return exclusive_ns;
    for (size_t owner = 0; owner < counter_registry_.ownerCount(); ++owner) {
        const auto next = counter_registry_.nextPublishedSnapshotCycle(owner, periodicDumpCycles());
        if (next != UINT64_MAX)
            exclusive_ns = std::min(exclusive_ns, clock_reference_->edge(next).floorNanoseconds());
    }
    return exclusive_ns;
}

void ObservationManager::startBackend() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_ || !enabled_ || !backend_) {
        return;
    }

    source_registry_.freeze();
    backend_->setSourceNameLookup(
        [this](uint16_t id) -> std::string_view { return source_registry_.getName(id); });

    const auto& derived_defs = counter_registry_.derivedDefs();
    if (!derived_defs.empty()) {
        backend_->setDerivedCounterDefs(derived_defs);
    }
    backend_->setCounterColumns(counter_registry_.counterColumns());
    backend_->setCounterSnapshotPlans(counter_registry_.snapshotPlans());

    if (!backend_->isRunning()) {
        backend_->start();
    }
}

void ObservationManager::stopBackend() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (backend_ && backend_->isRunning()) {
        backend_->stop();
    }

    source_registry_.unfreeze();
    if (backend_) backend_->rethrowIfFailed();
}

bool ObservationManager::isBackendRunning() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return backend_ && backend_->isRunning();
}

bool ObservationManager::timelineEnabled() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return backend_ && backend_->timelineEnabled();
}

bool ObservationManager::submitTimeline(TimelineStreamData&& data) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!backend_ || !backend_->isRunning() || !backend_->timelineEnabled()) {
        return false;
    }
    backend_->submitTimeline(std::move(data));
    return true;
}

void ObservationManager::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_owner_)
        throw std::logic_error("destroy the owning simulation before shutting down observation");
    shutdownLocked_();
}

void ObservationManager::shutdownLocked_() {
    std::exception_ptr error;
    if (backend_) {
        backend_->stop();
        try {
            backend_->rethrowIfFailed();
        } catch (...) {
            error = std::current_exception();
        }
    }

    contexts_.clear();
    counter_registry_.clear();
    clock_sources_.clear();
    clock_reference_.reset();
    clock_final_revision_.reset();
    clock_run_revision_ = 0;
    clock_final_sequence_ = 0;
    clock_final_after_edge_ = false;
    clock_run_boundary_ = {};
    source_registry_.clear();
    backend_.reset();
    shared_queue_.reset();

    enabled_ = false;
    initialized_ = false;
    if (error) std::rethrow_exception(error);
}

void ObservationManager::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (session_owner_)
        throw std::logic_error("destroy the owning simulation before resetting observation");
    config_ = ObservationYAMLConfig{};
    shutdownLocked_();
}

uint16_t ObservationManager::registerSourceName(const std::string& name) {
    return source_registry_.registerName(name);
}

std::string_view ObservationManager::getSourceName(uint16_t source_id) const noexcept {
    return source_registry_.getName(source_id);
}

size_t ObservationManager::contextCount() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return contexts_.size();
}

void ObservationManager::reregisterAllCounters() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_ || !enabled_) {
        return;
    }

    counter_registry_.reregisterAll(contexts_);
}

void ObservationManager::dumpFinalCounterSnapshot(uint64_t cycle) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_ || !enabled_ || !shared_queue_) {
        return;
    }

    if (!config_.counters.enabled) {
        return;
    }
    // Snapshots feed both sinks; dump if either consumes them.
    const bool timeline_counters = config_.timeline.enabled && config_.timeline.counters;
    if (!config_.counters.csv_output && !timeline_counters) {
        return;
    }

    if (clockMode()) {
        if (clock_final_revision_ && *clock_final_revision_ == clock_run_revision_) return;
        auto* producer = ThreadContextManager::instance().getContext();
        if (!producer) throw std::runtime_error("clock final snapshot producer unavailable");
        ++clock_final_sequence_;
        for (size_t owner = 0; owner < counter_registry_.ownerCount(); ++owner) {
            sampleClockOwner(owner, clock_run_boundary_);
            if (!counter_registry_.pushOwnerSnapshots(clock_final_sequence_, std::span(&owner, 1),
                                                      *producer, clock_run_boundary_, true,
                                                      clock_final_after_edge_))
                throw std::runtime_error("clock final snapshot queue exhausted");
        }
        clock_final_revision_ = clock_run_revision_;
        backend_->publishClockProgress(clock_run_boundary_.floorNanoseconds());
        return;
    }
    counter_registry_.dumpFinalSnapshot(cycle, shared_queue_.get(), contexts_);
}

ThreadContext* ObservationManager::periodicCounterProducer() {
    if (!periodicCounterSnapshotsEnabled()) return nullptr;
    ThreadContext* producer = ThreadContextManager::instance().getContext();
    if (!producer) {
        throw std::runtime_error(
            "periodic counters require a producer context, but the 64-context pool is exhausted");
    }
    return producer;
}

bool ObservationManager::pushPeriodicCounterSnapshots(uint64_t cycle,
                                                      std::span<const size_t> cluster_ids,
                                                      ThreadContext& producer) noexcept {
    if (!periodicCounterSnapshotsEnabled()) return false;
    return counter_registry_.pushOwnerSnapshots(cycle, cluster_ids, producer);
}

uint64_t ObservationManager::nextPeriodicCounterCycle(size_t cluster_id, uint64_t run_start,
                                                      uint64_t period) const noexcept {
    if (!periodicCounterSnapshotsEnabled()) return UINT64_MAX;
    return counter_registry_.nextOwnerSnapshotCycle(cluster_id, run_start, period);
}

void ObservationManager::printReport(std::ostream& out) const {
    std::lock_guard<std::mutex> lock(mutex_);

    out << "=== Observation Manager Report ===\n";
    out << "Enabled: " << (enabled_ ? "yes" : "no") << "\n";
    out << "Initialized: " << (initialized_ ? "yes" : "no") << "\n";

    if (!initialized_ || !enabled_) {
        return;
    }

    out << "Output directory: " << config_.output_dir << "\n";
    out << "Queue capacity: " << config_.queue_capacity << " bytes\n";
    out << "Contexts created: " << contexts_.size() << "\n";

    out << "\nCounters:\n";
    out << "  Enabled: " << (config_.counters.enabled ? "yes" : "no") << "\n";
    out << "  CSV output: " << (config_.counters.csv_output ? "yes" : "no") << "\n";

    const auto& ul = config_.unified_logging;
    out << "\nUnified Logging:\n";
    out << "  Enabled: " << (ul.enabled ? "yes" : "no") << "\n";

    out << "  Channels:\n";
    out << "    debug:  " << (ul.debug_channel.enabled ? "on" : "off") << "\n";
    out << "    info:   " << (ul.info_channel.enabled ? "on" : "off") << "\n";
    out << "    warn:   " << (ul.warn_channel.enabled ? "on" : "off") << "\n";
    out << "    error:  " << (ul.error_channel.enabled ? "on" : "off") << "\n";
    out << "    timeline events: " << (ul.trace_channel.enabled ? "on" : "off") << "\n";

    out << "\nTimeline (Perfetto):\n";
    out << "  Enabled: " << (config_.timeline.enabled ? "yes" : "no") << "\n";
    if (config_.timeline.enabled) {
        out << "  File: " << config_.timeline.file << "\n";
        out << "  Counter tracks: " << (config_.timeline.counters ? "yes" : "no") << "\n";
    }

    if (!ul.temporal.empty()) {
        out << "  Shared temporal filters: " << ul.temporal.size() << "\n";
    }

    out << "  Category patterns: " << ul.categories.size() << "\n";
    for (const auto& pattern : ul.categories) {
        out << "    - " << pattern.pattern;
        if (!pattern.temporal.empty()) {
            out << " (with " << pattern.temporal.size() << " temporal filter(s))";
        }
        out << "\n";
    }

    out << "\nPer-unit overrides: " << config_.unit_overrides.size() << "\n";
    for (const auto& [name, override] : config_.unit_overrides) {
        out << "  - " << name << ":";
        if (override.counters.has_value()) out << " counters";
        if (override.logging.has_value()) out << " logging";
        out << "\n";
    }

    if (backend_) {
        out << "\nBackend:\n";
        out << "  Running: " << (backend_->isRunning() ? "yes" : "no") << "\n";
        out << "  Events processed: " << backend_->eventsProcessed() << "\n";
        out << "  Bytes written: " << backend_->bytesWritten() << "\n";
        {
            const auto service = backend_->serviceStats();
            out << "  Scheduler service: " << service.calls << " polls, " << service.records
                << " records, " << service.elapsed_ns << " host ns, max " << service.max_poll_ns
                << " ns/poll (scheduler-managed I/O)\n";
        }
    }
}

}  // namespace chronon::observe
