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

ObservationManager::~ObservationManager() { shutdown(); }

void ObservationManager::initialize(const ObservationYAMLConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);

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
    backend_config.enable_counter_csv = config.counters.csv_output;
    backend_config.counter_csv_format = config.counters.csv_format;

    backend_config.trace_text = ul.trace_channel.text;

    backend_config.trace_file = ul.trace_channel.file;
    backend_config.debug_file = ul.debug_channel.file;
    backend_config.info_file = ul.info_channel.file;
    backend_config.warn_file = ul.warn_channel.file;
    backend_config.error_file = ul.error_channel.file;

    backend_config.timeline_enabled = config.timeline.enabled;
    backend_config.timeline_file = config.timeline.file;
    backend_config.timeline_trace_events = config.timeline.trace_events;
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
    shutdownLocked_();
}

void ObservationManager::shutdownLocked_() {
    if (backend_ && backend_->isRunning()) {
        backend_->stop();
    }

    contexts_.clear();
    counter_registry_.clear();
    source_registry_.clear();
    backend_.reset();
    shared_queue_.reset();

    enabled_ = false;
    initialized_ = false;
}

void ObservationManager::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdownLocked_();
    config_ = ObservationYAMLConfig{};
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
    out << "    trace:  " << (ul.trace_channel.enabled ? "on" : "off")
        << (ul.trace_channel.enabled && ul.trace_channel.text ? " (+text)" : "") << "\n";

    out << "\nTimeline (Perfetto):\n";
    out << "  Enabled: " << (config_.timeline.enabled ? "yes" : "no") << "\n";
    if (config_.timeline.enabled) {
        out << "  File: " << config_.timeline.file << "\n";
        out << "  Trace events: " << (config_.timeline.trace_events ? "yes" : "no") << "\n";
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
    }
}

}  // namespace chronon::observe
