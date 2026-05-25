// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "ObservationManager.hpp"

#include <iostream>
#include <thread>

#include "ObservationApi.hpp"
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
        // Already initialized - reset first
        shutdownLocked_();
    }

    config_ = config;
    enabled_ = config.enabled;
    source_registry_frozen_ = false;

    if (!enabled_) {
        initialized_ = true;
        return;
    }

    // Create shared observation queue (used for counter snapshots and lookahead commits)
    shared_queue_ = std::make_unique<ObservationQueue>(config.queue_capacity);

    // Propagate queue capacity to per-thread SPSC queues (the hot path for traces/logs)
    ThreadContextManager::instance().setQueueCapacity(config.queue_capacity);

    // Propagate backpressure settings (global defaults set all channels)
    ThreadContextManager::instance().setBackpressurePolicy(config.backpressure);
    ThreadContextManager::instance().setBackpressureMaxSpins(config.backpressure_max_spins);

    // Per-channel backpressure overrides
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

    // Create backend with configuration derived from UnifiedLoggingConfig

    ObservationBackend::Config backend_config;
    backend_config.output_dir = config.output_dir;
    backend_config.enable_counter_csv = config.counters.csv_output;
    backend_config.counter_csv_format = config.counters.csv_format;

    // Per-channel default formats
    backend_config.trace_format = ul.trace_channel.format;
    backend_config.debug_format = ul.debug_channel.format;
    backend_config.info_format = ul.info_channel.format;
    backend_config.warn_format = ul.warn_channel.format;
    backend_config.error_format = ul.error_channel.format;

    // Per-channel log file overrides
    backend_config.trace_file = ul.trace_channel.file;
    backend_config.debug_file = ul.debug_channel.file;
    backend_config.info_file = ul.info_channel.file;
    backend_config.warn_file = ul.warn_channel.file;
    backend_config.error_file = ul.error_channel.file;

    // Binary trace settings (compression, schema, index)
    backend_config.trace_config = ul.trace_channel;

    // Resolve category patterns and populate per-category per-channel overrides
    for (const auto& cat : ul.categories) {
        if (!cat.enabled) {
            continue;
        }

        CategoryMask mask = resolvePattern(cat.pattern);
        if (mask == 0) {
            continue;
        }

        // Build per-channel format override from category pattern
        bool has_override = cat.format.has_value() || cat.trace_format.has_value() ||
                            cat.debug_format.has_value() || cat.info_format.has_value() ||
                            cat.warn_format.has_value() || cat.error_format.has_value();

        if (has_override) {
            ObservationBackend::CategoryFormatOverride ovr;

            // Specific channel overrides take priority over shorthand
            ovr.trace_format = cat.trace_format.has_value() ? cat.trace_format : cat.format;
            ovr.debug_format = cat.debug_format.has_value() ? cat.debug_format : cat.format;
            ovr.info_format = cat.info_format.has_value() ? cat.info_format : cat.format;
            ovr.warn_format = cat.warn_format.has_value() ? cat.warn_format : cat.format;
            ovr.error_format = cat.error_format.has_value() ? cat.error_format : cat.format;

            backend_config.category_format_overrides[mask] = ovr;
        }
    }

    backend_ = std::make_unique<ObservationBackend>(*shared_queue_, backend_config);

    initialized_ = true;
}

ObservationContext* ObservationManager::createContextForUnit(
    const std::string& unit_name, std::function<uint64_t()> cycle_provider, uint32_t thread_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_ || !enabled_) {
        return nullptr;
    }

    // Source registry must remain immutable while backend is active because
    // lookups are lock-free on the backend thread.
    if (source_registry_frozen_) {
        return nullptr;
    }

    // Register unit name in source name registry and get source_id
    uint16_t source_id = registerSourceName(unit_name);

    // Create context with source_id for automatic unit name prefixes
    auto ctx = std::make_unique<ObservationContext>(shared_queue_.get(), std::move(cycle_provider),
                                                    thread_id, unit_name, source_id);

    // Apply configuration
    applyConfigToContext(*ctx, unit_name);

    // Register all counters with manager (Phase 3: Sparse Counter Architecture)
    ctx->registerAllCounters(this);

    // Store and return raw pointer
    ObservationContext* raw_ptr = ctx.get();
    contexts_.push_back(std::move(ctx));
    return raw_ptr;
}

void ObservationManager::applyConfigToContext(ObservationContext& ctx,
                                              const std::string& unit_name) {
    // Get effective configs (with per-unit overrides applied)
    auto counters_config = config_.getCountersConfig(unit_name);
    auto unified_config = config_.getUnifiedLoggingConfig(unit_name);

    // Configure counters
    ctx.setCountersEnabled(counters_config.enabled);

    // Configure unified logging and trace
    configureUnified(ctx, unified_config);
}

void ObservationManager::configureUnified(ObservationContext& ctx,
                                          const UnifiedLoggingConfig& unified_config) {
    if (!unified_config.enabled) {
        // Disable all log levels and trace
        ctx.filter().disableCategory(category::ALL_LOGS);
        ctx.filter().disableCategory(category::TRACE);
        return;
    }

    // Enable/disable per-level categories based on channel configs
    if (unified_config.debug_channel.enabled) {
        ctx.filter().enableCategory(category::LOG_DEBUG);
    } else {
        ctx.filter().disableCategory(category::LOG_DEBUG);
    }

    if (unified_config.info_channel.enabled) {
        ctx.filter().enableCategory(category::LOG_INFO);
    } else {
        ctx.filter().disableCategory(category::LOG_INFO);
    }

    if (unified_config.warn_channel.enabled) {
        ctx.filter().enableCategory(category::LOG_WARN);
    } else {
        ctx.filter().disableCategory(category::LOG_WARN);
    }

    if (unified_config.error_channel.enabled) {
        ctx.filter().enableCategory(category::LOG_ERROR);
    } else {
        ctx.filter().disableCategory(category::LOG_ERROR);
    }

    // Enable/disable trace category
    if (unified_config.trace_channel.enabled) {
        ctx.filter().enableCategory(category::TRACE);
    } else {
        ctx.filter().disableCategory(category::TRACE);
    }

    // Process shared category patterns (apply to all channels)
    for (const auto& pattern : unified_config.categories) {
        if (!pattern.enabled) {
            continue;
        }

        // Resolve pattern to mask
        CategoryMask mask = resolvePattern(pattern.pattern);
        if (mask == 0) {
            continue;
        }

        // Enable the category
        ctx.filter().enableCategory(mask);

        // Configure per-category temporal filters if present
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

    // Configure shared temporal filters (global, not per-category)
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

    // Set up source name lookup for unit prefixes in logs/traces
    source_registry_frozen_ = true;
    backend_->setSourceNameLookup(
        [this](uint16_t id) -> std::string_view { return getSourceName(id); });

    // Pass derived counter definitions to backend for CSV computation
    if (!derived_counter_defs_.empty()) {
        backend_->setDerivedCounterDefs(derived_counter_defs_);
    }

    if (!backend_->isRunning()) {
        backend_->start();
    }
}

void ObservationManager::stopBackend() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (backend_ && backend_->isRunning()) {
        backend_->stop();
    }

    source_registry_frozen_ = false;
}

bool ObservationManager::isBackendRunning() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return backend_ && backend_->isRunning();
}

void ObservationManager::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdownLocked_();
}

void ObservationManager::shutdownLocked_() {
    // Stop backend first
    if (backend_ && backend_->isRunning()) {
        backend_->stop();
    }

    // Clear all resources
    contexts_.clear();
    registered_counters_.clear();
    derived_counter_defs_.clear();
    source_names_.clear();
    backend_.reset();
    shared_queue_.reset();

    source_registry_frozen_ = false;
    enabled_ = false;
    initialized_ = false;
}

void ObservationManager::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdownLocked_();
    config_ = ObservationYAMLConfig{};
}

uint16_t ObservationManager::registerSourceName(const std::string& name) {
    // Note: Caller must hold mutex_ (called from createContextForUnit)
    uint16_t id = static_cast<uint16_t>(source_names_.size() + 1);
    source_names_.push_back(name);
    return id;
}

std::string_view ObservationManager::getSourceName(uint16_t source_id) const noexcept {
    // Lock-free lookup: source_names_ is append-only and IDs are 1-based
    if (source_id > 0 && source_id <= source_names_.size()) {
        return source_names_[source_id - 1];
    }
    return "";
}

size_t ObservationManager::contextCount() const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    return contexts_.size();
}

void ObservationManager::registerCounter(const std::string& unit_name, CounterId id,
                                         SimpleCounter* counter_ptr,
                                         const std::string& counter_name) {
    CounterKey key{unit_name, id, counter_name};
    registered_counters_[key] = counter_ptr;
}

void ObservationManager::reregisterAllCounters() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_ || !enabled_) {
        return;
    }

    // Clear existing counter registrations
    registered_counters_.clear();

    // Re-register all counters from all contexts
    for (const auto& ctx : contexts_) {
        if (ctx) {
            ctx->registerAllCounters(this);
        }
    }

    // Collect derived counter definitions from all contexts
    derived_counter_defs_.clear();
    for (const auto& ctx : contexts_) {
        if (ctx) {
            for (const auto& def : ctx->derivedCounterDefs()) {
                derived_counter_defs_.push_back(def);
            }
        }
    }
}

void ObservationManager::dumpCounterSnapshots(uint64_t cycle) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!initialized_ || !enabled_ || !shared_queue_) {
        return;
    }

    if (!config_.counters.enabled || !config_.counters.csv_output) {
        return;  // Counter CSV output disabled
    }

    // Collect all counter entries and compute total batch size.
    // This ensures atomic all-or-nothing writes — no partial counter dumps
    // when the shared queue is under pressure.
    struct SnapshotEntry {
        const std::string* unit_name;
        std::string counter_name;
        uint64_t value;
        size_t aligned_size;
    };

    auto alignedRecordSize = [](size_t unit_len, size_t ctr_len) -> size_t {
        size_t data_size = 8 + 2 + unit_len + 2 + ctr_len + 8;
        size_t record_size = sizeof(ObservationQueue::RecordHeader) + data_size;
        return (record_size + 7) & ~7;
    };

    std::vector<SnapshotEntry> entries;
    entries.reserve(registered_counters_.size());
    size_t total_size = 0;

    // Registered counters (registration-based pull model)
    for (const auto& [key, counter_ptr] : registered_counters_) {
        if (key.counter_name.empty()) {
            continue;
        }
        size_t aligned = alignedRecordSize(key.unit_name.length(), key.counter_name.length());
        entries.push_back({&key.unit_name, key.counter_name, counter_ptr->get(), aligned});
        total_size += aligned;
    }

    // Per-unit observe stats as counter snapshots
    constexpr size_t num_ch = static_cast<size_t>(ObservationChannel::NumChannels);
    for (const auto& ctx : contexts_) {
        if (!ctx) {
            continue;
        }
        const auto& stats = ctx->observationStats();
        if (stats.totalEmitted() == 0 && stats.totalDropped() == 0) {
            continue;
        }
        const std::string& unit_name = ctx->unitName();
        for (size_t i = 0; i < num_ch; ++i) {
            auto ch = static_cast<ObservationChannel>(i);
            const auto& ch_stats = stats.get(ch);
            const char* ch_name = ObservationStats::channelName(ch);

            if (ch_stats.emitted > 0) {
                std::string ctr_name = std::string("obs_") + ch_name + "_emitted";
                size_t aligned = alignedRecordSize(unit_name.length(), ctr_name.length());
                entries.push_back({&unit_name, std::move(ctr_name), ch_stats.emitted, aligned});
                total_size += aligned;
            }
            if (ch_stats.dropped > 0) {
                std::string ctr_name = std::string("obs_") + ch_name + "_dropped";
                size_t aligned = alignedRecordSize(unit_name.length(), ctr_name.length());
                entries.push_back({&unit_name, std::move(ctr_name), ch_stats.dropped, aligned});
                total_size += aligned;
            }
        }
    }

    if (entries.empty()) {
        return;
    }

    // Atomic batch write: all counters or none.
    // Use bounded spin-wait (matching ObservationContext backpressure pattern)
    // to give the backend time to drain if the queue is temporarily full.
    auto* ptr = shared_queue_->prepareWrite(total_size);
    if (ptr == nullptr) {
        if (total_size > shared_queue_->capacity()) {
            // Batch exceeds queue capacity — can never fit
            shared_queue_->incrementDropped();
            return;
        }
        constexpr uint32_t max_spins = 4096;
        uint32_t spins = 0;
        do {
            if (++spins > 64) {
                std::this_thread::yield();
            } else {
#if defined(__x86_64__) || defined(_M_X64)
                __builtin_ia32_pause();
#elif defined(__aarch64__)
                asm volatile("yield" ::: "memory");
#else
                std::this_thread::yield();
#endif
            }
            if ((spins & 0xFFu) == 0u) {
                (void)ThreadContextManager::instance().wakeBackend();
            }
            ptr = shared_queue_->prepareWrite(total_size);
        } while (ptr == nullptr && spins < max_spins);

        if (ptr == nullptr) {
            shared_queue_->incrementDropped();
            return;
        }
    }

    std::byte* write_pos = ptr;
    for (const auto& entry : entries) {
        auto* header = reinterpret_cast<ObservationQueue::RecordHeader*>(write_pos);
        header->total_size = static_cast<uint16_t>(entry.aligned_size);
        header->type = ObservationQueue::EventType::COUNTER_SNAPSHOT;
        header->flags = 0;
        header->padding = 0;

        std::byte* data = write_pos + sizeof(ObservationQueue::RecordHeader);
        size_t offset = 0;

        std::memcpy(data + offset, &cycle, 8);
        offset += 8;

        uint16_t unit_len = static_cast<uint16_t>(entry.unit_name->length());
        std::memcpy(data + offset, &unit_len, 2);
        offset += 2;
        std::memcpy(data + offset, entry.unit_name->data(), unit_len);
        offset += unit_len;

        uint16_t ctr_len = static_cast<uint16_t>(entry.counter_name.length());
        std::memcpy(data + offset, &ctr_len, 2);
        offset += 2;
        std::memcpy(data + offset, entry.counter_name.data(), ctr_len);
        offset += ctr_len;

        std::memcpy(data + offset, &entry.value, 8);

        write_pos += entry.aligned_size;
    }

    shared_queue_->finishAndCommitWrite(total_size);

    // Reset counters after snapshot — values are safely in the queue.
    for (const auto& [key, counter_ptr] : registered_counters_) {
        counter_ptr->reset();
    }
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

    auto formatName = [](OutputFormat f) -> const char* {
        switch (f) {
            case OutputFormat::Text:
                return "text";
            case OutputFormat::Binary:
                return "binary";
            case OutputFormat::Both:
                return "both";
        }
        return "unknown";
    };

    out << "  Channels:\n";
    out << "    debug:  " << (ul.debug_channel.enabled ? "on" : "off") << " ("
        << formatName(ul.debug_channel.format) << ")\n";
    out << "    info:   " << (ul.info_channel.enabled ? "on" : "off") << " ("
        << formatName(ul.info_channel.format) << ")\n";
    out << "    warn:   " << (ul.warn_channel.enabled ? "on" : "off") << " ("
        << formatName(ul.warn_channel.format) << ")\n";
    out << "    error:  " << (ul.error_channel.enabled ? "on" : "off") << " ("
        << formatName(ul.error_channel.format) << ")\n";
    out << "    trace:  " << (ul.trace_channel.enabled ? "on" : "off") << " ("
        << formatName(ul.trace_channel.format) << ")\n";

    if (!ul.temporal.empty()) {
        out << "  Shared temporal filters: " << ul.temporal.size() << "\n";
    }

    out << "  Category patterns: " << ul.categories.size() << "\n";
    for (const auto& pattern : ul.categories) {
        out << "    - " << pattern.pattern;
        if (pattern.format.has_value()) {
            out << " (format: " << formatName(*pattern.format) << ")";
        }
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
