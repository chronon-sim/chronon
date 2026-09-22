// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <iostream>
#include <iterator>
#include <set>
#include <tuple>

#include "../../observe/ObservationManager.hpp"
#include "TickSimulation.hpp"

namespace chronon::sender {

const ClockDomain& TickSimulation::addClockDomain(ClockDomain domain) {
    if (initialization_started_ || finalized_ || current_cycle_)
        throw std::logic_error(
            "Clock domains must be configured before initialization and execution");
    if (domain.id() == 0 || domain.id() == UINT32_MAX || domain.name() == "default") {
        throw std::invalid_argument("clock ID/name reserved by Chronon");
    }
    for (const auto& existing : clock_domains_) {
        if (existing.id() == domain.id() || existing.name() == domain.name()) {
            throw std::invalid_argument("duplicate clock ID or name");
        }
    }
    clock_domains_.push_back(std::move(domain));
    clock_mode_ = true;
    return clock_domains_.back();
}

const ClockDomain& TickSimulation::clockDomain(ClockDomainId id) const {
    if (id == 0) return default_clock_;
    for (const auto& domain : clock_domains_)
        if (domain.id() == id) return domain;
    throw std::invalid_argument("unknown clock domain ID");
}

void TickSimulation::validateClockOwner_(const Unit* unit) const {
    for (const auto& owned : units_)
        if (owned.get() == unit) return;
    throw std::invalid_argument("clock endpoint must belong to this simulation");
}

void TickSimulation::assignClockDomain(Unit& unit, ClockDomainId id) {
    if (initialization_started_ || finalized_ || current_cycle_)
        throw std::logic_error("runtime domain rebinding is unsupported");
    validateClockOwner_(&unit);
    unit.clock_ = &clockDomain(id);
    clock_mode_ = true;
}

void TickSimulation::prepareClockTopology_() {
    if (observe::ObservationManager::instance().isBackendRunning())
        throw std::logic_error("initialize clock simulation before starting observation backend");
    std::map<std::string, TickableUnit*> names;
    for (auto* unit : unit_ptrs_) {
        if (!names.emplace(unit->fullPath(), unit).second) {
            throw std::invalid_argument("multiclock units require unique fullPath identities");
        }
        for (auto* port : unit->ports()) port->appendOutgoingConnections(connections_);
    }
    std::sort(connections_.begin(), connections_.end());
    connections_.erase(std::unique(connections_.begin(), connections_.end()), connections_.end());
    for (auto* connection : connections_) {
        validateClockOwner_(connection->source());
        validateClockOwner_(connection->destination());
        if (connection->source()->clockDomainId() != connection->destination()->clockDomainId()) {
            throw std::invalid_argument("ordinary connection crosses hardware clock domains: " +
                                        connection->source()->fullPath() + " -> " +
                                        connection->destination()->fullPath() +
                                        "; declare an AsyncFifo circuit");
        }
    }
    const auto key = [](ConnectionBase* connection) {
        return std::tuple{connection->source()->fullPath(), connection->sourcePortName(),
                          connection->destination()->fullPath(), connection->destinationPortName(),
                          connection->delay()};
    };
    std::sort(connections_.begin(), connections_.end(),
              [&](auto* a, auto* b) { return key(a) < key(b); });
    for (size_t i = 0; i < connections_.size(); ++i)
        connections_[i]->setConnId(static_cast<uint32_t>(i));

    // Canonical Kahn order for same-domain zero-delay combinational paths.
    // Registered edges and CDC do not constrain evaluation order at one instant.
    std::vector<TickableUnit*> order;
    order.reserve(unit_ptrs_.size());
    if (std::none_of(connections_.begin(), connections_.end(),
                     [](const auto* connection) { return connection->delay() == 0; })) {
        // With no same-instant dependency, Kahn's ready queue is simply the
        // already-validated name order. Avoid rebuilding per-unit maps.
        for (const auto& [name, unit] : names) {
            (void)name;
            order.push_back(unit);
        }
    } else {
        std::map<Unit*, size_t> incoming;
        std::map<Unit*, std::vector<Unit*>> outgoing;
        std::map<std::string, TickableUnit*> ready;
        for (auto* unit : unit_ptrs_) incoming[unit] = 0;
        for (auto* connection : connections_) {
            if (!connection->delay()) {
                ++incoming[connection->destination()];
                outgoing[connection->source()].push_back(connection->destination());
            }
        }
        for (auto* unit : unit_ptrs_)
            if (!incoming[unit]) ready.emplace(unit->fullPath(), unit);
        while (!ready.empty()) {
            auto* unit = ready.begin()->second;
            ready.erase(ready.begin());
            order.push_back(unit);
            for (auto* successor : outgoing[unit]) {
                if (--incoming[successor] == 0)
                    ready.emplace(successor->fullPath(), static_cast<TickableUnit*>(successor));
            }
        }
    }
    if (order.size() != unit_ptrs_.size())
        throw std::invalid_argument("zero-delay combinational cycle");
    unit_ptrs_ = std::move(order);
    for (auto* unit : unit_ptrs_) unit->clock_topology_frozen_ = true;
    std::sort(cdc_.begin(), cdc_.end(),
              [](const auto& a, const auto& b) { return a->id() < b->id(); });
}

void TickSimulation::initializeClockRuntime_() {
    auto& observation = observe::ObservationManager::instance();
    if (observation.isEnabled()) {
        const auto scheduler_file = config_.timeline_trace.file.empty()
                                        ? std::filesystem::path("scheduler_timeline.pftrace")
                                        : std::filesystem::path(config_.timeline_trace.file);
        const auto model_file = observation.config().timeline.file.empty()
                                    ? std::filesystem::path("timeline.pftrace")
                                    : std::filesystem::path(observation.config().timeline.file);
        if (config_.timeline_trace.enabled &&
            scheduler_file.lexically_normal() == model_file.lexically_normal())
            throw std::invalid_argument(
                "scheduler and physical observation timelines require distinct files");
        const ClockDomain* reference = &default_clock_;
        const auto& name = observation.config().counters.reference_clock;
        if (name != "default") {
            reference = nullptr;
            for (const auto& clock : clock_domains_)
                if (clock.name() == name) reference = &clock;
            if (!reference) throw std::invalid_argument("unknown counter reference_clock: " + name);
        }
        observation.configureClockObservation(*reference, config_.max_lookahead_cycles);
        clock_observation_ = observation.backend();
    }
    if (config_.profile_clock_scheduler)
        clock_scheduler_profile_.resize(shouldUseParallelExecution_() ? thread_units_.size() : 1);
    for (auto* unit : unit_ptrs_) {
        auto& runtime = clock_runtime_[unit->clockDomainId()];
        runtime.clock = &unit->clockDomain();
        runtime.units.push_back(unit);
        if (clock_trace_) {
            unit->clock_trace_stream_ =
                clock_trace_->addStream(unit->clockDomain(), unit->id(), unit->fullPath());
        }
    }
    clock_active_cdc_.reserve(cdc_.size());
    clock_cdc_seen_.resize(cdc_.size());
    for (size_t f = 0; f < cdc_.size(); ++f) {
        const auto& fifo = *cdc_[f];
        if (!fifo.endpointEdgesOnly()) {
            clock_always_cdc_.push_back(f);
            continue;
        }
        const auto write = fifo.writeOwner()->clockDomainId();
        const auto read = fifo.readOwner()->clockDomainId();
        clock_runtime_.at(write).cdc.push_back(f);
        if (read != write) clock_runtime_.at(read).cdc.push_back(f);
    }
    std::vector<const ClockDomain*> clocks;
    for (const auto& [id, runtime] : clock_runtime_) {
        (void)id;
        clocks.push_back(runtime.clock);
    }
    clock_calendar_ = std::make_unique<ClockCalendar>(clocks);
    if (shouldUseParallelExecution_()) initializeClockParallel_();
    if (clock_trace_) {
        // Only the final pre-initialization configuration needs a service slot.
        clock_trace_->attachScheduler(hostServices());
        if (shouldUseParallelExecution_())
            clock_trace_->startParallel(config_.max_lookahead_cycles);
        else
            clock_trace_->start(true);
    }
    if (config_.enable_parallel && !shouldUseParallelExecution_())
        std::clog << "[chronon] " << parallel_fallback_reason_ << '\n';
}

void TickSimulation::requireClockRun_() {
    if (!clock_mode_)
        throw std::logic_error("configure clock domains before using physical-time run APIs");
    if (clock_failed_)
        throw std::logic_error("multiclock simulation cannot resume after an evaluation failure");
    if (!initialized_) initialize();
    // All previous work is settled at public run entry. Publish the boundary
    // for an existing external stop even if the selected run attempts no batch.
    termination_ctrl_.setSettledTime(clock_time_);
    if (clock_observation_ && !clock_observation_->isRunning())
        throw std::logic_error("start observation backend before running a clock simulation");
}

void TickSimulation::flushClockObservationProducer_() {
    if (!clock_observation_) return;
    if (auto* producer = observe::ThreadContextManager::instance().getContext())
        producer->queue().forceCommitWrite();
}

void TickSimulation::publishClockObservation_() {
    if (!clock_observation_ || clock_calendar_->empty()) return;
    const auto ns = clock_time_.floorNanoseconds();
    clock_observation_retired_ns_.store(ns, std::memory_order_release);
    clock_observation_->publishClockProgress(
        observe::ObservationManager::instance().clockSafeFrontier(ns));
}

void TickSimulation::finishClockObservationRun_(std::optional<SimTime> limit) {
    if (!clock_observation_ || clock_calendar_->empty()) return;
    auto& observation = observe::ObservationManager::instance();
    const auto boundary = limit && !wasTerminationRequested() ? *limit : clock_time_;
    // A legal no-op may name an earlier exclusive limit than a prior run's
    // cutoff. Keep the established time and before/after phase until new work.
    if (current_cycle_ == observation.clockRunRevision() &&
        boundary <= observation.clockRunBoundary()) {
        clock_observation_->rethrowIfFailed();
        return;
    }
    for (const auto owner : counter_owner_ids_) observation.sampleClockOwner(owner, boundary);
    flushClockObservationProducer_();
    observation.setClockRunBoundary(boundary, current_cycle_,
                                    current_cycle_ != 0 && (!limit || wasTerminationRequested()));
    publishClockObservation_();
    clock_observation_->rethrowIfFailed();
}

bool TickSimulation::executeClockBatch_() {
    if (host_services_) host_services_->poll(sequential_service_cursor_);
    if (clock_calendar_->empty() || wasTerminationRequested()) return false;
    if (current_cycle_ == UINT64_MAX) throw std::overflow_error("scheduler progress overflow");
    try {
        auto* profile = config_.profile_clock_scheduler && (current_cycle_ & 63) == 0
                            ? &clock_scheduler_profile_[0]
                            : nullptr;
        if (profile) ++profile->sweeps;
        detail::ClockProfileScope calendar_profile(profile ? &profile->admission_ns : nullptr);
        if (clock_observation_) {
            while (!clock_observation_->tryAdmitClockTime(clock_calendar_->nextTime())) {
                if (wasTerminationRequested()) return false;
                if (host_services_) host_services_->poll(sequential_service_cursor_);
                std::this_thread::yield();
            }
            if (wasTerminationRequested()) return false;
            auto& observation = observe::ObservationManager::instance();
            for (const auto owner : counter_owner_ids_)
                observation.sampleClockOwner(owner, clock_calendar_->nextTime());
        }
        const auto edges = clock_calendar_->pop();
        calendar_profile.finish();
        detail::ClockProfileScope actors_profile(profile ? &profile->actor_ns : nullptr);
        if (clock_trace_ && clock_trace_->needsProgress())
            clock_trace_->beginClockBatch(edges.front().time);
        // Most phased calendars select one domain. Borrow its sorted list;
        // coincident edges union the lists in preallocated scratch, then restore
        // stable FIFO-ID order. Never skip an empty lane's synchronizer edges.
        // Resolve that domain once for the whole batch. Topology is immutable,
        // so CDC selection, unit execution and retirement share the same object.
        auto* const single_runtime =
            edges.size() == 1 ? &clock_runtime_.at(edges.front().domain->id()) : nullptr;
        const auto runtime_for = [&](const ClockEdge& edge) -> ClockRuntime& {
            return single_runtime ? *single_runtime : clock_runtime_.at(edge.domain->id());
        };
        std::span<const size_t> active;
        if (edges.size() == 1 && clock_always_cdc_.empty()) {
            active = single_runtime->cdc;
        } else if (edges.size() == 2 && clock_always_cdc_.empty()) {
            // Initialization appends FIFO indices in sorted order. Merge two
            // coincident domains directly, emitting shared lanes only once.
            const auto& a = clock_runtime_.at(edges[0].domain->id()).cdc;
            const auto& b = clock_runtime_.at(edges[1].domain->id()).cdc;
            clock_active_cdc_.clear();
            std::set_union(a.begin(), a.end(), b.begin(), b.end(),
                           std::back_inserter(clock_active_cdc_));
            active = clock_active_cdc_;
        } else {
            clock_active_cdc_.clear();
            const auto append = [&](size_t f) {
                if (!clock_cdc_seen_[f]) {
                    clock_cdc_seen_[f] = 1;
                    clock_active_cdc_.push_back(f);
                }
            };
            for (const auto f : clock_always_cdc_) append(f);
            for (const auto& edge : edges)
                for (const auto f : runtime_for(edge).cdc) append(f);
            std::sort(clock_active_cdc_.begin(), clock_active_cdc_.end());
            for (const auto f : clock_active_cdc_) clock_cdc_seen_[f] = 0;
            active = clock_active_cdc_;
        }
        {
            detail::ClockProfileScope bridge_profile(profile ? &profile->bridge_ns : nullptr);
            for (const auto f : active) cdc_[f]->begin(edges);
        }
        detail::ClockProfileScope ticks_profile(profile ? &profile->tick_ns : nullptr);
        for (const auto& edge : edges) {
            auto& runtime = runtime_for(edge);
            for (auto* unit : runtime.units) {
                unit->clock_edge_executing_ = true;
                executeUnitCycle_(unit, edge.cycle);
                unit->clock_edge_executing_ = false;
            }
        }
        ticks_profile.finish();
        // Every CDC component sampled before ANY participating domain committed.
        {
            detail::ClockProfileScope bridge_profile(profile ? &profile->bridge_ns : nullptr);
            for (const auto f : active) cdc_[f]->commit();
            if (profile) profile->bridge_commits += active.size();
        }
        actors_profile.finish();
        for (const auto& edge : edges) runtime_for(edge).next_cycle = edge.cycle + 1;
        if (clock_observation_) flushClockObservationProducer_();
        clock_time_ = edges.front().time;
        ++current_cycle_;
        termination_ctrl_.setSettledTime(clock_time_);
        // All queues publish before this safe point, including every CDC commit.
        // Only observation time is quantized; the current ns bucket stays open.
        if (clock_trace_ && clock_trace_->needsProgress()) clock_trace_->endClockBatch();
        if (clock_observation_) publishClockObservation_();
        return true;
    } catch (...) {
        for (auto* unit : unit_ptrs_) unit->clock_edge_executing_ = false;
        clock_failed_ = true;
        throw;
    }
}

uint64_t TickSimulation::runClockEvents(uint64_t max_event_batches) {
    requireClockRun_();
    if (shouldUseParallelExecution_()) return runClockEpochFree_(max_event_batches);
    // A predicate interval of one needs a single batch, with no counted loop.
    if (max_event_batches == 1) {
        const auto count = executeClockBatch_();
        finishClockObservationRun_();
        return count;
    }
    uint64_t count = 0;
    while (count < max_event_batches && executeClockBatch_()) ++count;
    finishClockObservationRun_();
    return count;
}

uint64_t TickSimulation::runUntilTime(SimTime exclusive_limit) {
    requireClockRun_();
    if (exclusive_limit < clock_time_)
        throw std::invalid_argument("cannot run backwards in physical time");
    if (shouldUseParallelExecution_()) {
        const auto count = runClockEpochFree_(UINT64_MAX, exclusive_limit);
        finishClockObservationRun_(exclusive_limit);
        return count;
    }
    uint64_t count = 0;
    while (!clock_calendar_->empty() && clock_calendar_->nextTime() < exclusive_limit &&
           executeClockBatch_())
        ++count;
    finishClockObservationRun_(exclusive_limit);
    return count;
}

uint64_t TickSimulation::runDomainCycles(ClockDomainId id, uint64_t additional_edges) {
    requireClockRun_();
    const auto& runtime = clock_runtime_.at(id);
    if (!additional_edges) return 0;
    const auto last =
        clock_detail::narrow(clock_detail::Wide(runtime.next_cycle) + additional_edges - 1);
    const auto end = runtime.clock->edge(last);
    if (shouldUseParallelExecution_()) return runClockEpochFree_(UINT64_MAX, end, true);
    uint64_t count = 0;
    while (!clock_calendar_->empty() && clock_calendar_->nextTime() <= end && executeClockBatch_())
        ++count;
    finishClockObservationRun_();
    return count;
}

uint64_t TickSimulation::domainCycleCount(ClockDomainId id) const {
    (void)clockDomain(id);
    const auto found = clock_runtime_.find(id);
    return found == clock_runtime_.end() ? 0 : found->second.next_cycle;
}

bool TickSimulation::cdcDrained() const noexcept {
    return std::all_of(cdc_.begin(), cdc_.end(), [](const auto& fifo) { return fifo->drained(); });
}

uint64_t TickSimulation::drainCdc(uint64_t max_event_batches) {
    requireClockRun_();
    uint64_t count = 0;
    while (count < max_event_batches && !cdcDrained()) {
        if (!runClockEvents(1)) break;
        ++count;
    }
    finishClockObservationRun_();
    return count;
}

void TickSimulation::configureClockTrace(observe::ClockTraceRecorder::Config config) {
    if (initialized_) throw std::logic_error("configure clock tracing before initialize");
    clock_trace_ = std::make_unique<observe::ClockTraceRecorder>(std::move(config));
    clock_mode_ = true;
}

void TickSimulation::closeClockTrace() {
    if (!clock_trace_) return;
    for (auto* unit : unit_ptrs_) unit->clock_trace_stream_ = nullptr;
    for (auto& fifo : cdc_) fifo->setClockTraceStreams(nullptr, nullptr);
    clock_trace_->close();
}

}  // namespace chronon::sender
