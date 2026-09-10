// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <iostream>
#include <set>
#include <tuple>

#include "TickSimulation.hpp"

namespace chronon::sender {

const ClockDomain& TickSimulation::addClockDomain(ClockDomain domain) {
    if (initialized_ || current_cycle_)
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
    if (initialized_ || current_cycle_)
        throw std::logic_error("runtime domain rebinding is unsupported");
    validateClockOwner_(&unit);
    unit.clock_ = &clockDomain(id);
    clock_mode_ = true;
}

void TickSimulation::prepareClockTopology_() {
    // The legacy backend reorders raw cycles and has no hardware domain metadata.
    // Reject this combination instead of emitting plausible but incorrect timestamps.
    if (observe::ObservationManager::instance().isEnabled()) {
        throw std::logic_error(
            "multiclock uses configureClockTrace; disable the legacy ObservationManager backend");
    }
    if (config_.timeline_trace.enabled) {
        throw std::logic_error(
            "scheduler wall-time tracing for multiclock is unsupported; use configureClockTrace");
    }
    std::set<std::string> names;
    for (auto* unit : unit_ptrs_) {
        if (!names.insert(unit->fullPath()).second) {
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
    std::vector<TickableUnit*> order;
    while (!ready.empty()) {
        auto* unit = ready.begin()->second;
        ready.erase(ready.begin());
        order.push_back(unit);
        for (auto* successor : outgoing[unit]) {
            if (--incoming[successor] == 0)
                ready.emplace(successor->fullPath(), static_cast<TickableUnit*>(successor));
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
    for (auto* unit : unit_ptrs_) {
        auto& runtime = clock_runtime_[unit->clockDomainId()];
        runtime.clock = &unit->clockDomain();
        runtime.units.push_back(unit);
        if (clock_trace_) {
            unit->clock_trace_stream_ =
                clock_trace_->addStream(unit->clockDomain(), unit->id(), unit->fullPath());
        }
    }
    std::vector<const ClockDomain*> clocks;
    for (const auto& [id, runtime] : clock_runtime_) {
        (void)id;
        clocks.push_back(runtime.clock);
    }
    clock_calendar_ = std::make_unique<ClockCalendar>(clocks);
    if (clock_trace_) clock_trace_->start();
    if (config_.enable_parallel) std::clog << "[chronon] " << parallel_fallback_reason_ << '\n';
}

void TickSimulation::requireClockRun_() {
    if (!clock_mode_)
        throw std::logic_error("configure clock domains before using physical-time run APIs");
    if (clock_failed_)
        throw std::logic_error("multiclock simulation cannot resume after an evaluation failure");
    if (!initialized_) initialize();
}

bool TickSimulation::executeClockBatch_() {
    if (clock_calendar_->empty() || wasTerminationRequested()) return false;
    if (current_cycle_ == UINT64_MAX) throw std::overflow_error("scheduler progress overflow");
    try {
        const auto edges = clock_calendar_->pop();
        for (auto& fifo : cdc_) fifo->begin(edges);
        for (const auto& edge : edges) {
            auto& runtime = clock_runtime_.at(edge.domain->id());
            for (auto* unit : runtime.units) {
                unit->clock_edge_executing_ = true;
                executeUnitCycle_(unit, edge.cycle);
                unit->clock_edge_executing_ = false;
            }
        }
        // Every CDC component sampled before ANY participating domain committed.
        for (auto& fifo : cdc_) fifo->commit();
        for (const auto& edge : edges)
            clock_runtime_.at(edge.domain->id()).next_cycle = edge.cycle + 1;
        clock_time_ = edges.front().time;
        ++current_cycle_;
        // All queues publish before this safe point, including every CDC commit.
        // Only observation time is quantized; the current ns bucket stays open.
        if ((current_cycle_ & 63) == 0 && clock_trace_ && clock_trace_->needsProgress())
            clock_trace_->advance(clock_time_.floorNanoseconds());
        return true;
    } catch (...) {
        for (auto* unit : unit_ptrs_) unit->clock_edge_executing_ = false;
        clock_failed_ = true;
        throw;
    }
}

uint64_t TickSimulation::runClockEvents(uint64_t max_event_batches) {
    requireClockRun_();
    uint64_t count = 0;
    while (count < max_event_batches && executeClockBatch_()) ++count;
    return count;
}

uint64_t TickSimulation::runUntilTime(SimTime exclusive_limit) {
    requireClockRun_();
    if (exclusive_limit < clock_time_)
        throw std::invalid_argument("cannot run backwards in physical time");
    uint64_t count = 0;
    while (!clock_calendar_->empty() && clock_calendar_->nextTime() < exclusive_limit &&
           executeClockBatch_())
        ++count;
    return count;
}

uint64_t TickSimulation::runDomainCycles(ClockDomainId id, uint64_t additional_edges) {
    requireClockRun_();
    const auto& runtime = clock_runtime_.at(id);
    if (!additional_edges) return 0;
    const auto last =
        clock_detail::narrow(clock_detail::Wide(runtime.next_cycle) + additional_edges - 1);
    const auto end = runtime.clock->edge(last);
    uint64_t count = 0;
    while (!clock_calendar_->empty() && clock_calendar_->nextTime() <= end && executeClockBatch_())
        ++count;
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
    while (count < max_event_batches && !cdcDrained() && executeClockBatch_()) ++count;
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
    clock_trace_->close();
}

}  // namespace chronon::sender
