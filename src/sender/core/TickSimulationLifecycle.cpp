// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0

// Host-side construction, lifecycle and inspection. No per-tick dispatch here.
#include "../../observe/ObservationManager.hpp"
#include "TickSimulation.hpp"

namespace chronon::sender {

TickSimulation::TickSimulation(const TickSimulationConfig& config)
    : config_(config),
      default_clock_(ClockDomain::fromHz(0, "default", config.tick_frequency_hz)),
      current_cycle_(0),
      initialized_(false),
      pool_(static_cast<uint32_t>(normalizeThreadCount(config.num_threads))) {
    // Register observation teardown before this object's destructor, including
    // static simulations. Finalizers and pool teardown must see live services.
    (void)observe::ObservationManager::instance();
    config_.num_threads = normalizeThreadCount(config_.num_threads);
    timeline_trace_.configure(config_.timeline_trace);
    resolveSolver_();
}

TickSimulation::~TickSimulation() {
    try {
        finalize();
    } catch (...) {
        // Explicit finalize() reports failures. Destruction still releases all
        // resources and never retries a hook that already ran.
    }
    auto& observation = observe::ObservationManager::instance();
    if (observation.ownsSession(this)) {
        try {
            observation.stopBackend();
        } catch (...) {
        }
    }
    // Port handles remain valid throughout model destruction. The directory is
    // deliberately outside the scheduler's hot member layout.
    units_.clear();
    try {
        observation.releaseSession(this);
    } catch (...) {
    }
    if (observation_registered_) observation.unregisterSimulation();
    freeThreadProgressArray();
}

void TickSimulation::configureObservation(const observe::ObservationYAMLConfig& config) {
    if (initialization_started_ || finalized_)
        throw std::logic_error("configure observation before simulation initialization");
    if (clock_mode_) throw std::logic_error("multiclock observation requires configureClockTrace");
    observe::ObservationManager::instance().acquireSession(config, this);
}

PortDirectory& TickSimulation::portDirectory() {
    if (!port_directory_) port_directory_ = std::make_unique<PortDirectory>();
    return *port_directory_;
}

const PortDirectory& TickSimulation::portDirectory() const {
    if (!port_directory_) port_directory_ = std::make_unique<PortDirectory>();
    return *port_directory_;
}

void TickSimulation::bindTreeNode(Unit& unit, tree::TreeNode& node) {
    if (initialization_started_ || finalized_)
        throw std::logic_error("cannot bind unit tree after initialization has started");
    validateClockOwner_(&unit);
    unit.setTreeNode(&node, portDirectory());
}

void TickSimulation::registerConnection(ConnectionBase* connection) {
    if (initialization_started_ || finalized_)
        throw std::logic_error("runtime connection registration is unsupported");
    if (!connection) return;
    validateClockOwner_(connection->source());
    validateClockOwner_(connection->destination());
    if (std::find(connections_.begin(), connections_.end(), connection) != connections_.end())
        throw std::invalid_argument("connection is already registered");
    connection->setConnId(static_cast<uint32_t>(connections_.size()));
    connections_.push_back(connection);
}

void TickSimulation::finalize() {
    if (finalized_) return;
    finalized_ = true;
    initialized_ = false;
    std::exception_ptr failure;
    for (auto& unit : units_) {
        if (unit->state_ != UnitState::Initialized) continue;
        unit->state_ = UnitState::Finalized;
        try {
            unit->finalize();
        } catch (...) {
            if (!failure) failure = std::current_exception();
        }
    }
    if (failure) std::rethrow_exception(failure);
}

void TickSimulation::setUnitName(Unit& unit, std::string name) {
    if (initialization_started_ || finalized_)
        throw std::logic_error("cannot rename units after initialization has started");
    validateClockOwner_(&unit);
    if (name.empty()) throw std::invalid_argument("unit instance name must not be empty");
    if (unit.treeNode()) throw std::logic_error("cannot rename a unit after tree binding");
    unit.setInstanceName_(std::move(name));
}

size_t TickSimulation::assignedThread(Unit* unit) const {
    if (!unit) return SIZE_MAX;
    for (size_t i = 0; i < unit_ptrs_.size(); ++i) {
        if (static_cast<Unit*>(unit_ptrs_[i]) != unit) continue;
        if (i < unit_to_cluster_.size()) {
            size_t cluster = unit_to_cluster_[i];
            if (cluster < cluster_to_thread_.size()) {
                return cluster_to_thread_[cluster];
            }
        }
        if (i < cluster_to_thread_.size()) {
            return cluster_to_thread_[i];
        }
        return SIZE_MAX;
    }
    return SIZE_MAX;
}

void TickSimulation::writeTimelineTrace() {
    if (!timeline_trace_.enabled()) {
        return;
    }
    auto& obs = observe::ObservationManager::instance();
    struct Write {
        SchedulerTimelineTrace& trace;
        std::filesystem::path directory;
        std::exception_ptr error;
    } output{timeline_trace_,
             obs.isBackendRunning() && obs.backend() ? obs.backend()->outputDir()
                                                     : std::filesystem::path{},
             {}};
    // The standalone scheduler timeline has no observation backend to submit
    // to, but its file I/O still belongs to the same scheduler-owned lane.
    auto job = hostServices().addIO({}, &output, [](void* context) noexcept {
        auto& output = *static_cast<Write*>(context);
        try {
            if (output.directory.empty())
                output.trace.write();
            else
                output.trace.write(output.directory);
        } catch (...) {
            output.error = std::current_exception();
        }
    });
    job->submit();
    job->wait();
    if (output.error) std::rethrow_exception(output.error);
}

TickableUnit* TickSimulation::getUnit(const std::string& name) {
    for (auto& unit : units_) {
        if (unit->name() == name || unit->fullPath() == name) {
            return unit.get();
        }
    }
    return nullptr;
}

}  // namespace chronon::sender
