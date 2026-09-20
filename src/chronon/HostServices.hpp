// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace chronon {

/// Host work has no simulated clock, dependency edges, or lookahead frontier.
/// A poll must return without waiting for another worker or performing sink I/O.
class HostService {
public:
    virtual ~HostService() = default;
    virtual size_t poll(size_t record_budget) noexcept = 0;
};

/// A registration stays alive after removal so cached producer assistance cannot
/// call a destroyed service. detach() waits for the last claim, not for output.
class HostServiceRegistration {
public:
    explicit HostServiceRegistration(HostService& service) : service_(&service) {}
    std::atomic<bool> ready{true};

    size_t poll(bool urgent = false) noexcept {
        if (state_.load(std::memory_order_acquire) != State::Runnable) return 0;
        if (!urgent && !ready.load(std::memory_order_acquire)) return 0;
        std::unique_lock lock(mutex_, std::try_to_lock);
        if (!lock || !service_ || state_.load(std::memory_order_acquire) != State::Runnable)
            return 0;
        // Clear BEFORE consuming. Concurrent publication/completion then stays
        // pending, including notification while the consumer is releasing its claim.
        ready.exchange(false, std::memory_order_acq_rel);
        const auto begin = std::chrono::steady_clock::now();
        const auto records = service_->poll(256);
        const auto ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                  std::chrono::steady_clock::now() - begin)
                                                  .count());
        thread_service_ns += ns;
        ++calls_;
        records_ += records;
        elapsed_ns_ += ns;
        if (ns > max_poll_ns_) max_poll_ns_ = ns;
        return records;
    }

    /// Pause claims while a bounded handoff is owned by I/O. Publications still
    /// latch ready; completion resumes claims without losing those notifications.
    void setRunnable(bool value) noexcept {
        auto state = state_.load(std::memory_order_relaxed);
        while (state != State::Detached &&
               !state_.compare_exchange_weak(state, value ? State::Runnable : State::Paused,
                                             std::memory_order_release,
                                             std::memory_order_relaxed)) {
        }
        // Completion after detach must not reactivate a retired registration.
        if (value && state != State::Detached) ready.store(true, std::memory_order_release);
    }

    void detach() noexcept {
        std::lock_guard lock(mutex_);
        service_ = nullptr;
        state_.store(State::Detached, std::memory_order_release);
        ready.store(false, std::memory_order_release);
    }
    void activate(HostService& service) noexcept {
        std::lock_guard lock(mutex_);
        service_ = &service;
        state_.store(State::Runnable, std::memory_order_release);
        ready.store(true, std::memory_order_release);
    }

    struct Stats {
        uint64_t calls, records, elapsed_ns, max_poll_ns;
    };
    Stats stats() const {
        std::lock_guard lock(mutex_);
        return {calls_, records_, elapsed_ns_, max_poll_ns_};
    }
    static inline thread_local uint64_t thread_service_ns = 0;

private:
    mutable std::mutex mutex_;
    HostService* service_;
    enum class State : uint8_t { Runnable, Paused, Detached };
    std::atomic<State> state_{State::Runnable};
    uint64_t calls_ = 0, records_ = 0, elapsed_ns_ = 0, max_poll_ns_ = 0;
};

class HostIOJob;

namespace host_services_detail {
// One lazy blocking-I/O lane per scheduler. Jobs own this state so output can
// finish even when the simulation itself is destroyed before its observers.
struct HostIOExecutor {
    explicit HostIOExecutor(bool drive) : drive_services(drive) {}
    ~HostIOExecutor();
    void run() noexcept;
    std::mutex mutex;
    std::condition_variable changed;
    std::vector<HostIOJob*> jobs;
    std::vector<std::shared_ptr<HostServiceRegistration>> services;
    std::thread worker;
    size_t cursor = 0, service_cursor = 0;
    bool stopping = false;
    const bool drive_services;
};
}  // namespace host_services_detail

/// A preallocated, single-flight I/O task. submit() never waits for output;
/// wait() fences the callback and may only be used with producers quiescent.
/// The owner must detach its ingress service before destroying the job.
class HostIOJob {
public:
    using Callback = void (*)(void*) noexcept;
    ~HostIOJob() {
        wait();
        std::lock_guard lock(executor_->mutex);
        for (auto& job : executor_->jobs)
            if (job == this) job = nullptr;
    }
    HostIOJob(const HostIOJob&) = delete;
    HostIOJob& operator=(const HostIOJob&) = delete;
    void submit() noexcept {
        {
            std::lock_guard lock(executor_->mutex);
            // Overwriting an outstanding task would race its borrowed buffer.
            if (busy_) std::terminate();
            busy_ = pending_ = true;
        }
        executor_->changed.notify_all();
    }
    void wait() noexcept {
        std::unique_lock lock(executor_->mutex);
        executor_->changed.wait(lock, [&] { return !busy_; });
    }

private:
    friend class HostServices;
    friend struct host_services_detail::HostIOExecutor;
    HostIOJob(std::shared_ptr<host_services_detail::HostIOExecutor> executor,
              std::shared_ptr<HostServiceRegistration> service, void* context, Callback callback)
        : executor_(std::move(executor)),
          service_(std::move(service)),
          context_(context),
          callback_(callback) {}
    std::shared_ptr<host_services_detail::HostIOExecutor> executor_;
    std::shared_ptr<HostServiceRegistration> service_;
    void* context_;
    Callback callback_;
    bool busy_ = false, pending_ = false;
};

inline host_services_detail::HostIOExecutor::~HostIOExecutor() {
    {
        std::lock_guard lock(mutex);
        stopping = true;
    }
    changed.notify_all();
    if (worker.joinable()) worker.join();
}

inline void host_services_detail::HostIOExecutor::run() noexcept {
    std::unique_lock lock(mutex);
    while (!stopping) {
        HostIOJob* next = nullptr;
        for (size_t i = 0; i < jobs.size(); ++i) {
            auto* candidate = jobs[cursor++ % jobs.size()];
            if (candidate && candidate->pending_) {
                next = candidate;
                break;
            }
        }
        if (next) {
            next->pending_ = false;
            lock.unlock();
            next->callback_(next->context_);
            lock.lock();
            next->busy_ = false;
            // Resume ingress only after its I/O slot is reusable. This also
            // preserves publications made while the previous batch was busy.
            if (next->service_) next->service_->setRunnable(true);
            changed.notify_all();
            continue;
        }
        if (drive_services && !services.empty()) {
            // Standalone users reuse the same bounded ingress on this lane;
            // simulations instead supply poll opportunities on model workers.
            const size_t count = services.size();
            lock.unlock();
            for (size_t i = 0; i < count; ++i) {
                lock.lock();
                auto service = services[service_cursor++ % services.size()];
                lock.unlock();
                service->poll();
            }
            lock.lock();
            bool pending = false;
            for (auto* job : jobs) pending |= job && job->pending_;
            if (pending) continue;
            changed.wait_for(lock, std::chrono::microseconds(50));
        } else {
            changed.wait(lock);
        }
    }
}

/// Scheduler-owned registrations and blocking I/O. Register only while model
/// workers are quiescent. Each opportunity visits one ingress/job fairly.
class HostServices {
public:
    explicit HostServices(bool drive_services = false)
        : executor_(std::make_shared<host_services_detail::HostIOExecutor>(drive_services)) {}
    ~HostServices() {
        for (auto& entry : entries_) entry->detach();
    }
    std::shared_ptr<HostServiceRegistration> add(HostService& service) {
        auto entry = std::make_shared<HostServiceRegistration>(service);
        entries_.push_back(entry);
        if (executor_->drive_services) {
            std::lock_guard lock(executor_->mutex);
            executor_->services.push_back(entry);
        }
        return entry;
    }
    std::unique_ptr<HostIOJob> addIO(std::shared_ptr<HostServiceRegistration> service,
                                     void* context, HostIOJob::Callback callback) {
        auto job = std::unique_ptr<HostIOJob>(
            new HostIOJob(executor_, std::move(service), context, callback));
        {
            std::lock_guard lock(executor_->mutex);
            if (!executor_->worker.joinable())
                executor_->worker = std::thread([state = executor_.get()] { state->run(); });
            // Reuse retired slots so repeated observation sessions stay bounded.
            for (auto& slot : executor_->jobs) {
                if (!slot) {
                    slot = job.get();
                    return job;
                }
            }
            executor_->jobs.push_back(job.get());
        }
        return job;
    }
    void poll(size_t& cursor) noexcept {
        if (entries_.empty()) return;
        entries_[cursor++ % entries_.size()]->poll();
    }

private:
    std::vector<std::shared_ptr<HostServiceRegistration>> entries_;
    std::shared_ptr<host_services_detail::HostIOExecutor> executor_;
};

}  // namespace chronon
