// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// SPDX-License-Identifier: MPL-2.0
#include <iostream>
#include <stdexcept>

#include "chronon/HostServices.hpp"

using namespace chronon;

#define CHECK(condition)                                        \
    do {                                                        \
        if (!(condition)) throw std::runtime_error(#condition); \
    } while (false)

struct Probe : HostService {
    size_t poll(size_t) noexcept override {
        ++polls;
        return 1;
    }
    unsigned polls = 0;
};
struct Output {
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false, released = false;
    unsigned calls = 0;
    std::thread::id thread;
    static void run(void* context) noexcept {
        auto& self = *static_cast<Output*>(context);
        std::unique_lock lock(self.mutex);
        self.thread = std::this_thread::get_id();
        self.entered = true;
        ++self.calls;
        self.changed.notify_all();
        self.changed.wait(lock, [&] { return self.released; });
    }
};
int main() {
    auto scheduler = std::make_unique<HostServices>();
    Probe first, second;
    auto a = scheduler->add(first), b = scheduler->add(second);
    Output blocked, fast;
    fast.released = true;
    auto job_a = scheduler->addIO(a, &blocked, Output::run);
    auto job_b = scheduler->addIO(b, &fast, Output::run);
    a->setRunnable(false);
    job_a->submit();
    {
        std::unique_lock lock(blocked.mutex);
        blocked.changed.wait(lock, [&] { return blocked.entered; });
    }
    // A blocked sink does not occupy a model worker or its ingress claim.
    size_t cursor = 0;
    scheduler->poll(cursor);
    scheduler->poll(cursor);
    CHECK(first.polls == 0 && second.polls == 1);
    b->setRunnable(false);
    job_b->submit();
    {
        std::lock_guard lock(fast.mutex);
        CHECK(fast.calls == 0);  // Both outputs share one lane.
    }
    {
        std::lock_guard lock(blocked.mutex);
        blocked.released = true;
    }
    blocked.changed.notify_all();
    job_a->wait();
    job_b->wait();
    CHECK(blocked.thread == fast.thread && fast.thread != std::this_thread::get_id());
    a->poll();
    b->poll();
    CHECK(first.polls == 1 && second.polls == 2);
    // Observer shutdown can outlive the simulation. Outstanding I/O and final
    // flushes retain the executor, while detached ingress cannot call owners.
    {
        std::lock_guard lock(blocked.mutex);
        blocked.entered = blocked.released = false;
    }
    a->setRunnable(false);
    job_a->submit();
    {
        std::unique_lock lock(blocked.mutex);
        blocked.changed.wait(lock, [&] { return blocked.entered; });
    }
    scheduler.reset();
    {
        std::lock_guard lock(blocked.mutex);
        blocked.released = true;
    }
    blocked.changed.notify_all();
    job_a->wait();
    job_b->submit();
    job_b->wait();
    a->poll(true);
    b->poll(true);
    CHECK(first.polls == 1 && second.polls == 2);
    CHECK(blocked.calls == 2 && fast.calls == 2);
    job_a.reset();
    job_b.reset();
    std::cout << "scheduler I/O ownership, backpressure and lifetime passed\n";
}
