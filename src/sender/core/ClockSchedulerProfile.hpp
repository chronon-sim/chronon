// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <chrono>
#include <cstdint>

#include "../../chronon/HostServices.hpp"

namespace chronon::sender {

/// Opt-in host diagnostics, accumulated by one worker and read only between runs.
/// Timings/counts sample one sweep in 64; actor_ns includes tick_ns and bridge_ns.
/// They describe sampled wall time, not CPU time or a sum of independent costs.
struct alignas(64) ClockSchedulerProfile {
    uint64_t sweeps = 0, idle_sweeps = 0;
    uint64_t retirement_ns = 0, admission_ns = 0, actor_ns = 0;
    uint64_t tick_ns = 0, bridge_ns = 0, wait_ns = 0;
    uint64_t cluster_polls = 0, bridge_polls = 0, cluster_ticks = 0, bridge_commits = 0;
    uint64_t allowance_waits = 0, dependency_waits = 0, completion_loads = 0;
};

namespace detail {
// Same steady clock used by scheduler sampling. Disabled scopes read no clock.
class ClockProfileScope {
public:
    explicit ClockProfileScope(uint64_t* total) : total_(total) {
        if (total_) {
            service_begin_ = HostServiceRegistration::thread_service_ns;
            begin_ = std::chrono::steady_clock::now();
        }
    }
    ~ClockProfileScope() { finish(); }
    void finish() {
        if (!total_) return;
        const auto elapsed =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                      std::chrono::steady_clock::now() - begin_)
                                      .count());
        const auto service = HostServiceRegistration::thread_service_ns - service_begin_;
        *total_ += elapsed > service ? elapsed - service : 0;
        total_ = nullptr;
    }

private:
    uint64_t* total_;
    std::chrono::steady_clock::time_point begin_;
    uint64_t service_begin_ = 0;
};
}  // namespace detail
}  // namespace chronon::sender
