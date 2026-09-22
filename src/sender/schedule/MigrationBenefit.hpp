// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <vector>

namespace chronon::sender::detail {

// Admission/feedback state for the existing clock planner. One planner owns
// this state; workers continue executing while estimates are inspected. Costs
// and progress share the configured reference-clock basis, never local cycles.
struct MigrationBenefit {
    static uint64_t now() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }
    struct Confidence {
        double cost = 0;
        uint64_t samples = 0;
        bool stable = false;
        bool observe(double next, uint64_t count, bool ready) {
            if (!ready || !std::isfinite(next)) {
                *this = {};
                return false;
            }
            if (!samples || count < samples) {
                cost = next;
                samples = count;
                stable = false;
            } else if (count - samples >= 4) {
                stable = std::abs(next - cost) <= 0.25 * std::max(next, cost);
                cost = next;
                samples = count;
            }
            return stable;
        }
    };
    struct Sample {
        uint64_t active_ns = 0, active = 0, inactive_ns = 0, inactive = 0;
        uint64_t cycles = 0, active_cycles = 0;
    };
    struct Window {
        Sample previous;
        double cost = 0;
        uint64_t samples = 0;
        bool ready = false;
        void observe(Sample now) {
            if (now.active < previous.active || now.inactive < previous.inactive ||
                now.cycles < previous.cycles)
                *this = {};
            const auto cycles = now.cycles - previous.cycles;
            if (cycles < 4) return;
            const auto active_cycles = std::min(cycles, now.active_cycles - previous.active_cycles);
            const auto active = now.active - previous.active;
            const auto inactive = now.inactive - previous.inactive;
            if ((active_cycles && active < 4) || (active_cycles < cycles && inactive < 4)) return;
            const double rate = double(active_cycles) / cycles;
            cost = active_cycles ? rate * double(now.active_ns - previous.active_ns) / active : 0;
            if (active_cycles < cycles)
                cost += (1 - rate) * double(now.inactive_ns - previous.inactive_ns) / inactive;
            samples += active_cycles == cycles ? active
                       : !active_cycles        ? inactive
                                               : std::min(active, inactive);
            ready = true;
            previous = now;
        }
    };
    std::vector<Confidence> confidence;
    std::vector<Window> windows;
    unsigned backoff = 1;
    double planning_ns = 0, handoff_ns = 0, timer_ns = 0;
    uint64_t window_cycle = 0, window_ns = 0;
    double before_ns_per_cycle = 0;
    uint64_t pending_generation = 0, pending_cycle = 0;
    bool pending = false;
    uint64_t planning_calls = 0, planning_total_ns = 0;
    uint64_t feedback_good = 0, feedback_bad = 0;

    static uint64_t add(uint64_t a, uint64_t b) { return a + std::min(b, UINT64_MAX - a); }
    static uint64_t multiply(uint64_t a, uint64_t b) {
        return a && b > UINT64_MAX / a ? UINT64_MAX : a * b;
    }
    static double netSaving(double active_gain, double topology_delta) {
        // Heuristic bonuses rank moves but are not nanoseconds saved. Do not
        // claim that removing communication necessarily speeds the workload up;
        // charge added communication fully and discount the active estimate.
        return 0.5 * active_gain + std::min(0.0, topology_delta);
    }
    bool profitable(double active_gain, double topology_delta, double actor_tick_ns,
                    double max_active, double horizon, double current_planning_ns, double timer_ns,
                    size_t actors, double observed_ns_per_cycle, double min_gain) const {
        const double saving = netSaving(active_gain, topology_delta);
        // Timer/profiling allowance and a cold-cache allowance prevent a tiny
        // nominal gain from buying frequent moves. These are conservative cost
        // estimates, not an assertion that all elapsed handoff time is CPU work.
        const double overhead = std::max(planning_ns, current_planning_ns) + handoff_ns +
                                2 * timer_ns * actors + 8 * actor_tick_ns;
        // Model cost omits coordinator/polling work. A tiny fractional model
        // improvement cannot establish a useful wall-time gain when that work
        // dominates. Apply the same safety margin to the requested gain floor.
        const double uncertainty =
            std::max(0.02 * max_active, 4 * min_gain * observed_ns_per_cycle);
        return std::isfinite(saving) && observed_ns_per_cycle > 0 && saving > uncertainty &&
               horizon > 0 && saving * horizon > 4 * overhead;
    }
    void defer() { backoff = std::min(32u, backoff * 2); }
    uint64_t interval(uint64_t base) const { return multiply(base, backoff); }
    void startRun(uint64_t cycle, uint64_t now) {
        window_cycle = cycle;
        window_ns = now;
        pending = false;  // Never include time while the simulation was stopped.
    }
    double rate(uint64_t cycle, uint64_t now) const {
        return cycle > window_cycle && now >= window_ns
                   ? static_cast<double>(now - window_ns) / (cycle - window_cycle)
                   : 0;
    }
    // Returns false while one move is being assessed. There is no execution
    // fence: only another ownership move waits for enough physical progress.
    bool feedback(uint64_t cycle, uint64_t now, uint64_t generation, uint64_t interval) {
        if (!pending) return true;
        if (generation == pending_generation) return false;
        if (cycle < add(pending_cycle, multiply(interval, 4))) return false;
        const double after = rate(cycle, now);
        if (after > 0 && before_ns_per_cycle > 0 && after < before_ns_per_cycle * 0.98) {
            ++feedback_good;
            backoff = 1;
        } else {
            ++feedback_bad;
            defer();
        }
        pending = false;
        window_cycle = cycle;
        window_ns = now;
        return true;
    }
};

}  // namespace chronon::sender::detail
