// SPDX-License-Identifier: MPL-2.0
#pragma once

#include <array>
#include <cassert>

#include "sender/schedule/EpochFreeTopologyCost.hpp"

namespace chronon::sender::epoch_free_cost {

// A bounded initial-placement snapshot needs no dynamic storage. Keep the full
// oracle's accumulation order, including its heavy-pair order; only invariant
// preparation and allocation differ. Larger graphs use PreparedTopologyCost.
class SmallPlacementCost {
public:
    static constexpr size_t kCapacity = 8;
    struct Summary {
        double objective = 0.0, max_active = 0.0, cross_pressure = 0.0;
        double max_incoming_pressure = 0.0, heavy_colocation_penalty = 0.0;
        double idle_thread_penalty = 0.0;
        size_t active_threads = 0;
        std::array<double, kCapacity> active{}, incoming_pressure{};
        std::array<size_t, kCapacity> heavy_count{};
    };

    SmallPlacementCost(const PartitionInput& input, size_t threads)
        : input_(input), threads_(threads) {
        assert(input.num_units <= kCapacity && threads <= kCapacity);
        std::array<double, kCapacity> incident{}, effective{};
        for (size_t u = 0; u < input.num_units; ++u) {
            if (u >= input.adjacency.size()) continue;
            for (const auto& edge : input.adjacency[u]) {
                const double pressure = edgePressure(input, edge);
                incident[u] += pressure;
                if (edge.neighbor < input.num_units) incident[edge.neighbor] += pressure;
            }
        }
        double total_effective = 0.0;
        for (size_t u = 0; u < input.num_units; ++u) {
            effective[u] = cost(u) + 0.02 * std::sqrt(incident[u]);
            total_effective += effective[u];
        }
        const double average = input.num_units ? total_effective / input.num_units : 0.0;
        for (size_t u = 0; u < input.num_units; ++u)
            if (effective[u] > average * 1.25) heavy_[heavy_size_++] = u;
        std::sort(heavy_.begin(), heavy_.begin() + heavy_size_, [&](size_t a, size_t b) {
            return effective[a] != effective[b] ? effective[a] > effective[b] : a < b;
        });
        heavy_size_ = std::min(heavy_size_, threads * 2);
        size_t pair = 0;
        for (size_t i = 0; i < heavy_size_; ++i) {
            for (size_t j = i + 1; j < heavy_size_; ++j) {
                const auto a = heavy_[i], b = heavy_[j];
                const double protection =
                    1.0 + directPressureBetween(input, a, b) / std::max(1.0, average);
                weights_[pair++] = 0.20 * std::min(effective[a], effective[b]) / protection;
            }
        }
    }

    Summary evaluate(const std::vector<size_t>& assignment) const {
        Summary out;
        if (!input_.num_units || !threads_) return out;
        double total_active = 0.0;
        for (size_t u = 0; u < input_.num_units; ++u) {
            const size_t owner = assignment[u];
            if (owner >= threads_) continue;
            out.active[owner] += cost(u);
            total_active += cost(u);
        }
        out.max_active = *std::max_element(out.active.begin(), out.active.begin() + threads_);
        for (size_t u = 0; u < input_.num_units; ++u) {
            if (u >= input_.adjacency.size()) continue;
            for (const auto& edge : input_.adjacency[u]) {
                if (edge.neighbor >= assignment.size()) continue;
                const size_t destination = assignment[edge.neighbor];
                if (assignment[u] == destination) continue;
                const double pressure = edgePressure(input_, edge);
                out.cross_pressure += pressure;
                if (destination < threads_) out.incoming_pressure[destination] += pressure;
            }
        }
        out.max_incoming_pressure = *std::max_element(out.incoming_pressure.begin(),
                                                      out.incoming_pressure.begin() + threads_);
        for (size_t i = 0; i < heavy_size_; ++i) {
            const size_t owner = assignment[heavy_[i]];
            if (owner < threads_) ++out.heavy_count[owner];
        }
        size_t pair = 0;
        for (size_t i = 0; i < heavy_size_; ++i) {
            for (size_t j = i + 1; j < heavy_size_; ++j, ++pair) {
                if (assignment[heavy_[i]] == assignment[heavy_[j]])
                    out.heavy_colocation_penalty += weights_[pair];
            }
        }
        const double average = total_active / threads_;
        double balance_penalty = 0.0;
        for (size_t t = 0; t < threads_; ++t) {
            if (out.active[t] > 0.0) ++out.active_threads;
            const double diff = out.active[t] - average;
            balance_penalty += diff * diff / std::max(1.0, average);
        }
        if (out.active_threads < threads_)
            out.idle_thread_penalty = 0.25 * total_active *
                                      static_cast<double>(threads_ - out.active_threads) /
                                      static_cast<double>(threads_);
        out.objective = out.max_active + 0.05 * balance_penalty + 0.35 * out.max_incoming_pressure +
                        0.12 * out.cross_pressure + out.heavy_colocation_penalty +
                        out.idle_thread_penalty;
        return out;
    }

private:
    double cost(size_t unit) const {
        return unit < input_.unit_cost_ns.size() ? input_.unit_cost_ns[unit] : 1.0;
    }
    const PartitionInput& input_;
    size_t threads_, heavy_size_ = 0;
    std::array<size_t, kCapacity> heavy_{};
    std::array<double, kCapacity*(kCapacity - 1) / 2> weights_{};
};

}  // namespace chronon::sender::epoch_free_cost
