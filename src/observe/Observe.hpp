// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Observe.hpp
//
// Compatibility include for the Chronon observation system. Prefer Observation.hpp.
//
// This header includes all necessary components for using the observation
// system in simulation units.

#pragma once

// Core types
#include "Types.hpp"

// Counter system
#include "Counter.hpp"

// High-performance per-thread queues
#include "SPSCQueue.hpp"
#include "ThreadContext.hpp"
#include "ThreadContextManager.hpp"

// Observation context
#include "ObservationContext.hpp"

// Filtering
#include "ObservationFilter.hpp"

// Modern macro-free API
#include "ObservationApi.hpp"

// Unit mixin
#include "ObservableUnit.hpp"

// Per-instance counters
#include "LocalCounter.hpp"

// Derived (computed) counters
#include "DerivedCounter.hpp"

/**
 * @file Observe.hpp
 * @brief Unified observability system for Chronon simulation framework.
 *
 * ## Overview
 *
 * The observation system provides three main capabilities:
 * - **Counters**: Per-unit counters (~2-3ns increment)
 * - **Tracing**: Filtered trace events (~2ns when disabled)
 * - **Logging**: Level-filtered debug logging
 *
 * ## Quick Start
 *
 * 1. Define categories:
 * @code
 * inline const auto CACHE_HIT = Category<"cache_hit", "Cache hit events">{};
 * inline const auto CACHE_MISS = Category<"cache_miss", "Cache miss events">{};
 * @endcode
 *
 * 2. Make your Unit inherit from ObservableUnit and declare Counter members:
 * @code
 * class MyUnit : public Unit, public observe::ObservableUnit {
 *     Counter instructions_{this, "instructions", "Instructions executed"};
 *
 * public:
 *     void tick() override {
 *         ++instructions_;
 *         instructions_ += 5;
 *
 *         trace<"Hit at addr=0x{:x}">(CACHE_HIT, addr);
 *
 *         debug<"Cycle {} processing">(cycle);
 *         info<"Operation complete">();
 *     }
 * };
 * @endcode
 *
 * ## Observation Lifecycle
 *
 * Observation is managed automatically by SimulationApp via ObservationManager.
 * Configuration is YAML-driven with per-channel (debug/info/warn/error/trace)
 * control over enable/format settings:
 * @code
 * // In main.cpp — SimulationApp handles everything automatically
 * int main(int argc, char* argv[]) {
 *     return chronon::SimulationApp("My Simulator")
 *         .setDefaultConfig("config.yaml")
 *         .run(argc, argv);
 * }
 * @endcode
 *
 * ## Performance
 *
 * | Operation | Latency |
 * |-----------|---------|
 * | Counter increment | ~0.5ns |
 * | Trace (disabled) | ~2ns |
 * | Trace (enabled, per-thread queues) | ~20-30ns |
 * | Log (disabled) | 0ns (compile-time elimination) |
 *
 * ## High-Performance Per-Thread Queues
 *
 * For multi-threaded simulations using TickSimulation + stdexec:
 * - Each thread gets its own lock-free SPSC queue (no mutex contention!)
 * - Backend thread polls all per-thread queues
 * - Throughput: ~50M events/sec with 8 threads
 *
 * ## Lookahead Support
 *
 * For parallel lookahead execution:
 * - Counters use thread-local storage (no contention)
 * - Events can be buffered and committed/rolled back with epochs
 *
 * @see ObservationContext for the main API
 * @see ObservableUnit for the unit mixin
 * @see ObservationManager for configuration and lifecycle
 */
