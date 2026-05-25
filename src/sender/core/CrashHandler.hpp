// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Crash handling for simulation tick exceptions and fatal signals.

#pragma once

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

namespace chronon::sender {

class TickableUnit;

namespace detail {

/**
 * Thread-local state tracking which unit is currently ticking.
 *
 * Set before each tick(), cleared after. Read by signal/exception handlers
 * to report which unit crashed.
 */
struct TickContext {
    TickableUnit* unit = nullptr;
    uint64_t cycle = 0;
    char unit_name[64] = {};      ///< copied into TLS via memcpy (heap-independent)
    const char* phase = nullptr;  ///< points to a string literal in .rodata
};

extern thread_local TickContext current_tick_context_;

/// RAII setter that always clears the thread-local tick context, even on
/// exception unwind.
class TickContextGuard {
public:
    TickContextGuard(TickableUnit* unit, uint64_t cycle, const char* unit_name, uint8_t name_len,
                     const char* phase) noexcept {
        current_tick_context_.unit = unit;
        current_tick_context_.cycle = cycle;
        current_tick_context_.phase = phase;
        // Precomputed length keeps this to ~1 instruction for short names.
        std::memcpy(current_tick_context_.unit_name, unit_name, name_len + 1);
    }

    ~TickContextGuard() noexcept {
        current_tick_context_.unit = nullptr;
        current_tick_context_.cycle = 0;
        current_tick_context_.unit_name[0] = '\0';
        current_tick_context_.phase = nullptr;
    }

    TickContextGuard(const TickContextGuard&) = delete;
    TickContextGuard& operator=(const TickContextGuard&) = delete;
};

}  // namespace detail

/**
 * Exception wrapping a crash during a unit's tick().
 *
 * Carries the unit name, cycle, and cause. Thrown by TickSimulation when a
 * tick() throws or when a parallel worker captures an exception.
 */
class TickException : public std::runtime_error {
public:
    TickException(std::string unit_name, uint64_t cycle, std::string cause)
        : std::runtime_error(buildMessage(unit_name, cycle, cause)),
          unit_name_(std::move(unit_name)),
          cycle_(cycle),
          cause_(std::move(cause)) {}

    const std::string& unitName() const noexcept { return unit_name_; }
    uint64_t cycle() const noexcept { return cycle_; }
    const std::string& cause() const noexcept { return cause_; }

private:
    static std::string buildMessage(const std::string& unit, uint64_t cycle,
                                    const std::string& cause) {
        return "Tick exception in unit '" + unit + "' at cycle " + std::to_string(cycle) + ": " +
               cause;
    }

    std::string unit_name_;
    uint64_t cycle_;
    std::string cause_;
};

/**
 * Signal handler and emergency flush for simulation crashes.
 *
 * Installs handlers for SIGSEGV/SIGBUS/SIGABRT/SIGFPE/SIGILL. On signal:
 * reads the thread-local tick context, writes crash info to stderr via
 * async-signal-safe write(), then _exit(128 + signo). emergencyFlush() can
 * also run from non-signal catch paths.
 */
class CrashHandler {
public:
    /// Idempotent. Throws std::runtime_error if signal registration fails.
    static void install();

    /// Best-effort flush of all observer data. NOT async-signal-safe — must
    /// be called only from normal catch paths.
    static void emergencyFlush();

private:
    static void signalHandler(int signo, siginfo_t* info, void* context);
    static std::atomic<bool> installed_;
};

}  // namespace chronon::sender
