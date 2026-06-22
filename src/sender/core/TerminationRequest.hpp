// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// Unit-initiated simulation termination system.
///
/// Design goals:
/// - Very low hot-path overhead (~1-2ns per epoch, ~64 cycles)
/// - First request wins (thread-safe)
/// - Rich context: reason, exit code, cycle, unit name, message

#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wextra-semi"
#include <stdexec/stop_token.hpp>
#pragma GCC diagnostic pop

namespace chronon::sender {

/// Why the simulation is being terminated.
enum class TerminationReason : uint8_t {
    None = 0,
    Completed = 1,            ///< Normal completion (e.g., retired N instructions)
    ExitSyscall = 2,          ///< Exit syscall encountered
    Error = 3,                ///< Error condition detected
    UserInterrupted = 4,      ///< External stop (e.g., Ctrl+C, API call)
    MaxCyclesReached = 5,     ///< Hit cycle limit
    CheckpointRequested = 6,  ///< Checkpoint reached (for save/restore)
};

/// Context for a termination request: reason, exit code, cycle, unit, message.
struct TerminationRequest {
    TerminationReason reason = TerminationReason::None;
    int32_t exit_code = 0;
    uint64_t cycle = 0;
    std::string unit_name;
    std::string message;

    bool isRequested() const noexcept { return reason != TerminationReason::None; }

    std::string_view reasonString() const noexcept {
        switch (reason) {
            case TerminationReason::None:
                return "None";
            case TerminationReason::Completed:
                return "Completed";
            case TerminationReason::ExitSyscall:
                return "ExitSyscall";
            case TerminationReason::Error:
                return "Error";
            case TerminationReason::UserInterrupted:
                return "UserInterrupted";
            case TerminationReason::MaxCyclesReached:
                return "MaxCyclesReached";
            case TerminationReason::CheckpointRequested:
                return "CheckpointRequested";
            default:
                return "Unknown";
        }
    }
};

/**
 * Thread-safe controller for simulation termination.
 *
 * Hot-path check is a single relaxed atomic load (~1-2ns), called once per
 * epoch (~64 cycles). Request submission is mutex-protected and first-wins;
 * the cost (~80ns) is paid at most once per run.
 */
class TerminationController {
public:
    TerminationController() = default;
    ~TerminationController() = default;

    TerminationController(const TerminationController&) = delete;
    TerminationController& operator=(const TerminationController&) = delete;
    TerminationController(TerminationController&&) = delete;
    TerminationController& operator=(TerminationController&&) = delete;

    /**
     * Request simulation termination. Thread-safe; first request wins.
     *
     * @return true if this was the first request, false if already terminated.
     */
    bool requestTermination(TerminationReason reason, int32_t exit_code = 0, uint64_t cycle = 0,
                            std::string_view unit_name = "",
                            std::string_view message = "") noexcept {
        if (termination_requested_.load(std::memory_order_relaxed)) {
            return false;
        }

        std::lock_guard<std::mutex> lock(request_mutex_);

        if (termination_requested_.load(std::memory_order_relaxed)) {
            return false;
        }

        request_.reason = reason;
        request_.exit_code = exit_code;
        request_.cycle = cycle;
        request_.unit_name = std::string(unit_name);
        request_.message = std::string(message);

        // Release ordering pairs with acquire in getRequest() so consumers
        // observe the fully-populated request_ once the flag is set.
        termination_requested_.store(true, std::memory_order_release);

        if (stop_source_) {
            stop_source_->request_stop();
        }

        return true;
    }

    [[gnu::always_inline]] bool isTerminationRequested() const noexcept {
        return termination_requested_.load(std::memory_order_relaxed);
    }

    /// Only valid after isTerminationRequested() returns true.
    const TerminationRequest& getRequest() const noexcept {
        // Acquire load pairs with release in requestTermination().
        (void)termination_requested_.load(std::memory_order_acquire);
        return request_;
    }

    /**
     * Wire to an external stop_source for unified stop propagation.
     * When set, requestTermination() also calls stop_source->request_stop().
     */
    void setStopSource(stdexec::inplace_stop_source* src) noexcept { stop_source_ = src; }

    /// Not thread-safe — call only when simulation is stopped.
    void reset() noexcept {
        termination_requested_.store(false, std::memory_order_relaxed);
        request_ = TerminationRequest{};
    }

private:
    std::atomic<bool> termination_requested_{false};
    TerminationRequest request_;
    mutable std::mutex request_mutex_;
    stdexec::inplace_stop_source* stop_source_ = nullptr;
};

}  // namespace chronon::sender
