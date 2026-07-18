// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// @file
/// State-machine based unit for high-performance simulation.

#pragma once

#include <exception>

#include "../util/PipelinePhase.hpp"
#include "CrashHandler.hpp"
#include "TerminationRequest.hpp"
#include "Unit.hpp"

namespace chronon::sender {

/**
 * State-machine based simulation unit.
 *
 * tick() is called every cycle; state lives in member variables. Avoids
 * coroutine overhead and integrates with stdexec parallel execution.
 *
 * @code
 * class FetchUnit : public TickableUnit {
 *     OutPort<Instruction> out{this, "out"};
 *     uint64_t pc_ = 0;
 *     void tick() override {
 *         if (out.canSend()) out.send(Instruction{pc_++});
 *     }
 * };
 * @endcode
 */
class TickableUnit : public Unit {
public:
    explicit TickableUnit(std::string name) : Unit(std::move(name)) {}

    virtual ~TickableUnit() = default;

    /// Per-cycle behavior. Must complete synchronously.
    virtual void tick() = 0;

    /// Return true to signal the simulation can stop.
    virtual bool isCompleted() const { return false; }

    /// Inlined hot path executed millions of times per second.
    [[gnu::always_inline]] inline void executeTick() {
        detail::TickContextGuard tick_ctx(this, localCycle(), crashName(), crashNameLen(), "tick");
        try {
            beginActiveTick_();
            prepareCyclePorts_();
            tick();
            finishActiveTick_();
            advanceLocalCycle();
        } catch (const TickException&) {
            throw;
        } catch (const std::exception& e) {
            throw TickException(name(), localCycle(), e.what());
        } catch (...) {
            throw TickException(name(), localCycle(), "unknown exception");
        }
    }

    /// Hot path for units that have not opted into activity scheduling.
    [[gnu::always_inline]] inline void executeTickAlwaysActive() {
        detail::TickContextGuard tick_ctx(this, localCycle(), crashName(), crashNameLen(), "tick");
        try {
            prepareCyclePorts_();
            tick();
            advanceLocalCycle();
        } catch (const TickException&) {
            throw;
        } catch (const std::exception& e) {
            throw TickException(name(), localCycle(), e.what());
        } catch (...) {
            throw TickException(name(), localCycle(), "unknown exception");
        }
    }

    /// Batched inactive-cycle advance used when an entire cluster is idle.
    [[gnu::always_inline]] inline void advanceIdleTick(uint64_t delta) { advanceLocalCycle(delta); }

protected:
    /**
     * Phase-templated tick for compile-time pipeline slot selection.
     *
     * Override instead of tick() when using StageReg. Phase0/Phase1
     * dispatch is based on cycle parity. Default delegates to tick().
     *
     * @code
     * template<ValidPhase P>
     * void tickPhase() {
     *     if (reg_.valid<P>()) process(reg_.get<P>());
     *     reg_.set<P>(new_data);
     * }
     * @endcode
     */
    template <ValidPhase P>
    void tickPhase() {
        tick();
    }

    /**
     * Request simulation termination. First request wins; the simulation stops
     * at the next scheduler boundary, and parallel lookahead workers are
     * signaled through the shared stop token.
     */
    void requestTermination(TerminationReason reason, int32_t exit_code = 0,
                            std::string_view message = "") {
        if (termination_ctrl_) {
            termination_ctrl_->requestTermination(reason, exit_code, localCycle(), name(), message);
        }
    }

    /// Overload taking an explicit cycle (avoids cross-thread localCycle()
    /// reads in MMIO callbacks).
    void requestTermination(TerminationReason reason, int32_t exit_code, uint64_t cycle,
                            std::string_view message) {
        if (termination_ctrl_) {
            termination_ctrl_->requestTermination(reason, exit_code, cycle, name(), message);
        }
    }

    void requestExitSyscall(int32_t exit_code) {
        requestTermination(TerminationReason::ExitSyscall, exit_code, "Exit syscall");
    }

    void requestError(std::string_view message) {
        requestTermination(TerminationReason::Error, 1, message);
    }

private:
    friend class TickSimulation;

    void setTerminationController(TerminationController* ctrl) noexcept {
        termination_ctrl_ = ctrl;
    }

    TerminationController* termination_ctrl_ = nullptr;
};

}  // namespace chronon::sender
