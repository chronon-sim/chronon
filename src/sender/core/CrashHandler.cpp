// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#include "CrashHandler.hpp"

#include <execinfo.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <stdexcept>
#include <string>

#include "../../observe/ObservationManager.hpp"
#include "../../observe/ThreadContextManager.hpp"

namespace chronon::sender {

namespace detail {
thread_local TickContext current_tick_context_;
}  // namespace detail

std::atomic<bool> CrashHandler::installed_{false};

namespace {

// In a fatal-signal handler there is no recovery if stderr writes fail, so
// ignore warn_unused_result on write().
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"

template <size_t N>
void safeWriteLiteral(const char (&s)[N]) {
    ::write(STDERR_FILENO, s, N - 1);
}

/// Write a uint64_t as decimal to stderr (async-signal-safe).
void safeWriteUint64(uint64_t val) {
    char buf[21];  // max uint64_t is 20 digits
    int pos = 20;
    buf[pos] = '\0';
    if (val == 0) {
        buf[--pos] = '0';
    } else {
        while (val > 0) {
            buf[--pos] = '0' + static_cast<char>(val % 10);
            val /= 10;
        }
    }
    ::write(STDERR_FILENO, &buf[pos], 20 - pos);
}

/// Write a null-terminated C string to stderr (async-signal-safe).
void safeWriteStr(const char* s) {
    if (!s) {
        safeWriteLiteral("(null)");
        return;
    }
    size_t len = 0;
    while (s[len] != '\0') ++len;
    ::write(STDERR_FILENO, s, len);
}

/// Write an uintptr_t as hex to stderr (async-signal-safe).
void safeWriteHex(uintptr_t val) {
    char buf[2 + sizeof(uintptr_t) * 2];
    buf[0] = '0';
    buf[1] = 'x';
    for (size_t i = 0; i < sizeof(uintptr_t) * 2; ++i) {
        unsigned nibble =
            static_cast<unsigned>((val >> ((sizeof(uintptr_t) * 2 - i - 1) * 4)) & 0xF);
        buf[2 + i] = static_cast<char>((nibble < 10) ? ('0' + nibble) : ('a' + nibble - 10));
    }
    ::write(STDERR_FILENO, buf, sizeof(buf));
}

#pragma GCC diagnostic pop

void safeWriteSignalName(int signo) {
    switch (signo) {
        case SIGSEGV:
            safeWriteLiteral("SIGSEGV");
            break;
        case SIGBUS:
            safeWriteLiteral("SIGBUS");
            break;
        case SIGABRT:
            safeWriteLiteral("SIGABRT");
            break;
        case SIGFPE:
            safeWriteLiteral("SIGFPE");
            break;
        case SIGILL:
            safeWriteLiteral("SIGILL");
            break;
        default:
            safeWriteLiteral("UNKNOWN");
            break;
    }
}
}  // anonymous namespace

void CrashHandler::install() {
    if (installed_.load(std::memory_order_acquire)) {
        return;
    }

    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = &CrashHandler::signalHandler;
    // SA_RESETHAND: one-shot, so a re-entered crash drops to the default handler.
    sa.sa_flags = SA_SIGINFO | SA_RESETHAND;
    sigemptyset(&sa.sa_mask);

    const int signals[] = {SIGSEGV, SIGBUS, SIGABRT, SIGFPE, SIGILL};
    for (int signo : signals) {
        if (sigaction(signo, &sa, nullptr) != 0) {
            const int err = errno;
            throw std::runtime_error("CrashHandler::install failed for signal " +
                                     std::to_string(signo) + ": " + std::strerror(err));
        }
    }

    installed_.store(true, std::memory_order_release);
}

void CrashHandler::emergencyFlush() {
    observe::ThreadContextManager::instance().flushAll();

    auto& obs_mgr = observe::ObservationManager::instance();
    if (obs_mgr.isEnabled() && obs_mgr.isBackendRunning()) {
        obs_mgr.stopBackend();
    }
}

void CrashHandler::signalHandler(int signo, siginfo_t* /*info*/, void* /*context*/) {
    safeWriteLiteral("\n=== CHRONON CRASH ===\n");
    safeWriteLiteral("Signal: ");
    safeWriteSignalName(signo);
    safeWriteLiteral(" (");
    safeWriteUint64(static_cast<uint64_t>(signo));
    safeWriteLiteral(")\n");

    const auto ctx = detail::current_tick_context_;
    if (ctx.unit) {
        safeWriteLiteral("TickCtx: active\n");
        safeWriteLiteral("Unit:    ");
        safeWriteStr(ctx.unit_name);
        safeWriteLiteral("\n");
        safeWriteLiteral("Phase:   ");
        safeWriteStr(ctx.phase);
        safeWriteLiteral("\n");
        safeWriteLiteral("Cycle:   ");
        safeWriteUint64(ctx.cycle);
        safeWriteLiteral("\n");
        safeWriteLiteral("UnitPtr: ");
        safeWriteHex(reinterpret_cast<uintptr_t>(ctx.unit));
        safeWriteLiteral("\n");
    } else {
        safeWriteLiteral("TickCtx: inactive\n");
    }

    // Skip backtrace under sanitizers — backtrace() calls malloc, which is
    // not async-signal-safe and deadlocks ASan/TSan signal handling.
#if !defined(CHRONON_SANITIZER_BUILD)
    safeWriteLiteral("Backtrace:\n");
    void* bt[32];
    int depth = ::backtrace(bt, 32);
    ::backtrace_symbols_fd(bt, depth, STDERR_FILENO);
#endif

    safeWriteLiteral("Exiting now.\n");
    _exit(128 + signo);
}

}  // namespace chronon::sender
