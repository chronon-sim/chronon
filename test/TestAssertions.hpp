// Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
// Author: Haomeng Wang <chang_yun@outlook.com>
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

#pragma once

#include <cstdlib>
#include <iostream>

namespace chronon::test {

inline int& failureCount() {
    static int count = 0;
    return count;
}

inline void reportFailure(const char* kind, const char* expr, const char* file, int line) {
    std::cerr << kind << " failed at " << file << ":" << line << ": " << expr << "\n";
}

inline void recordExpectation(bool condition, const char* expr, const char* file, int line) {
    if (!condition) {
        reportFailure("EXPECT", expr, file, line);
        ++failureCount();
    }
}

[[noreturn]] inline void abortAssertion(const char* expr, const char* file, int line) {
    reportFailure("ASSERT", expr, file, line);
    std::abort();
}

}  // namespace chronon::test

#ifndef EXPECT
#define EXPECT(cond)                                                                            \
    do {                                                                                        \
        ::chronon::test::recordExpectation(static_cast<bool>(cond), #cond, __FILE__, __LINE__); \
    } while (0)
#endif

#ifndef ASSERT
#define ASSERT(cond)                                                    \
    do {                                                                \
        if (!(cond)) {                                                  \
            ::chronon::test::abortAssertion(#cond, __FILE__, __LINE__); \
        }                                                               \
    } while (0)
#endif

// Compatibility aliases for existing Chronon tests. They are intentionally
// fatal to preserve the previous CHECK/REQUIRE behavior.
#ifndef CHECK
#define CHECK(cond) ASSERT(cond)
#endif

#ifndef REQUIRE
#define REQUIRE(cond) ASSERT(cond)
#endif
