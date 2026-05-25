# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# ChrononCompileOptions.cmake
#
# Centralized compile options for all Chronon project targets.
# Creates chronon_compile_options INTERFACE target.
#
# Usage: target_link_libraries(my_target PRIVATE chronon_compile_options)

include_guard(GLOBAL)

# ── Debug: use -g3 for full macro debug info ──────────────────────────
set(CMAKE_C_FLAGS_DEBUG   "-O0 -g3" CACHE STRING "C flags for Debug" FORCE)
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3" CACHE STRING "C++ flags for Debug" FORCE)

# ── Options ──────────────────────────────────────────────────────────────
option(CHRONON_ENABLE_WERROR     "Treat warnings as errors"         OFF)
option(CHRONON_ENABLE_ASAN       "Enable AddressSanitizer + UBSan"  OFF)
option(CHRONON_ENABLE_TSAN       "Enable ThreadSanitizer"           OFF)
option(CHRONON_ENABLE_CLANG_TIDY "Enable clang-tidy static analysis" OFF)
option(CHRONON_ENABLE_COVERAGE  "Enable code coverage (gcov, GCC only)" OFF)

# ASan and TSan are mutually exclusive
if(CHRONON_ENABLE_ASAN AND CHRONON_ENABLE_TSAN)
    message(FATAL_ERROR "CHRONON_ENABLE_ASAN and CHRONON_ENABLE_TSAN cannot be enabled simultaneously.")
endif()

# ── Interface target ─────────────────────────────────────────────────────
add_library(chronon_compile_options INTERFACE)

# Base warnings
target_compile_options(chronon_compile_options INTERFACE
    -Wall
    -Wextra
    -Wpedantic
)

# -Werror
if(CHRONON_ENABLE_WERROR)
    target_compile_options(chronon_compile_options INTERFACE -Werror)
    message(STATUS "Chronon: -Werror ENABLED")
endif()

# ── Sanitizer: ASan + UBSan ─────────────────────────────────────────────
if(CHRONON_ENABLE_ASAN)
    target_compile_options(chronon_compile_options INTERFACE
        -fsanitize=address,undefined
        -fno-omit-frame-pointer
        -fno-sanitize-recover=undefined
    )
    target_link_options(chronon_compile_options INTERFACE
        -fsanitize=address,undefined
    )
    message(STATUS "Chronon: ASan + UBSan ENABLED")
endif()

# ── Sanitizer: TSan ──────────────────────────────────────────────────────
if(CHRONON_ENABLE_TSAN)
    target_compile_options(chronon_compile_options INTERFACE
        -fsanitize=thread
        -fno-omit-frame-pointer
    )
    target_link_options(chronon_compile_options INTERFACE
        -fsanitize=thread
    )
    message(STATUS "Chronon: TSan ENABLED")
endif()

# ── Sanitizer compile definition (used to gate signal-unsafe code) ──────
if(CHRONON_ENABLE_ASAN OR CHRONON_ENABLE_TSAN)
    target_compile_definitions(chronon_compile_options INTERFACE
        CHRONON_SANITIZER_BUILD
    )
endif()

# ── Code coverage (gcov) ────────────────────────────────────────────────
if(CHRONON_ENABLE_COVERAGE)
    target_compile_options(chronon_compile_options INTERFACE --coverage)
    target_link_options(chronon_compile_options INTERFACE --coverage)
    message(STATUS "Chronon: Code coverage ENABLED")
endif()

# ── clang-tidy ───────────────────────────────────────────────────────────
if(CHRONON_ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_EXE NAMES clang-tidy)
    if(CLANG_TIDY_EXE)
        set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXE}" PARENT_SCOPE)
        message(STATUS "Chronon: clang-tidy ENABLED (${CLANG_TIDY_EXE})")
    else()
        message(WARNING "CHRONON_ENABLE_CLANG_TIDY=ON but clang-tidy not found")
    endif()
endif()
