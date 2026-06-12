# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Observability System CMakeLists.txt
#
# Unified observation system for performance counting, tracing, and logging.

# =============================================================================
# Library Definition
# =============================================================================

add_library(chronon_observe
    CounterRegistry.cpp
    DerivedCounter.cpp
    LocalCounter.cpp
    ObservationBackend.cpp
    ObservationBackendIO.cpp
    ObservationContext.cpp
    ObservationManager.cpp
    PerfettoTraceWriter.cpp
    ReorderBuffer.cpp
)

target_include_directories(chronon_observe
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../..>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
        $<INSTALL_INTERFACE:include>
)

# =============================================================================
# Dependencies
# =============================================================================

# Find and link fmt library
find_package(fmt QUIET)
if(fmt_FOUND)
    target_link_libraries(chronon_observe PUBLIC fmt::fmt)
else()
    # Fallback to system fmt library
    target_link_libraries(chronon_observe PUBLIC fmt)
endif()

# stdexec (header-only, for inplace_stop_token in ObservationBackend)
target_link_libraries(chronon_observe PUBLIC STDEXEC::stdexec)

# Perfetto SDK (PRIVATE: PerfettoTraceWriter hides it behind a pimpl, so
# perfetto.h never leaks into chronon headers)
target_link_libraries(chronon_observe PRIVATE perfetto_sdk)

# zlib for compressed_packets batches in the Perfetto writer
target_link_libraries(chronon_observe PRIVATE ZLIB::ZLIB)

# Thread support
find_package(Threads REQUIRED)
target_link_libraries(chronon_observe PUBLIC Threads::Threads)

target_compile_features(chronon_observe PUBLIC cxx_std_20)

# =============================================================================
# Compiler Flags (warnings, -Werror, sanitizers via centralized target)
# =============================================================================

target_link_libraries(chronon_observe PRIVATE chronon_compile_options)

# =============================================================================
# SIMD Support (AVX2 for watermark calculation)
# =============================================================================

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mavx2" HAS_AVX2)
if(HAS_AVX2)
    target_compile_options(chronon_observe PRIVATE
        $<$<CONFIG:Release>:-mavx2>
        $<$<CONFIG:RelWithDebInfo>:-mavx2>
    )
endif()
