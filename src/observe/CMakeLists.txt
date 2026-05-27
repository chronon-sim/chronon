# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Observability System CMakeLists.txt
#
# Unified observation system for performance counting, tracing, and logging.

# =============================================================================
# FlatBuffers Schema Generation
# =============================================================================

if(TARGET flatc)
    set(FLATBUFFERS_SCHEMA ${CMAKE_CURRENT_SOURCE_DIR}/TraceSchema.fbs)
    set(FLATBUFFERS_GENERATED ${CMAKE_CURRENT_BINARY_DIR}/TraceSchema_generated.h)

    add_custom_command(
        OUTPUT ${FLATBUFFERS_GENERATED}
        COMMAND $<TARGET_FILE:flatc> --cpp -o ${CMAKE_CURRENT_BINARY_DIR} ${FLATBUFFERS_SCHEMA}
        DEPENDS ${FLATBUFFERS_SCHEMA} flatc
        COMMENT "Generating FlatBuffers header from TraceSchema.fbs"
    )

    add_custom_target(generate_trace_schema DEPENDS ${FLATBUFFERS_GENERATED})
else()
    message(WARNING "flatc target not found - binary trace format will not be available")
endif()

# =============================================================================
# Library Definition
# =============================================================================

add_library(chronon_observe
    BinaryTraceReader.cpp
    BinaryTraceWriter.cpp
    CompressionBuffer.cpp
    CounterRegistry.cpp
    DerivedCounter.cpp
    LocalCounter.cpp
    ObservationBackend.cpp
    ObservationBackendIO.cpp
    ObservationContext.cpp
    ObservationManager.cpp
    ReorderBuffer.cpp
)

# Add dependency on generated FlatBuffers header
if(TARGET generate_trace_schema)
    add_dependencies(chronon_observe generate_trace_schema)
endif()

target_include_directories(chronon_observe
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../..>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
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

# FlatBuffers (header-only) - provided by CPM or system
if(TARGET flatbuffers)
    target_link_libraries(chronon_observe PUBLIC flatbuffers)
else()
    find_package(Flatbuffers QUIET)
    if(Flatbuffers_FOUND)
        target_link_libraries(chronon_observe PUBLIC flatbuffers::flatbuffers)
    else()
        find_path(FLATBUFFERS_INCLUDE_DIR flatbuffers/flatbuffers.h)
        if(FLATBUFFERS_INCLUDE_DIR)
            target_include_directories(chronon_observe PUBLIC ${FLATBUFFERS_INCLUDE_DIR})
        endif()
    endif()
endif()

# zstd compression (optional)
find_package(zstd QUIET)
if(zstd_FOUND)
    target_link_libraries(chronon_observe PUBLIC zstd::libzstd_shared)
    target_compile_definitions(chronon_observe PRIVATE CHRONON_HAS_ZSTD=1)
else()
    # Try pkg-config
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(ZSTD QUIET libzstd)
        if(ZSTD_FOUND)
            target_include_directories(chronon_observe PRIVATE ${ZSTD_INCLUDE_DIRS})
            target_link_libraries(chronon_observe PUBLIC ${ZSTD_LIBRARIES})
            target_compile_definitions(chronon_observe PRIVATE CHRONON_HAS_ZSTD=1)
        endif()
    endif()
endif()

# stdexec (header-only, for inplace_stop_token in ObservationBackend)
target_link_libraries(chronon_observe PUBLIC STDEXEC::stdexec)

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
