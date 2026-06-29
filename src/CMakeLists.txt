# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Chronon Simulation Framework
# High-performance sender-based simulation framework for CPU microarchitecture modeling

cmake_minimum_required(VERSION 3.17)

# Framework core library (header-only sender framework with minimal source)
add_library(chronon STATIC
    # Tree hierarchy system (still used)
    tree/TreeNode.cpp

    # Crash handler for simulation tick exceptions and fatal signals
    sender/core/CrashHandler.cpp

    # Tick-based simulation driver (split across three files by concern)
    sender/core/TickSimulation.cpp
    sender/core/TickSimulationPartition.cpp
    sender/core/TickSimulationParallel.cpp
    sender/core/TickSimulationDynamicRebalance.cpp
    sender/core/TickSimulationDynamicRuntime.cpp
    sender/core/TickSimulationValidation.cpp

    # SimulationApp - unified entry point for simulations
    sender/app/SimulationApp.cpp
)

# Alias for consistent naming
add_library(chronon::core ALIAS chronon)

# Include directories
target_include_directories(chronon
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# C++20 required
target_compile_features(chronon PUBLIC cxx_std_20)

# Link stdexec for sender/receiver support
target_link_libraries(chronon PUBLIC STDEXEC::stdexec)

# Link yaml-cpp for configuration support
target_link_libraries(chronon PUBLIC yaml-cpp::yaml-cpp)

# Thread support
find_package(Threads REQUIRED)
target_link_libraries(chronon PUBLIC Threads::Threads)

# Compiler flags (warnings, -Werror, sanitizers via centralized target)
target_link_libraries(chronon PRIVATE chronon_compile_options)

# Enable position-independent code for use in shared libraries
set_property(TARGET chronon PROPERTY POSITION_INDEPENDENT_CODE ON)

# ============================================================================
# Add observe subsystem (unified observability: counters, traces, logs)
# ============================================================================
add_subdirectory(observe)

# Alias for observe library
add_library(chronon::observe ALIAS chronon_observe)

# Link observe library to core (needed for SimulationApp)
target_link_libraries(chronon PUBLIC chronon_observe)

# Install rules gated behind CHRONON_INSTALL (OFF by default).
# stdexec (from CPM) is not in any export set, so install(EXPORT) fails
# unless the consumer explicitly handles dependency resolution.
if(CHRONON_INSTALL)
    install(DIRECTORY chronon/ sender/ tree/ observe/
        DESTINATION include/chronon
        FILES_MATCHING PATTERN "*.hpp"
    )

    install(TARGETS chronon chronon_observe chronon_compile_options
        EXPORT ChrononTargets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include
    )

    install(EXPORT ChrononTargets
        FILE ChrononTargets.cmake
        NAMESPACE chronon::
        DESTINATION lib/cmake/chronon
    )
endif()

# Framework configuration summary
message(STATUS "Chronon Sender Framework Configuration:")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Standard: C++20")
message(STATUS "  Compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
