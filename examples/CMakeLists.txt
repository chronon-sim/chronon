# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Chronon Framework Examples
#
# These examples demonstrate the sender-based simulation framework
# using TickableUnit and TickSimulation with stdexec parallel execution.

cmake_minimum_required(VERSION 3.17)

# Compile and execute the actual introductory example, so documentation cannot
# silently drift from the public interface. Regenerate when README changes.
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${chronon_SOURCE_DIR}/README.md")
file(READ "${chronon_SOURCE_DIR}/README.md" CHRONON_README)
string(REGEX MATCH "## Basic Usage\n\n```cpp\n([^`]+)```" CHRONON_README_EXAMPLE "${CHRONON_README}")
if(NOT CHRONON_README_EXAMPLE)
    message(FATAL_ERROR "README Basic Usage must contain a complete C++ example")
endif()
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/readme_example.cpp" "${CMAKE_MATCH_1}")
add_executable(chronon_readme_example "${CMAKE_CURRENT_BINARY_DIR}/readme_example.cpp")
target_link_libraries(chronon_readme_example PRIVATE chronon::core chronon_compile_options)
add_test(NAME readme_example COMMAND chronon_readme_example)
set_tests_properties(readme_example PROPERTIES PROCESSORS 8 TIMEOUT 30)
add_dependencies(regress chronon_readme_example)

add_executable(multiclock_cdc_example multiclock_cdc.cpp)
target_link_libraries(multiclock_cdc_example PRIVATE chronon::core chronon_compile_options)
add_test(NAME multiclock_cdc_example COMMAND multiclock_cdc_example)

# ==============================================================================
# CPU Pipeline Example (Tick-based with stdexec parallel execution)
# ==============================================================================

if(CHRONON_ENABLE_OUTPORT_CANCELLATION)
    add_executable(cpu_pipeline_example
        cpu_pipeline_example.cpp
    )

    target_include_directories(cpu_pipeline_example PRIVATE
        ${CMAKE_SOURCE_DIR}/src
    )

    target_link_libraries(cpu_pipeline_example PRIVATE
        chronon::core
        chronon::observe
        STDEXEC::stdexec
        yaml-cpp::yaml-cpp
        chronon_compile_options
    )

    target_compile_features(cpu_pipeline_example PRIVATE cxx_std_20)

# ==============================================================================
# CPU Pipeline YAML Example (YAML-driven configuration)
# ==============================================================================

    add_executable(cpu_pipeline_yaml_example
        cpu_pipeline_yaml_example.cpp
    )

    target_include_directories(cpu_pipeline_yaml_example PRIVATE
        ${CMAKE_SOURCE_DIR}/src
    )

    target_link_libraries(cpu_pipeline_yaml_example PRIVATE
        chronon::core
        chronon::observe
        STDEXEC::stdexec
        yaml-cpp::yaml-cpp
        chronon_compile_options
    )

    target_compile_features(cpu_pipeline_yaml_example PRIVATE cxx_std_20)
endif()

# ==============================================================================
# Summary
# ==============================================================================

message(STATUS "Chronon Examples:")
if(CHRONON_ENABLE_OUTPORT_CANCELLATION)
    message(STATUS "  - cpu_pipeline_example: Multi-stage CPU pipeline (tick-based with stdexec)")
    message(STATUS "  - cpu_pipeline_yaml_example: CPU pipeline with YAML configuration")
else()
    message(STATUS "  - CPU pipeline examples: skipped (require OutPort cancellation)")
endif()
