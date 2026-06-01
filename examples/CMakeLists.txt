# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Chronon Framework Examples
#
# These examples demonstrate the sender-based simulation framework
# using TickableUnit and TickSimulation with stdexec parallel execution.

cmake_minimum_required(VERSION 3.17)

# ==============================================================================
# CPU Pipeline Example (Tick-based with stdexec parallel execution)
# ==============================================================================

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

# ==============================================================================
# Parallel Scheduler Benchmark (issue #24 hot-path micro-benchmark)
# ==============================================================================

add_executable(bench_parallel_scheduler
    bench_parallel_scheduler.cpp
)

target_include_directories(bench_parallel_scheduler PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(bench_parallel_scheduler PRIVATE
    chronon::core
    chronon::observe
    STDEXEC::stdexec
    yaml-cpp::yaml-cpp
    chronon_compile_options
)

target_compile_features(bench_parallel_scheduler PRIVATE cxx_std_20)

# ==============================================================================
# Summary
# ==============================================================================

message(STATUS "Chronon Examples:")
message(STATUS "  - cpu_pipeline_example: Multi-stage CPU pipeline (tick-based with stdexec)")
message(STATUS "  - cpu_pipeline_yaml_example: CPU pipeline with YAML configuration")
