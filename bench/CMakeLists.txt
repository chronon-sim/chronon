# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Experimental benchmark targets. These are intentionally opt-in through
# CHRONON_BUILD_BENCHMARKS so they do not become part of the default examples or
# test surface.

function(add_chronon_bench target)
    add_executable(${target} ${ARGN})
    target_include_directories(${target} PRIVATE
        ${PROJECT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}
    )
    target_link_libraries(${target} PRIVATE
        chronon::core
        chronon::observe
        STDEXEC::stdexec
        yaml-cpp::yaml-cpp
        chronon_compile_options
        Threads::Threads
    )
    target_compile_features(${target} PRIVATE cxx_std_20)
endfunction()

add_chronon_bench(bench_synthetic_framework
    bench_synthetic_framework.cpp
)

add_chronon_bench(bench_boom
    bench_boom.cpp
)

add_chronon_bench(synthetic_workload_yaml_example
    synthetic_workload_yaml_example.cpp
)

add_chronon_bench(bench_synthetic_determinism
    test_synthetic_determinism.cpp
)

message(STATUS "Chronon Benchmarks:")
message(STATUS "  - bench_synthetic_framework: tunable synthetic framework benchmark")
message(STATUS "  - bench_boom: BOOMv3-flavored topology benchmark")
message(STATUS "  - synthetic_workload_yaml_example: YAML-driven synthetic workload runner")
