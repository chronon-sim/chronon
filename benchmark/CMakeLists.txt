# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# SPDX-License-Identifier: MPL-2.0

add_executable(chronon_scheduler_invocation_benchmark scheduler_invocation_benchmark.cpp)
target_link_libraries(chronon_scheduler_invocation_benchmark PRIVATE chronon::core chronon_compile_options)
add_executable(chronon_placement_scoring_benchmark placement_scoring_benchmark.cpp)
target_link_libraries(chronon_placement_scoring_benchmark PRIVATE chronon::core chronon_compile_options)
# Count allocations in separate executables: a shared atomic counter changes
# allocator contention and must not be enabled in throughput measurements.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SIZEOF_VOID_P EQUAL 8
   AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND NOT BUILD_SHARED_LIBS)
    foreach(kind scheduler_invocation placement_scoring)
        add_executable(chronon_${kind}_allocations ${kind}_benchmark.cpp)
        target_link_libraries(chronon_${kind}_allocations PRIVATE chronon::core chronon_compile_options)
        target_compile_definitions(chronon_${kind}_allocations PRIVATE
            CHRONON_BENCH_SCRATCH=1 CHRONON_COUNT_CLOCK_ALLOCATIONS=1)
        target_link_options(chronon_${kind}_allocations PRIVATE "-Wl,--wrap=_Znwm" "-Wl,--wrap=_Znam")
    endforeach()
endif()

add_executable(chronon_multiclock_benchmark multiclock_benchmark.cpp)
target_link_libraries(chronon_multiclock_benchmark PRIVATE chronon::core chronon_compile_options)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SIZEOF_VOID_P EQUAL 8
   AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang" AND NOT BUILD_SHARED_LIBS)
    target_compile_definitions(chronon_multiclock_benchmark PRIVATE CHRONON_COUNT_CLOCK_ALLOCATIONS=1)
    target_link_options(chronon_multiclock_benchmark PRIVATE "-Wl,--wrap=_Znwm" "-Wl,--wrap=_Znam")
endif()
add_executable(chronon_single_clock_regression_benchmark single_clock_regression.cpp)
target_link_libraries(chronon_single_clock_regression_benchmark PRIVATE chronon::core chronon_compile_options)
add_executable(chronon_trace_output_benchmark trace_output_benchmark.cpp)
target_link_libraries(chronon_trace_output_benchmark PRIVATE chronon::observe chronon_compile_options)

add_executable(chronon_counter_periodic_benchmark
    counter_periodic_benchmark.cpp
)

target_include_directories(chronon_counter_periodic_benchmark PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(chronon_counter_periodic_benchmark PRIVATE
    chronon::core
    chronon_observe
    chronon_compile_options
)

target_compile_features(chronon_counter_periodic_benchmark PRIVATE cxx_std_20)

add_executable(chronon_weighted_dependency_reduction_benchmark
    weighted_dependency_reduction_benchmark.cpp
)

target_include_directories(chronon_weighted_dependency_reduction_benchmark PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(chronon_weighted_dependency_reduction_benchmark PRIVATE
    chronon_compile_options
)

target_compile_features(chronon_weighted_dependency_reduction_benchmark PRIVATE cxx_std_20)

add_executable(chronon_direct_spsc_benchmark
    direct_spsc_benchmark.cpp
)

target_include_directories(chronon_direct_spsc_benchmark PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(chronon_direct_spsc_benchmark PRIVATE
    chronon_compile_options
    Threads::Threads
)

target_compile_features(chronon_direct_spsc_benchmark PRIVATE cxx_std_20)

add_executable(chronon_typed_mpsc_dispatch_benchmark
    typed_mpsc_dispatch_benchmark.cpp
)

target_include_directories(chronon_typed_mpsc_dispatch_benchmark PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(chronon_typed_mpsc_dispatch_benchmark PRIVATE
    chronon::core
    chronon_compile_options
)

target_compile_features(chronon_typed_mpsc_dispatch_benchmark PRIVATE cxx_std_20)

add_executable(chronon_mpsc_lane_frontier_benchmark
    mpsc_lane_frontier_benchmark.cpp
)

target_include_directories(chronon_mpsc_lane_frontier_benchmark PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(chronon_mpsc_lane_frontier_benchmark PRIVATE
    chronon_compile_options
)

target_compile_features(chronon_mpsc_lane_frontier_benchmark PRIVATE cxx_std_20)

add_executable(chronon_transparent_broadcast_fusion_benchmark
    transparent_broadcast_fusion_benchmark.cpp
)

target_include_directories(chronon_transparent_broadcast_fusion_benchmark PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(chronon_transparent_broadcast_fusion_benchmark PRIVATE
    chronon::core
    chronon_compile_options
)

target_compile_features(chronon_transparent_broadcast_fusion_benchmark PRIVATE cxx_std_20)

add_executable(chronon_representative_workload_benchmark
    representative_workload_benchmark.cpp
)

target_include_directories(chronon_representative_workload_benchmark PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(chronon_representative_workload_benchmark PRIVATE
    chronon::core
    chronon_compile_options
)

target_compile_features(chronon_representative_workload_benchmark PRIVATE cxx_std_20)

if(CHRONON_BUILD_TESTS)
    add_test(NAME benchmark_multiclock_scaling_smoke
        COMMAND chronon_multiclock_benchmark scaling 2000 2 4 4 8 1 4)
    set_tests_properties(benchmark_multiclock_scaling_smoke PROPERTIES PROCESSORS 2 TIMEOUT 30)
    add_test(NAME benchmark_multiclock_shared_segmented
        COMMAND chronon_multiclock_benchmark scaling 2000 4 4 4 0 1 1
            --lanes 8 --segment 137 --clocks coincident --profile 1)
    add_test(NAME benchmark_multiclock_feedback
        COMMAND chronon_multiclock_benchmark scaling 2000 4 8 8 8 0 1
            --topology ring --activity 4 --clocks coprime)
    set_tests_properties(benchmark_multiclock_shared_segmented benchmark_multiclock_feedback
        PROPERTIES PROCESSORS 4 TIMEOUT 30)
    add_test(NAME benchmark_direct_spsc_smoke
        COMMAND chronon_direct_spsc_benchmark 10000 1)
    set_tests_properties(benchmark_direct_spsc_smoke PROPERTIES RUN_SERIAL TRUE)

    add_executable(chronon_representative_workload_test
        representative_workload_test.cpp
    )

    target_include_directories(chronon_representative_workload_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
    )

    target_link_libraries(chronon_representative_workload_test PRIVATE
        chronon_compile_options
    )

    target_compile_features(chronon_representative_workload_test PRIVATE cxx_std_20)

    add_test(NAME benchmark_representative_workload_generator
        COMMAND chronon_representative_workload_test)
    add_test(NAME benchmark_representative_workload_end_to_end
        COMMAND chronon_representative_workload_benchmark
            --profile nucleus --seed 12345 --units 8 --threads 1,2
            --warmup 32 --cycles 128 --repetitions 1)
    add_test(NAME benchmark_representative_workload_random_sweep
        COMMAND chronon_representative_workload_benchmark
            --profile random --seed 20260719 --scenario-count 2
            --units 12 --channels 2 --max-fanout 4 --threads 1,2
            --warmup 16 --cycles 64 --repetitions 1)
    add_test(NAME benchmark_representative_scheduler_floor
        COMMAND chronon_representative_workload_benchmark
            --profile scheduler-floor --seed 24 --units 8 --threads 1,2
            --warmup 16 --cycles 64 --repetitions 1)
    add_test(NAME benchmark_representative_memory_footprint
        COMMAND chronon_representative_workload_benchmark
            --profile memory --seed 42 --units 16 --working-set 65536
            --memory-pattern-weights 1,1,1,1 --threads 1,2
            --warmup 0 --cycles 32 --repetitions 1)
endif()
