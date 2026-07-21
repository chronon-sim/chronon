# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# SPDX-License-Identifier: MPL-2.0

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
