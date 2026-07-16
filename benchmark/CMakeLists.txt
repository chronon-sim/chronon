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
)

target_compile_features(chronon_direct_spsc_benchmark PRIVATE cxx_std_20)
