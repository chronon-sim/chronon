# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Observe module tests
#
# Tests for high-performance observability system

# ==============================================================================
# SPSC Queue and Thread Context Tests
# ==============================================================================

add_executable(test_spsc_queue
    test_spsc_queue.cpp
)

target_include_directories(test_spsc_queue PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_spsc_queue
    chronon::core
    chronon_compile_options
)

target_compile_features(test_spsc_queue PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_spsc_queue)

# Add test
add_test(
    NAME spsc_queue_test
    COMMAND test_spsc_queue
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

if(NOT CHRONON_ENABLE_COUNTER_UPDATES)
    add_executable(test_counter_updates_disabled
        test_counter_updates_disabled.cpp
    )
    target_include_directories(test_counter_updates_disabled PRIVATE
        ${PROJECT_SOURCE_DIR}/src
    )
    target_link_libraries(test_counter_updates_disabled
        chronon::core
        chronon_observe
        chronon_compile_options
    )
    target_compile_features(test_counter_updates_disabled PRIVATE cxx_std_20)
    add_dependencies(regress test_counter_updates_disabled)
    add_test(
        NAME counter_updates_disabled_test
        COMMAND test_counter_updates_disabled
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
endif()

# ==============================================================================
# Sparse Counter Architecture Test
# ==============================================================================

add_executable(test_sparse_counter
    test_sparse_counter.cpp
)

target_include_directories(test_sparse_counter PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_sparse_counter
    chronon::core
    chronon_observe
    chronon_compile_options
)

target_compile_features(test_sparse_counter PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_sparse_counter)

# Add test
add_test(
    NAME sparse_counter_test
    COMMAND test_sparse_counter
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Counter Integration Test
# ==============================================================================

add_executable(test_counter_integration
    test_counter_integration.cpp
)

target_include_directories(test_counter_integration PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_counter_integration
    chronon::core
    chronon_observe
    chronon_compile_options
)

target_compile_features(test_counter_integration PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_counter_integration)

# Add test
add_test(
    NAME counter_integration_test
    COMMAND test_counter_integration
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Local Counter Test
# ==============================================================================

add_executable(test_local_counter
    test_local_counter.cpp
)

target_include_directories(test_local_counter PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_local_counter
    chronon::core
    chronon_observe
    chronon_compile_options
)

target_compile_features(test_local_counter PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_local_counter)

# Add test
add_test(
    NAME local_counter_test
    COMMAND test_local_counter
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Counter Units Test
# ==============================================================================

add_executable(test_counter_units
    test_counter_units.cpp
)

add_executable(test_observation_hardening
    test_observation_hardening.cpp
)

target_include_directories(test_counter_units PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/examples
)

target_include_directories(test_observation_hardening PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_counter_units
    chronon::core
    chronon_observe
    chronon_compile_options
)

target_link_libraries(test_observation_hardening
    chronon::core
    chronon_observe
    chronon_compile_options
    ZLIB::ZLIB
)

target_compile_features(test_counter_units PRIVATE cxx_std_20)
target_compile_features(test_observation_hardening PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_counter_units)
add_dependencies(regress test_observation_hardening)

# Add test
add_test(
    NAME counter_units_test
    COMMAND test_counter_units
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

add_test(
    NAME observation_hardening_test
    COMMAND test_observation_hardening
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Perfetto Trace Writer Test
# ==============================================================================

add_executable(test_perfetto_trace_writer
    test_perfetto_trace_writer.cpp
)

target_include_directories(test_perfetto_trace_writer PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_perfetto_trace_writer
    chronon_observe
    chronon_compile_options
    ZLIB::ZLIB  # independent decoder inflates compressed_packets batches
)

target_compile_features(test_perfetto_trace_writer PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_perfetto_trace_writer)

# Add test
add_test(
    NAME perfetto_trace_writer_test
    COMMAND test_perfetto_trace_writer
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Timeline API (lanes / spans / flows / typed args) Test
# ==============================================================================

add_executable(test_timeline_api
    test_timeline_api.cpp
)

target_include_directories(test_timeline_api PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_timeline_api
    chronon::core
    chronon_observe
    chronon_compile_options
    ZLIB::ZLIB  # independent decoder inflates compressed_packets batches
)

target_compile_features(test_timeline_api PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_timeline_api)

# Add test
add_test(
    NAME timeline_api_test
    COMMAND test_timeline_api
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Timeline string annotation args
# ==============================================================================

add_executable(test_timeline_string_args
    test_timeline_string_args.cpp
)

target_include_directories(test_timeline_string_args PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_timeline_string_args
    chronon::core
    chronon_observe
    chronon_compile_options
    ZLIB::ZLIB
)

target_compile_features(test_timeline_string_args PRIVATE cxx_std_20)

add_dependencies(regress test_timeline_string_args)

add_test(
    NAME timeline_string_args_test
    COMMAND test_timeline_string_args
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Runtime Pipeline API Test
# ==============================================================================

add_executable(test_runtime_pipeline_api
    test_runtime_pipeline_api.cpp
)

target_include_directories(test_runtime_pipeline_api PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_runtime_pipeline_api
    chronon::core
    chronon_observe
    chronon_compile_options
    ZLIB::ZLIB
)

target_compile_features(test_runtime_pipeline_api PRIVATE cxx_std_20)

add_dependencies(regress test_runtime_pipeline_api)

add_test(
    NAME runtime_pipeline_api_test
    COMMAND test_runtime_pipeline_api
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
