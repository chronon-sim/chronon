# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

add_executable(test_clock_trace test_clock_trace.cpp)
target_link_libraries(test_clock_trace PRIVATE chronon::observe)
target_compile_options(test_clock_trace PRIVATE -UNDEBUG)
add_test(NAME clock_trace_test COMMAND test_clock_trace)

add_executable(test_timeline_registry_lifecycle test_timeline_registry_lifecycle.cpp)
target_link_libraries(test_timeline_registry_lifecycle
    PRIVATE chronon::observe chronon_compile_options)
foreach(mode disabled enabled late_enable identity caches concurrent pending_lifetime)
    add_test(NAME timeline_registry_${mode}_test COMMAND test_timeline_registry_lifecycle ${mode})
    set_tests_properties(timeline_registry_${mode}_test PROPERTIES TIMEOUT 45)
endforeach()
add_executable(test_timeline_registry_backend test_timeline_registry_backend.cpp)
target_link_libraries(test_timeline_registry_backend
    PRIVATE chronon::observe chronon_compile_options ZLIB::ZLIB)
add_test(NAME timeline_registry_backend_test COMMAND test_timeline_registry_backend)
set_tests_properties(timeline_registry_backend_test PROPERTIES TIMEOUT 45)
add_dependencies(regress test_timeline_registry_lifecycle test_timeline_registry_backend)

add_executable(test_timeline_shared_context test_timeline_shared_context.cpp)
target_link_libraries(test_timeline_shared_context
    PRIVATE chronon::observe chronon_compile_options ZLIB::ZLIB)
foreach(mode enabled late_enable)
    add_test(NAME timeline_shared_context_${mode}_test COMMAND test_timeline_shared_context ${mode})
    set_tests_properties(timeline_shared_context_${mode}_test PROPERTIES TIMEOUT 45)
endforeach()
add_dependencies(regress test_timeline_shared_context)

# Observe module tests
#
# Tests for high-performance observability system

if(UNIX)
    add_executable(test_trace_output_failure test_trace_output_failure.cpp)
    target_link_libraries(test_trace_output_failure
        PRIVATE chronon::core chronon_compile_options ZLIB::ZLIB)
    add_dependencies(regress test_trace_output_failure)
    foreach(mode raw compressed immediate async final_flush manager_stop manager_shutdown app app_crash)
        add_test(NAME trace_output_failure_${mode}_test COMMAND test_trace_output_failure ${mode})
        set_tests_properties(trace_output_failure_${mode}_test PROPERTIES TIMEOUT 45)
    endforeach()
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        add_test(NAME trace_output_failure_full_device_test COMMAND test_trace_output_failure full_device)
        set_tests_properties(trace_output_failure_full_device_test PROPERTIES TIMEOUT 45)
    endif()
endif()

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

add_executable(test_thread_context_lifecycle test_thread_context_lifecycle.cpp)
target_link_libraries(test_thread_context_lifecycle PRIVATE chronon::observe chronon_compile_options)
add_dependencies(regress test_thread_context_lifecycle)
foreach(mode sequential concurrent_limit late_tls unread consumer wakeup flush_exit)
    add_test(NAME thread_context_${mode}_test COMMAND test_thread_context_lifecycle ${mode})
    set_tests_properties(thread_context_${mode}_test PROPERTIES TIMEOUT 45)
endforeach()

add_executable(test_thread_context_backend test_thread_context_backend.cpp)
target_link_libraries(test_thread_context_backend PRIVATE chronon::observe chronon_compile_options)
add_dependencies(regress test_thread_context_backend)
foreach(mode backend reordered_backend shutdown)
    add_test(NAME thread_context_${mode}_test COMMAND test_thread_context_backend ${mode})
    set_tests_properties(thread_context_${mode}_test PROPERTIES TIMEOUT 45)
endforeach()

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

if(CHRONON_ENABLE_OUTPORT_CANCELLATION)
    add_executable(test_counter_units
        test_counter_units.cpp
    )

    target_include_directories(test_counter_units PRIVATE
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_SOURCE_DIR}/examples
    )

    target_link_libraries(test_counter_units
        chronon::core
        chronon_observe
        chronon_compile_options
    )

    target_compile_features(test_counter_units PRIVATE cxx_std_20)
    add_dependencies(regress test_counter_units)

    add_test(
        NAME counter_units_test
        COMMAND test_counter_units
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
endif()

add_executable(test_observation_hardening
    test_observation_hardening.cpp
)

target_include_directories(test_observation_hardening PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_observation_hardening
    chronon::core
    chronon_observe
    chronon_compile_options
    ZLIB::ZLIB
)

target_compile_features(test_observation_hardening PRIVATE cxx_std_20)
add_dependencies(regress test_observation_hardening)

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
    test_timeline_producer_gate.cpp
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
