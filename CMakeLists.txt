# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Chronon - High-Performance Event-Driven Simulation Framework
# A fast, multithreaded simulation framework for CPU microarchitecture modeling

cmake_minimum_required(VERSION 3.25)
project(chronon VERSION 0.3.0 LANGUAGES CXX)

# Auto-configure git hooks only when built as the top-level project
# (skip when fetched as a dependency via CPM/FetchContent).
if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    find_package(Git QUIET)
    if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
        execute_process(
            COMMAND ${GIT_EXECUTABLE} config --local core.hooksPath scripts/hooks
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
            RESULT_VARIABLE GIT_HOOKS_RESULT
            ERROR_QUIET
        )
        if(GIT_HOOKS_RESULT EQUAL 0)
            message(STATUS "Git hooks: using scripts/hooks/ (core.hooksPath)")
        endif()
    endif()
endif()

# C++20 required
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Download CPM.cmake
set(CPM_DOWNLOAD_VERSION 0.40.2)
if(CPM_SOURCE_CACHE)
    set(CPM_DOWNLOAD_LOCATION "${CPM_SOURCE_CACHE}/cpm/CPM_${CPM_DOWNLOAD_VERSION}.cmake")
elseif(DEFINED ENV{CPM_SOURCE_CACHE})
    set(CPM_DOWNLOAD_LOCATION "$ENV{CPM_SOURCE_CACHE}/cpm/CPM_${CPM_DOWNLOAD_VERSION}.cmake")
else()
    set(CPM_DOWNLOAD_LOCATION "${CMAKE_BINARY_DIR}/cmake/CPM_${CPM_DOWNLOAD_VERSION}.cmake")
endif()

function(chronon_download_cpm output_path)
    get_filename_component(cpm_download_dir "${output_path}" DIRECTORY)
    file(MAKE_DIRECTORY "${cpm_download_dir}")
    message(STATUS "Downloading CPM.cmake to ${output_path}")
    set(cpm_download_status "")
    file(DOWNLOAD
        https://github.com/cpm-cmake/CPM.cmake/releases/download/v${CPM_DOWNLOAD_VERSION}/CPM.cmake
        "${output_path}"
        STATUS cpm_download_status
        TLS_VERIFY ON
    )
    list(GET cpm_download_status 0 cpm_download_code)
    list(GET cpm_download_status 1 cpm_download_message)
    if(NOT cpm_download_code EQUAL 0)
        file(REMOVE "${output_path}")
        message(FATAL_ERROR "Failed to download CPM.cmake: ${cpm_download_message}")
    endif()
endfunction()

if(NOT (EXISTS ${CPM_DOWNLOAD_LOCATION}))
    chronon_download_cpm("${CPM_DOWNLOAD_LOCATION}")
endif()

file(READ "${CPM_DOWNLOAD_LOCATION}" CPM_DOWNLOAD_CONTENT LIMIT 65536)
if(NOT CPM_DOWNLOAD_CONTENT MATCHES "CPMAddPackage")
    message(STATUS "Cached CPM.cmake is incomplete; retrying download")
    file(REMOVE "${CPM_DOWNLOAD_LOCATION}")
    chronon_download_cpm("${CPM_DOWNLOAD_LOCATION}")
    file(READ "${CPM_DOWNLOAD_LOCATION}" CPM_DOWNLOAD_CONTENT LIMIT 65536)
endif()
if(NOT CPM_DOWNLOAD_CONTENT MATCHES "CPMAddPackage")
    message(FATAL_ERROR "CPM.cmake at ${CPM_DOWNLOAD_LOCATION} does not define CPMAddPackage")
endif()
unset(CPM_DOWNLOAD_CONTENT)

include("${CPM_DOWNLOAD_LOCATION}")
if(NOT COMMAND CPMAddPackage)
    message(FATAL_ERROR "CPM.cmake at ${CPM_DOWNLOAD_LOCATION} did not load CPMAddPackage")
endif()

# Options
option(CHRONON_BUILD_TESTS "Build Chronon tests" ON)
option(CHRONON_INSTALL "Generate install/export rules (requires exportable deps)" OFF)

# Centralized compile options (warnings, -Werror, sanitizers, clang-tidy)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/ChrononCompileOptions.cmake)

# Use ccache if available
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
    message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
endif()

# Dependencies
find_package(Threads REQUIRED)
find_package(yaml-cpp QUIET)

if(NOT yaml-cpp_FOUND)
    message(STATUS "yaml-cpp not found, some features may be disabled")
elseif(NOT TARGET yaml-cpp::yaml-cpp)
    if(TARGET yaml-cpp)
        add_library(yaml-cpp::yaml-cpp ALIAS yaml-cpp)
    elseif(YAML_CPP_LIBRARIES)
        add_library(yaml-cpp::yaml-cpp INTERFACE IMPORTED)
        target_link_libraries(yaml-cpp::yaml-cpp INTERFACE ${YAML_CPP_LIBRARIES})
        if(YAML_CPP_INCLUDE_DIR)
            target_include_directories(yaml-cpp::yaml-cpp INTERFACE ${YAML_CPP_INCLUDE_DIR})
        endif()
    endif()
endif()

# Add stdexec (C++ std::execution reference implementation)
CPMAddPackage(
    NAME stdexec
    GITHUB_REPOSITORY NVIDIA/stdexec
    GIT_TAG nvhpc-25.09
    GIT_SHALLOW ON
    OPTIONS
        "STDEXEC_BUILD_TESTS OFF"
        "STDEXEC_BUILD_EXAMPLES OFF"
        "BUILD_TESTING OFF"
)

# Mark stdexec includes as SYSTEM to isolate from -Werror
if(TARGET stdexec)
    set_target_properties(stdexec PROPERTIES SYSTEM TRUE)
endif()

# Perfetto SDK (amalgamated C++ sources) for timeline.pftrace output.
# The cpp-sdk-src release asset contains only perfetto.h + perfetto.cc.
CPMAddPackage(
    NAME perfetto
    URL https://github.com/google/perfetto/releases/download/v56.1/perfetto-cpp-sdk-src.zip
    DOWNLOAD_ONLY YES
)
if(perfetto_ADDED)
    add_library(perfetto_sdk STATIC ${perfetto_SOURCE_DIR}/perfetto.cc)
    target_include_directories(perfetto_sdk SYSTEM PUBLIC ${perfetto_SOURCE_DIR})
    target_compile_features(perfetto_sdk PUBLIC cxx_std_17)
    # Third-party code: keep it out of our warning/-Werror surface.
    target_compile_options(perfetto_sdk PRIVATE -w)
    # Sanitizer instrumentation must match the rest of the build: protozero's
    # arena poisoning is split between header-inlined code (compiled into
    # chronon TUs) and perfetto.cc — mixing instrumented and uninstrumented
    # TUs trips false use-after-poison reports under clang ASan.
    if(CHRONON_ENABLE_ASAN)
        target_compile_options(perfetto_sdk PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(perfetto_sdk PUBLIC -fsanitize=address,undefined)
    endif()
    if(CHRONON_ENABLE_TSAN)
        target_compile_options(perfetto_sdk PRIVATE
            -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(perfetto_sdk PUBLIC -fsanitize=thread)
    endif()
    find_package(Threads REQUIRED)
    target_link_libraries(perfetto_sdk PUBLIC Threads::Threads)
endif()

# zlib (system library) for TracePacket.compressed_packets deflate batching.
find_package(ZLIB REQUIRED)
if(APPLE AND TARGET ZLIB::ZLIB)
    # FindZLIB may expose the SDK's usr/include root as an imported target
    # include directory. If that SDK does not match the compiler's implicit SDK,
    # adding it explicitly breaks libc++ include_next resolution on macOS.
    set(_chronon_zlib_removed_sdk_include OFF)
    foreach(_chronon_zlib_include_prop
            INTERFACE_INCLUDE_DIRECTORIES
            INTERFACE_SYSTEM_INCLUDE_DIRECTORIES)
        get_target_property(_chronon_zlib_includes
            ZLIB::ZLIB
            ${_chronon_zlib_include_prop})
        if(_chronon_zlib_includes)
            set(_chronon_zlib_filtered_includes "${_chronon_zlib_includes}")
            list(FILTER _chronon_zlib_filtered_includes EXCLUDE REGEX
                ".*/[^/]+\\.sdk/usr/include/?$")
            if(NOT _chronon_zlib_filtered_includes STREQUAL _chronon_zlib_includes)
                set_target_properties(ZLIB::ZLIB PROPERTIES
                    ${_chronon_zlib_include_prop} "${_chronon_zlib_filtered_includes}")
                set(_chronon_zlib_removed_sdk_include ON)
            endif()
        endif()
    endforeach()
    if(_chronon_zlib_removed_sdk_include)
        message(STATUS "ZLIB: using compiler SDK search path instead of explicit SDK include root")
    endif()
endif()

# Add the main framework library
add_subdirectory(src)

# Tests and examples
if(CHRONON_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
    add_subdirectory(examples)
endif()

# Summary
message(STATUS "")
message(STATUS "Chronon Configuration Summary:")
message(STATUS "  Version: ${PROJECT_VERSION}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Standard: C++${CMAKE_CXX_STANDARD}")
message(STATUS "  Compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  Build Tests/Examples: ${CHRONON_BUILD_TESTS}")
message(STATUS "  Werror: ${CHRONON_ENABLE_WERROR}")
message(STATUS "  ASan+UBSan: ${CHRONON_ENABLE_ASAN}")
message(STATUS "  TSan: ${CHRONON_ENABLE_TSAN}")
message(STATUS "  Coverage: ${CHRONON_ENABLE_COVERAGE}")
message(STATUS "  clang-tidy: ${CHRONON_ENABLE_CLANG_TIDY}")
message(STATUS "")
