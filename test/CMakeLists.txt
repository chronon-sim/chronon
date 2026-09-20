# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

project(chronon_tests)

# Create regress target if it doesn't exist
if(NOT TARGET regress)
    add_custom_target(regress COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure)
endif()

include(public_headers.cmake)

# ==============================================================================
# TreeNode Unit Tests (Core Infrastructure - still used)
# ==============================================================================

add_executable(test_tree_node
    test_tree_node.cpp
)

target_include_directories(test_tree_node PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_tree_node
    chronon::core
    chronon_compile_options
)

target_compile_features(test_tree_node PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_tree_node)

# Add test
add_test(
    NAME tree_node_test
    COMMAND test_tree_node
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Parameter System Unit Tests
# ==============================================================================

add_executable(test_params
    test_params.cpp
)

target_include_directories(test_params PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_params
    chronon::core
    chronon_compile_options
)

target_compile_features(test_params PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_params)

# Add test
add_test(
    NAME params_test
    COMMAND test_params
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# YAML Serialization Tests
# ==============================================================================

add_executable(test_yaml_serialization
    test_yaml_serialization.cpp
)

target_include_directories(test_yaml_serialization PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(test_yaml_serialization
    chronon::core
    yaml-cpp::yaml-cpp
    chronon_compile_options
)

target_compile_features(test_yaml_serialization PRIVATE cxx_std_20)

# Add to regress target
add_dependencies(regress test_yaml_serialization)

# Add test
add_test(
    NAME yaml_serialization_test
    COMMAND test_yaml_serialization
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

# ==============================================================================
# Sender framework tests (primary tests for new framework)
# ==============================================================================

add_subdirectory(sender)

# ==============================================================================
# Observe module tests (observability API)
# ==============================================================================

add_subdirectory(observe)

if(CHRONON_INSTALL)
    add_test(NAME installed_consumer
        COMMAND ${CMAKE_COMMAND}
            -DCHRONON_BUILD_DIR=${CMAKE_BINARY_DIR}
            -DINSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}
            -DCONSUMER_SOURCE=${CMAKE_CURRENT_SOURCE_DIR}/install
            -DWORK_DIR=${PROJECT_BINARY_DIR}/out/install-consumer
            -DCXX=${CMAKE_CXX_COMPILER}
            -DCONFIG=$<CONFIG>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/install/run.cmake)
    set_tests_properties(installed_consumer PROPERTIES TIMEOUT 180 RUN_SERIAL TRUE)

    include(FetchContent)
    set(package_dependency_cache "${CMAKE_CURRENT_BINARY_DIR}/package-dependencies.cmake")
    file(WRITE "${package_dependency_cache}" "# Local dependency sources for package tests.\n")
    foreach(dependency stdexec perfetto icm rapids-cmake)
        FetchContent_GetProperties(${dependency} SOURCE_DIR dependency_source)
        string(TOUPPER "${dependency}" dependency_upper)
        file(APPEND "${package_dependency_cache}"
            "set(FETCHCONTENT_SOURCE_DIR_${dependency_upper} \"${dependency_source}\" CACHE PATH \"\" FORCE)\n")
    endforeach()
    add_test(NAME cpm_consumers
        COMMAND ${CMAKE_COMMAND}
            -DCHRONON_SOURCE=${chronon_SOURCE_DIR}
            -DCHRONON_BUILD_DIR=${CMAKE_BINARY_DIR}
            -DCONSUMER_SOURCE=${CMAKE_CURRENT_SOURCE_DIR}/package
            -DWORK_DIR=${PROJECT_BINARY_DIR}/out/cpm-consumers
            -DCPM_MODULE=${CPM_DOWNLOAD_LOCATION}
            -DSTDEXEC_BUILD=${stdexec_BINARY_DIR}
            -DDEPENDENCY_CACHE=${package_dependency_cache}
            -DCXX=${CMAKE_CXX_COMPILER}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/package/run.cmake)
    set_tests_properties(cpm_consumers PROPERTIES TIMEOUT 1200 RUN_SERIAL TRUE)
endif()
