# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

project(chronon_tests)

# Create regress target if it doesn't exist
if(NOT TARGET regress)
    add_custom_target(regress COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure)
endif()

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
    yaml-cpp
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

