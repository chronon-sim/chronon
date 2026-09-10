# SPDX-License-Identifier: MPL-2.0
cmake_minimum_required(VERSION 3.25)
project(chronon_installed_consumer LANGUAGES CXX)
find_package(chronon 0.4 CONFIG REQUIRED)
add_executable(installed_consumer main.cpp)
target_link_libraries(installed_consumer PRIVATE chronon::core)
enable_testing()
add_test(NAME consumer_run COMMAND installed_consumer)
