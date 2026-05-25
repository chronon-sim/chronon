# Copyright (c) 2026 EHTech (Beijing) Co., Ltd.
# Author: Haomeng Wang <chang_yun@outlook.com>
# SPDX-License-Identifier: MPL-2.0

# Tools CMakeLists.txt
#
# Command-line tools for Chronon.

# trace_reader - Binary trace file reader and converter
add_executable(trace_reader trace_reader.cpp)
target_link_libraries(trace_reader PRIVATE chronon_observe chronon_compile_options)
target_include_directories(trace_reader PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

# Install
install(TARGETS trace_reader
    RUNTIME DESTINATION bin
)
