# SPDX-License-Identifier: MPL-2.0
# Compile each documented entry point without relying on another header's includes.
set(chronon_header_sources)
foreach(header Simulation Observation Application Chronon)
    set(source "${CMAKE_CURRENT_BINARY_DIR}/header_${header}.cpp")
    file(WRITE "${source}" "#include \"chronon/${header}.hpp\"\n")
    list(APPEND chronon_header_sources "${source}")
endforeach()
add_library(chronon_header_checks OBJECT ${chronon_header_sources})
target_link_libraries(chronon_header_checks PRIVATE chronon::core)
if(TARGET regress)
    add_dependencies(regress chronon_header_checks)
endif()
