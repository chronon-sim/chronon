# SPDX-License-Identifier: MPL-2.0
function(run)
    execute_process(COMMAND ${ARGV} RESULT_VARIABLE result)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "CPM consumer command failed (${result}): ${ARGV}")
    endif()
endfunction()
file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}/archive/chronon" "${WORK_DIR}/cache/cpm")
# Snapshot the current sources, including uncommitted fixes, without depending
# on a Git checkout or archiving unrelated build/output directories.
file(COPY "${CHRONON_SOURCE}/CMakeLists.txt" "${CHRONON_SOURCE}/src"
    "${CHRONON_SOURCE}/cmake" DESTINATION "${WORK_DIR}/archive/chronon")
execute_process(COMMAND "${CMAKE_COMMAND}" -E tar cf "${WORK_DIR}/chronon.tar" --format=gnutar chronon
    WORKING_DIRECTORY "${WORK_DIR}/archive" COMMAND_ERROR_IS_FATAL ANY)
file(SHA256 "${WORK_DIR}/chronon.tar" archive_hash)
# stdexec downloads two bootstrap files independently of CPM. Reuse the already
# configured dependency inputs so consumer verification does not need network.
file(GLOB cpm_files "${CHRONON_BUILD_DIR}/cmake/CPM*.cmake")
get_filename_component(cpm_dir "${CPM_MODULE}" DIRECTORY)
file(GLOB cached_cpm_files "${cpm_dir}/CPM*.cmake")
list(APPEND cpm_files ${cached_cpm_files})
file(COPY ${cpm_files} DESTINATION "${WORK_DIR}/cache/cpm")
foreach(mode direct cached_nested override)
    set(build "${WORK_DIR}/${mode}")
    file(MAKE_DIRECTORY "${build}/_deps/stdexec-build" "${build}/cmake")
    file(COPY "${STDEXEC_BUILD}/RAPIDS.cmake" "${STDEXEC_BUILD}/execution.bs"
        DESTINATION "${build}/_deps/stdexec-build")
    file(COPY ${cpm_files} DESTINATION "${build}/cmake")
    set(options)
    if(mode STREQUAL "cached_nested")
        # A second, independent build must find Chronon in the shared cache even
        # after the original archive becomes unavailable.
        file(RENAME "${WORK_DIR}/chronon.tar" "${WORK_DIR}/chronon.tar.hidden")
        list(APPEND options -DNESTED=ON -DPRELOAD_DEPENDENCIES=ON)
    elseif(mode STREQUAL "override")
        list(APPEND options "-DCPM_chronon_SOURCE=${CHRONON_SOURCE}" -DPRELOAD_DEPENDENCIES=ON)
    endif()
    run("${CMAKE_COMMAND}" -S "${CONSUMER_SOURCE}" -B "${build}"
        -C "${DEPENDENCY_CACHE}" "-DCMAKE_CXX_COMPILER=${CXX}" -DCMAKE_BUILD_TYPE=Release
        "-DCPM_MODULE=${CPM_MODULE}" "-DCPM_SOURCE_CACHE=${WORK_DIR}/cache"
        "-DCHRONON_ARCHIVE=${WORK_DIR}/chronon.tar" "-DCHRONON_ARCHIVE_HASH=${archive_hash}"
        ${options})
    run("${CMAKE_COMMAND}" --build "${build}" --target package_consumer --config Release -j2)
    run("${CMAKE_CTEST_COMMAND}" --test-dir "${build}" -C Release --output-on-failure)
endforeach()
