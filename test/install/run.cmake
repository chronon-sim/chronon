# SPDX-License-Identifier: MPL-2.0
# Install, relocate, then build a separate project with no source/build includes.
function(run)
    execute_process(COMMAND ${ARGV} RESULT_VARIABLE result)
    if(NOT result EQUAL 0)
        message(FATAL_ERROR "Installed consumer command failed (${result}): ${ARGV}")
    endif()
endfunction()
file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}")
run("${CMAKE_COMMAND}" --install "${CHRONON_BUILD_DIR}"
    --prefix "${WORK_DIR}/staging" --config "${CONFIG}")
file(RENAME "${WORK_DIR}/staging" "${WORK_DIR}/relocated")
run("${CMAKE_COMMAND}" -S "${CONSUMER_SOURCE}" -B "${WORK_DIR}/consumer"
    "-DCMAKE_PREFIX_PATH=${WORK_DIR}/relocated" "-DCMAKE_CXX_COMPILER=${CXX}"
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF)
run("${CMAKE_COMMAND}" --build "${WORK_DIR}/consumer" --config "${CONFIG}" -j2)
run("${CMAKE_CTEST_COMMAND}" --test-dir "${WORK_DIR}/consumer" -C "${CONFIG}"
    --output-on-failure)
