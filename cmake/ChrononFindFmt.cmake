# SPDX-License-Identifier: MPL-2.0
# Shared by source builds and the installed package. A fmt CMake package is
# preferred, but system headers and a compiled library are sufficient.
set(ChrononFmt_FOUND TRUE)
if(NOT TARGET fmt::fmt)
    find_package(fmt QUIET)
    if(NOT TARGET fmt::fmt)
        find_path(ChrononFmt_INCLUDE_DIR NAMES fmt/format.h)
        find_library(ChrononFmt_LIBRARY NAMES fmt)
        mark_as_advanced(ChrononFmt_INCLUDE_DIR ChrononFmt_LIBRARY)
        if(ChrononFmt_INCLUDE_DIR AND ChrononFmt_LIBRARY)
            add_library(fmt::fmt UNKNOWN IMPORTED)
            set_target_properties(fmt::fmt PROPERTIES
                IMPORTED_LOCATION "${ChrononFmt_LIBRARY}"
                INTERFACE_INCLUDE_DIRECTORIES "${ChrononFmt_INCLUDE_DIR}")
        else()
            set(ChrononFmt_FOUND FALSE)
            set(ChrononFmt_NOT_FOUND_MESSAGE
                "Chronon requires fmt: provide a fmt CMake package or fmt/format.h and libfmt.")
        endif()
    endif()
endif()
