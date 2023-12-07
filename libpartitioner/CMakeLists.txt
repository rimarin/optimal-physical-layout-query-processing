set(ARROW_BUILD_SHARED True)
set(ARROW_DEPENDENCY_SOURCE "SYSTEM")

# From arrow_io: "This benchmark either requires SSE4.2 or ARMV8 SIMD to be enabled"
# Setting anything different from "SSE4_2" or "ARMV8" leads to the CMake Error:
# "By not providing "Findxsimd.cmake" in CMAKE_MODULE_PATH this project has
#  asked CMake to find a package configuration file provided by "xsimd", but
#  CMake did not find one."
set(ARROW_SIMD_LEVEL SSE4_2)

# Turn on Arrow features here (and add them to target_link_libraries when appropriate)
set(ARROW_PARQUET ON)
set(ARROW_IPC ON)
set(ARROW_DATASET ON)
set(ARROW_FILESYSTEM ON)
set(ARROW_COMPUTE ON)


# Get Arrow
include(FetchContent)

FetchContent_Declare(Arrow
        GIT_REPOSITORY https://github.com/apache/arrow.git
        GIT_TAG        main
        GIT_SHALLOW    TRUE SOURCE_SUBDIR cpp
        OVERRIDE_FIND_PACKAGE
)

FetchContent_MakeAvailable(Arrow)

# HACK: Stolen from
# https://github.com/rapidsai/cudf/blob/branch-23.08/cpp/cmake/thirdparty/get_arrow.cmake#LL236C1-L238C8
file(INSTALL "${arrow_BINARY_DIR}/src/arrow/util/config.h"
        DESTINATION "${arrow_SOURCE_DIR}/cpp/src/arrow/util")
file(INSTALL "${arrow_BINARY_DIR}/src/parquet/parquet_version.h"
        DESTINATION "${arrow_SOURCE_DIR}/cpp/src/parquet")

# NOTE: I had to add this to make headers available which I find weird
target_include_directories(
        arrow_shared
        INTERFACE "$<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>"
)

include_directories(common/)
include_directories(include/)
include_directories(partitioning/)
include_directories(storage/)

set(LIBPARTITIONER_SOURCES
        common/KDTree.cpp
        common/QuadTree.cpp
        storage/DataWriter.cpp
        storage/DataReader.cpp
        partitioning/NoPartitioning.cpp
        partitioning/FixedGridPartitioning.cpp
        partitioning/GridFilePartitioning.cpp
        partitioning/HilbertCurvePartitioning.cpp
        partitioning/KDTreePartitioning.cpp
        partitioning/QuadTreePartitioning.cpp
        partitioning/STRTreePartitioning.cpp
        partitioning/ZOrderCurvePartitioning.cpp)

# Declare the library
add_library(libpartitioner STATIC ${LIBPARTITIONER_SOURCES})

target_link_libraries(libpartitioner
        PRIVATE
        arrow_shared
        arrow_dataset_shared
        arrow_acero_shared
        parquet_shared)

# Specify here the include directories exported by this library
target_include_directories(libpartitioner PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} include)

# Fix from: https://github.com/apache/arrow/issues/28378#issuecomment-1378020927
if(ARROW_USE_XSIMD)
    set(xsimd_SOURCE "BUNDLED")
    resolve_dependency(xsimd)

    add_library(xsimd INTERFACE IMPORTED)
    if(CMAKE_VERSION VERSION_LESS 3.11)
        set_target_properties(xsimd PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                "${XSIMD_INCLUDE_DIR}")
    else()
        target_include_directories(xsimd INTERFACE "${XSIMD_INCLUDE_DIR}")
    endif()
endif()