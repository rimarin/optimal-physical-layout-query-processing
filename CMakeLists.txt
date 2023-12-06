cmake_minimum_required(VERSION 3.25)

project(oplqp)

set(CMAKE_BUILD_TYPE Release)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(src)
add_subdirectory(test)

set(SOURCES
    experimentsConfig.cpp
    partitioner.cpp
    src/common/KDTree.cpp
    src/common/QuadTree.cpp
    src/storage/DataWriter.cpp
    src/storage/DataReader.cpp
    src/partitioning/NoPartitioning.cpp
    src/partitioning/FixedGridPartitioning.cpp
    src/partitioning/GridFilePartitioning.cpp
    src/partitioning/HilbertCurvePartitioning.cpp
    src/partitioning/KDTreePartitioning.cpp
    src/partitioning/QuadTreePartitioning.cpp
    src/partitioning/STRTreePartitioning.cpp
    src/partitioning/ZOrderCurvePartitioning.cpp)
add_executable(oplqp ${SOURCES})

target_include_directories(oplqp PRIVATE src/include)

set(ARROW_BUILD_SHARED True)
set(ARROW_DEPENDENCY_SOURCE "SYSTEM")

# From arrow_io: "This benchmark either requires SSE4.2 or ARMV8 SIMD to be enabled"
# Setting anything different from "SSE4_2" or "ARMV8" leads to the CMake Error:
# "By not providing "Findxsimd.cmake" in CMAKE_MODULE_PATH this project has
#  asked CMake to find a package configuration file provided by "xsimd", but
#  CMake did not find one."
set(ARROW_SIMD_LEVEL SSE4_2)
# set(ARROW_SIMD_LEVEL NONE) # macOS-specific workaround

# Turn on Arrow features here (and add them to target_link_libraries when
# appropriate)
set(ARROW_ACERO ON)
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
#file(INSTALL "${arrow_BINARYs_DIR}/src/parquet/parquet_version.h"
#        DESTINATION "${arrow_SOURCE_DIR}/cpp/src/parquet")

# NOTE: I had to add this to make headers available which I find weird
target_include_directories(
        arrow_shared
        INTERFACE "$<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>"
)

target_link_libraries(oplqp
        PRIVATE
        arrow_shared
        arrow_dataset_shared
        arrow_acero_shared
        parquet_shared)

set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "partitioner")

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