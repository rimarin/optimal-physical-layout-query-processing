set(ARROW_DEPENDENCY_SOURCE "SYSTEM")

# From arrow_io: "This benchmark either requires SSE4.2 or ARMV8 SIMD to be enabled"
# Setting anything different from "SSE4_2" or "ARMV8" leads to the CMake Error:
# "By not providing "Findxsimd.cmake" in CMAKE_MODULE_PATH this project has
#  asked CMake to find a package configuration file provided by "xsimd", but
#  CMake did not find one."
set(ARROW_SIMD_LEVEL SSE4_2)

# Turn on Arrow features here (and add them to target_link_libraries when appropriate)
set(ARROW_ACERO ON)
set(ARROW_PARQUET ON)
set(ARROW_IPC ON)
set(ARROW_DATASET ON)
set(ARROW_FILESYSTEM ON)
set(ARROW_COMPUTE ON)
set(ARROW_WITH_ZLIB ON)
set(ARROW_WITH_SNAPPY ON)
set(ARROW_WITH_GZIP ON)
set(ARROW_BUILD_STATIC ON)
set(ARROW_DEPENDENCY_SOURCE BUNDLED)
set(THREADS_PREFER_PTHREAD_FLAG ON)

# Get Arrow
include(FetchContent)

FetchContent_Declare(Arrow
        GIT_REPOSITORY https://github.com/apache/arrow.git
        GIT_TAG        apache-arrow-15.0.0
        GIT_SHALLOW    TRUE SOURCE_SUBDIR cpp
        OVERRIDE_FIND_PACKAGE
)

FetchContent_GetProperties(Arrow)
if(NOT arrow_POPULATED)
    FetchContent_Populate(Arrow)

    add_subdirectory(${arrow_SOURCE_DIR}/cpp ${arrow_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()
# FetchContent_MakeAvailable(Arrow)

# HACK: Stolen from
# https://github.com/rapidsai/cudf/blob/branch-23.08/cpp/cmake/thirdparty/get_arrow.cmake#LL236C1-L238C8
file(INSTALL "${arrow_BINARY_DIR}/src/arrow/util/config.h"
        DESTINATION "${arrow_SOURCE_DIR}/cpp/src/arrow/util")
file(INSTALL "${arrow_BINARY_DIR}/src/parquet/parquet_version.h"
        DESTINATION "${arrow_SOURCE_DIR}/cpp/src/parquet")

# NOTE: I had to add this to make headers available which I find weird
target_include_directories(
        arrow_static
        INTERFACE "$<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>"
)

set(DUCKDB_DIR "/home/vpandey-ldap/libduckdb-linux-amd64")

find_library(DUCKDB_LIB NAMES duckdb PATHS ${DUCKDB_DIR} NO_CMAKE_SYSTEM_PATH)

if(NOT DUCKDB_LIB)
    message(FATAL_ERROR "libduckdb not found. Please check the DUCKDB_DIR variable.")
else()
    message(STATUS "libduckdb found at ${DUCKDB_LIB}")
endif()

set(ENABLE_SANITIZER
        OFF
        CACHE INTERNAL "Disable ASAN")

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

include_directories(${DUCKDB_DIR}/include)

include_directories(common/)
include_directories(include/)
include_directories(external/)
include_directories(partitioning/)
include_directories(storage/)
include_directories(structures/)

set(LIBPARTITIONER_SOURCES
        partitioning/FixedGridPartitioning.cpp
        partitioning/GridFilePartitioning.cpp
        partitioning/HilbertCurvePartitioning.cpp
        partitioning/KDTreePartitioning.cpp
        partitioning/NoPartitioning.cpp
        partitioning/Partitioning.cpp
        partitioning/QuadTreePartitioning.cpp
        partitioning/STRTreePartitioning.cpp
        partitioning/ZOrderCurvePartitioning.cpp
        storage/DataWriter.cpp
        storage/DataReader.cpp
        storage/TableGenerator.cpp
        structures/KDTree.cpp
        structures/QuadTree.cpp)

# Declare the library
add_library(libpartitioner STATIC ${LIBPARTITIONER_SOURCES})

find_package(Threads REQUIRED)

target_link_libraries(libpartitioner
        PUBLIC
        arrow_static
        arrow_dataset_static
        arrow_acero_static
        parquet_static
        ${DUCKDB_LIB}
        Threads::Threads)

# Specify here the include directories exported by this library
target_include_directories(libpartitioner PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} include)

target_include_directories(libpartitioner PRIVATE ${DUCKDB_DIR} include)

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
