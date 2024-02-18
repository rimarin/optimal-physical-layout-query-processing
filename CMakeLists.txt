cmake_minimum_required(VERSION 3.5)

project(partitioner)

enable_testing()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_BUILD_TYPE Debug)

# set(DUCKDB_DIR "/data/ssd/riccardo.marin/libduckdb-linux-amd64")
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

add_subdirectory(libpartitioner)
add_subdirectory(partitioner)
add_subdirectory(test)