cmake_minimum_required(VERSION 3.26)

project(g_test_run)

add_subdirectory(_lib)
include_directories(../src/)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Parquet REQUIRED)

set(CMAKE_CXX_STANDARD 17)

set(SOURCES
        main.cpp
        ../src/common/DataLayoutConverter.cpp
        ../src/common/KDTree.cpp
        ../src/common/QuadTree.cpp
        ../src/storage/DataWriter.cpp
        ../src/storage/DataReader.cpp
        ../src/partitioning/NoPartitioning.cpp
        ../src/partitioning/FixedGridPartitioning.cpp
        ../src/partitioning/KDTreePartitioning.cpp
        ../src/partitioning/QuadTreePartitioning.cpp)
add_executable(g_test_run ${SOURCES})

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

target_link_libraries(g_test_run PRIVATE Arrow::arrow_shared parquet arrow_dataset gtest gtest_main)
target_include_directories(g_test_run PRIVATE src/include)

