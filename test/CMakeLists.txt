cmake_minimum_required(VERSION 3.26)

project(oplqp_test)

add_subdirectory(_lib)
include_directories(../src/)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Parquet REQUIRED)

set(CMAKE_CXX_STANDARD 17)

set(SOURCES
        fixture.cpp
        testFixedGrid.cpp
        testKDTree.cpp
        testQuadTree.cpp
        testReadWrite.cpp
        ../src/common/DataLayoutConverter.cpp
        ../src/common/KDTree.cpp
        ../src/common/QuadTree.cpp
        ../src/storage/DataWriter.cpp
        ../src/storage/DataReader.cpp
        ../src/partitioning/NoPartitioning.cpp
        ../src/partitioning/FixedGridPartitioning.cpp
        ../src/partitioning/GridFilePartitioning.cpp
        ../src/partitioning/KDTreePartitioning.cpp
        ../src/partitioning/QuadTreePartitioning.cpp)
add_executable(oplqp_test ${SOURCES})

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

target_link_libraries(oplqp_test PRIVATE Arrow::arrow_shared parquet arrow_dataset gtest gtest_main)
target_include_directories(oplqp_test PRIVATE src/include)

