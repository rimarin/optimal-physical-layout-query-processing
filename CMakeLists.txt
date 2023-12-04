cmake_minimum_required(VERSION 3.26)

project(oplqp)
add_subdirectory(src)
add_subdirectory(test)
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Parquet REQUIRED)
find_package(Protobuf REQUIRED)

include(arrow)

if(NOT Arrow_FOUND)
    build_arrow()
endif()

set(CMAKE_CXX_STANDARD 17)

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

target_link_libraries(oplqp PRIVATE Arrow::arrow_static parquet_static arrow_dataset_static)
target_include_directories(oplqp PRIVATE src/include)

set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "partitioner")
