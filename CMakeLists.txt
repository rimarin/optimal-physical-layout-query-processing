cmake_minimum_required(VERSION 3.26)

project(oplqp)
add_subdirectory(src)
add_subdirectory(test)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Parquet REQUIRED)

set(CMAKE_CXX_STANDARD 17)

set(SOURCES
    src/apply_partitioning.cpp
    src/common/DataLayoutConverter.cpp
    src/common/KDTree.cpp
    src/common/QuadTree.cpp
    src/storage/DataWriter.cpp
    src/storage/DataReader.cpp
    src/partitioning/NoPartitioning.cpp
    src/partitioning/FixedGridPartitioning.cpp
    src/partitioning/KDTreePartitioning.cpp
    src/partitioning/QuadTreePartitioning.cpp)
add_executable(oplqp ${SOURCES})

target_link_libraries(oplqp PRIVATE Arrow::arrow_shared parquet arrow_dataset)
target_include_directories(oplqp PRIVATE src/include)