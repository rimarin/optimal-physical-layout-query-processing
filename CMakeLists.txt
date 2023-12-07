cmake_minimum_required(VERSION 3.5)

project(partitioner)

enable_testing()

set(CMAKE_BUILD_TYPE Release)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(libpartitioner)
add_subdirectory(test)
add_subdirectory(partitioner)
