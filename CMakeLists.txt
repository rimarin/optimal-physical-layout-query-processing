cmake_minimum_required(VERSION 3.26)

project(oplqp)

find_package(Arrow REQUIRED)

set(CMAKE_CXX_STANDARD 17)

add_executable(oplqp src/main.cpp)

target_link_libraries(oplqp PRIVATE Arrow::arrow_shared parquet)
