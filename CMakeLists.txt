cmake_minimum_required(VERSION 3.26)

project(oplqp)
add_subdirectory(src)

find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(Parquet REQUIRED)

set(CMAKE_CXX_STANDARD 17)

target_link_libraries(oplqp PRIVATE Arrow::arrow_shared parquet arrow_dataset)
target_include_directories(oplqp PRIVATE src/include)