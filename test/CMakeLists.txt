include(FetchContent)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        release-1.12.1
)

FetchContent_MakeAvailable(googletest)
add_library(GTest::GTest INTERFACE IMPORTED)
target_link_libraries(GTest::GTest INTERFACE gtest_main)

include_directories(${GTEST_INCLUDE_DIRS})

file(GLOB SOURCES *.cpp)
add_executable(partitioner_test ${SOURCES})

set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fsanitize=address")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

target_link_libraries(partitioner_test libpartitioner gtest gtest_main gmock_main)

target_include_directories(partitioner_test
        PUBLIC ../libpartitioner/include
        PUBLIC ../partitioner
)

target_include_directories(partitioner_test PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_test(NAME partitioner_test
        COMMAND partitioner_test)
