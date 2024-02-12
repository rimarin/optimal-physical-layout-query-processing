include(FetchContent)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        release-1.12.1
)

FetchContent_MakeAvailable(googletest)
add_library(GTest::GTest INTERFACE IMPORTED)
target_link_libraries(GTest::GTest INTERFACE gtest_main)

set(DUCKDB_DIR "/data/ssd/riccardo.marin/libduckdb-linux-amd64")

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

include_directories(${GTEST_INCLUDE_DIRS})

file(GLOB SOURCES *.cpp)
add_executable(partitioner_test ${SOURCES})

set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fsanitize=address")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

target_link_libraries(partitioner_test libpartitioner gtest gtest_main gmock_main ${DUCKDB_LIB})

target_include_directories(partitioner_test
        PUBLIC ../libpartitioner/include
        PUBLIC ../partitioner
)

target_include_directories(partitioner_test PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_include_directories(partitioner_test PRIVATE ${DUCKDB_DIR} include)

add_test(NAME partitioner_test
        COMMAND partitioner_test)
