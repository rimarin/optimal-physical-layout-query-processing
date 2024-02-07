set(PARTITIONER_SOURCES
    experimentsConfig.cpp
    partitioner.cpp
)

add_executable(partitioner ${PARTITIONER_SOURCES})

set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

target_include_directories(partitioner
        PUBLIC ../libpartitioner/include
)

target_link_libraries(partitioner libpartitioner)

install(TARGETS partitioner DESTINATION bin)
