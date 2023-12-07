set(PARTITIONER_SOURCES
    experimentsConfig.cpp
    partitioner.cpp
)

add_executable(partitioner ${PARTITIONER_SOURCES})

target_link_libraries(partitioner libpartitioner)

install(TARGETS partitioner DESTINATION bin)
