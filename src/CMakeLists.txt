include_directories(include/)
add_executable(oplqp main.cpp
                    partitioning/FixedGrid.cpp
                    storage/DataWriter.cpp)
