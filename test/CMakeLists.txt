include_directories(../src/)
add_executable(oplqp ../src/storage/DataWriter.cpp
                     ../src/storage/DataReader.cpp
                     ../src/partitioning/NoPartitioning.cpp
                     ../src/partitioning/FixedGrid.cpp
                     ../src/partitioning/KDTree.cpp
                     main.cpp)
