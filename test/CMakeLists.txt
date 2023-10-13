include_directories(../src/)
add_executable(oplqp ../src/storage/DataWriter.cpp
                     ../src/storage/DataReader.cpp
                     ../src/partitioning/NoPartitioning.cpp
                     ../src/partitioning/FixedGridPartitioning.cpp
                     ../src/common/KDTree.cpp
                     ../src/partitioning/KDTreePartitioning.cpp
                     main.cpp)
