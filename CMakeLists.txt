cmake_minimum_required(VERSION 3.10)

project(BlackBox LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add include/ directory so #include "BlackBox.hpp" works
include_directories(include)

# Collect source files
set(SOURCES
    src/main.cpp
    src/engine/BlackBox.cpp
    src/core/Analyzer.cpp
    src/storage/LogStore.cpp
    src/server/BlackBoxHttpServer.cpp
    src/algorithms/SearchAlgorithms.cpp
)

add_executable(BlackBox ${SOURCES})

# Tests
add_executable(BlackBoxTests
    src/engine/BlackBox.cpp
    src/core/Analyzer.cpp
    src/storage/LogStore.cpp
    src/algorithms/SearchAlgorithms.cpp
    tests/BlackBoxTests.cpp
)
target_include_directories(BlackBoxTests PRIVATE include)
