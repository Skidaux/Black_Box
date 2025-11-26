cmake_minimum_required(VERSION 3.10)

project(BlackBox LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add include/ directory so #include "MiniElastic.hpp" works
include_directories(include)

# Collect source files
set(SOURCES
    src/main.cpp
    src/engine/MiniElastic.cpp
    src/core/Analyzer.cpp
    src/storage/LogStore.cpp
    src/server/MiniElasticHttpServer.cpp
)

add_executable(BlackBox ${SOURCES})
