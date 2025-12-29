cmake_minimum_required(VERSION 3.10)

project(BlackBox LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(POLICY CMP0069)
  cmake_policy(SET CMP0069 NEW)
endif()

# Prefer optimized builds by default; can override with -DCMAKE_BUILD_TYPE=Debug
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Common high-performance flags
set(BLACKBOX_OPT_FLAGS)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  list(APPEND BLACKBOX_OPT_FLAGS -O3 -march=native -DNDEBUG)
  if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
elseif(MSVC)
  list(APPEND BLACKBOX_OPT_FLAGS /O2 /DNDEBUG)
endif()

find_package(Threads REQUIRED)

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
target_compile_options(BlackBox PRIVATE ${BLACKBOX_OPT_FLAGS})
target_link_libraries(BlackBox PRIVATE Threads::Threads)

# Tests
add_executable(BlackBoxTests
    src/engine/BlackBox.cpp
    src/core/Analyzer.cpp
    src/storage/LogStore.cpp
    src/algorithms/SearchAlgorithms.cpp
    tests/BlackBoxTests.cpp
)
target_include_directories(BlackBoxTests PRIVATE include)
target_compile_options(BlackBoxTests PRIVATE ${BLACKBOX_OPT_FLAGS})
target_link_libraries(BlackBoxTests PRIVATE Threads::Threads)
