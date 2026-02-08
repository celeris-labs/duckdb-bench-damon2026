cmake_minimum_required(VERSION 3.14)
project(damon_bench CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Release build by default for benchmarking
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Optimization flags for benchmarking
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native")

# Find DuckDB - adjust path as needed
# Option 1: System-installed DuckDB
find_package(DuckDB QUIET)

if(NOT DuckDB_FOUND)
    # Option 2: FetchContent to download DuckDB
    include(FetchContent)
    FetchContent_Declare(
        duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG        v1.4.4
        GIT_SHALLOW    TRUE
    )
    
    # DuckDB build options
    set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
    set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
    
    message(STATUS "Fetching DuckDB...")
    FetchContent_MakeAvailable(duckdb)
endif()

add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
add_link_options(-fsanitize=address)

# Main benchmark executable
add_executable(damon_bench src/main.cpp)

if(DuckDB_FOUND)
    target_link_libraries(damon_bench PRIVATE DuckDB::DuckDB)
else()
    target_link_libraries(damon_bench PRIVATE duckdb)
endif()
