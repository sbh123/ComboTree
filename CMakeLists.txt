project(ComboTree)
cmake_minimum_required(VERSION 3.0)

# use `cmake -DSERVER:BOOL=ON ..` when running in server
option(SERVER "Run in server" ON)

# ComboTree Configuration
# use `make clean && make CXX_DEFINES="-DNAME=VALUE"` to override during compile
if(SERVER)
  set(CLEVEL_PMEM_FILE_SIZE "(1024*1024*1024*16UL)")
  set(CLEVEL_PMEM_FILE      \"/pmem0/combotree-clevel-\")
  set(BLEVEL_PMEM_FILE      \"/pmem0/combotree-blevel-\")
  set(SERVER                1)
else()
  set(CLEVEL_PMEM_FILE_SIZE "(1024*1024*512UL)")
  set(CLEVEL_PMEM_FILE      \"/mnt/pmem0/combotree-clevel-\")
  set(BLEVEL_PMEM_FILE      \"/mnt/pmem0/combotree-blevel-\")
  set(SERVER                0)
endif(SERVER)
set(BLEVEL_EXPAND_BUF_KEY 6)
set(EXPANSION_FACTOR      4)
set(DEFAULT_SPAN          2)
set(PMEMKV_THRESHOLD      1024)
set(ENTRY_SIZE_FACTOR     1.2)
set(STREAMING_STORE       0)
set(STREAMING_LOAD        0)
set(BUF_SORT              0)

configure_file(
  "${PROJECT_SOURCE_DIR}/src/combotree_config.h.in"
  "${PROJECT_SOURCE_DIR}/src/combotree_config.h"
)

# -march=native used to detect CPU flags and use corrosponding flush instruction
set(CMAKE_CXX_FLAGS "-march=native -Wall")

# Require C++17
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_STANDARD 17)

# Debug or Release
# Use command `cmake -DCMAKE_BUILD_TYPE=Release ..` to make a release build
if(CMAKE_BUILD_TYPE MATCHES Release)
  message(STATUS "Release build.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  add_definitions(-DNDEBUG)
else()
  message(STATUS "Debug build.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
endif()

# combotree shared library
include_directories(include)

set(COMBO_TREE_SRC
      src/alevel.cc
      src/blevel.cc
      src/clevel.cc
      src/combotree.cc
      src/pmemkv.cc
)

add_library(combotree SHARED ${COMBO_TREE_SRC})
target_link_libraries(combotree pmem pmemkv pthread)

# benchmark
add_executable(benchmark tests/benchmark.cc)
target_link_libraries(benchmark combotree)

# multi_benchmark
add_executable(multi_benchmark tests/multi_benchmark.cc)
target_link_libraries(multi_benchmark combotree)

# Unit Test
enable_testing()
include_directories(src)

## generate_data
add_executable(generate_data tests/generate_data.cc)

## clevel_test
add_executable(clevel_test tests/clevel_test.cc src/clevel.cc)
target_link_libraries(clevel_test pmem)
add_test(clevel_test clevel_test)

## combotree_test
add_executable(combotree_test tests/combotree_test.cc)
target_link_libraries(combotree_test combotree)
add_test(combotree_test combotree_test)

## multi_combotree_test
add_executable(multi_combotree_test tests/multi_combotree_test.cc)
target_link_libraries(multi_combotree_test combotree)
add_test(multi_combotree_test multi_combotree_test)