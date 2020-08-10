cmake_minimum_required(VERSION 3.3)
project(ComboTree)

## Combo Tree Configuration
set(DEFAULT_SPAN        2)
set(PMEMKV_THRESHOLD    1024)
set(CLEVEL_LEAF_ENTRY   8)      # must be even, and must <= 16
set(CLEVEL_INDEX_ENTRY  8)      # must be even
set(EXPANSION_FACTOR    2)
set(ENTRY_SIZE_FACTOR   1.1)    # the entry size factor when expanding

configure_file(
  "${PROJECT_SOURCE_DIR}/src/combotree_config.h.in"
  "${PROJECT_SOURCE_DIR}/src/combotree_config.h"
)

set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

## Debug or Release
# use command `cmake -DCMAKE_BUILD_TYPE=Release ..` to make a release build
if(CMAKE_BUILD_TYPE MATCHES Release)
  message(STATUS "Release build.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  add_definitions(-DNDEBUG)
else()
  message(STATUS "Debug build.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
endif()

include_directories(include)
include_directories(src)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)
find_package(PkgConfig QUIET)

# find libpmemobj++
# include(libpmemobj++)

# find libpmemobj
# if(PKG_CONFIG_FOUND)
#   pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)
# else()
#   find_package(LIBPMEMOBJ REQUIRED)
# endif()

## Test
enable_testing()

add_executable(experiment tests/experiment.cc)

# vector_vs_array test
add_executable(vector_vs_array tests/vector_vs_array.cc)

# lock_vs_lockfree test
add_executable(lock_vs_lockfree tests/lock_vs_lockfree.cc)
target_link_libraries(lock_vs_lockfree pthread)

# mutex test
add_executable(mutex_test tests/mutex_test.cc)

# persistentptr_vs_offset test
add_executable(persistentptr_vs_offset tests/persistentptr_vs_offset.cc)
target_link_libraries(persistentptr_vs_offset pmemobj)

# pmemkv_vs_clevel test
add_executable(pmemkv_vs_clevel tests/pmemkv_vs_clevel.cc src/clevel.cc src/pmemkv.cc)
target_link_libraries(pmemkv_vs_clevel pmemobj pmemkv)

# pmemkv_vs_clevel test
add_executable(pmem_vs_pmemobj tests/pmem_vs_pmemobj.cc)
target_link_libraries(pmem_vs_pmemobj pmemobj pmem)

# clevel test
add_executable(clevel_test tests/clevel_test.cc src/clevel.cc)
target_link_libraries(clevel_test pmemobj)

# blevel test
add_executable(blevel_test tests/blevel_test.cc src/blevel.cc src/clevel.cc)
target_link_libraries(blevel_test pmemobj)

# alevel test
add_executable(alevel_test tests/alevel_test.cc src/alevel.cc src/blevel.cc src/clevel.cc)
target_link_libraries(alevel_test pmemobj)

add_executable(generate_workload tests/generate_workload.cc)

# pmemkv test
add_executable(pmemkv_test tests/pmemkv_test.cc src/pmemkv.cc)
target_link_libraries(pmemkv_test pmemkv)

set(COMBO_TREE_SRC
      src/pmemkv.cc
      src/alevel.cc
      src/blevel.cc
      src/clevel.cc
      src/combotree.cc
)

# combotree test
add_executable(combotree_test tests/combotree_test.cc ${COMBO_TREE_SRC})
target_link_libraries(combotree_test pmemkv pmemobj pthread)

# kvbench test
include_directories(/home/qyzhang/Projects/kvbench/include)
add_executable(kvbench_combo_tree_v2 tests/kvbench_combo_tree.cc ${COMBO_TREE_SRC})
target_link_libraries(kvbench_combo_tree_v2 pmemkv pmemobj protobuf pthread)

add_test(alevel_test alevel_test)
add_test(blevel_test blevel_test)
add_test(clevel_test clevel_test)
add_test(pmemkv_test pmemkv_test)