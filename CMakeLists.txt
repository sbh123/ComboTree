cmake_minimum_required(VERSION 3.3)
project(ComboTree)

set(CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g")

include_directories(src)

## Test
enable_testing()

add_executable(experiment tests/experiment.cc)

# clevel test
add_executable(clevel_test tests/clevel_test.cc src/clevel.cc)
target_link_libraries(clevel_test pmemobj)

# blevel test
add_executable(blevel_test tests/blevel_test.cc src/blevel.cc src/clevel.cc)
target_link_libraries(blevel_test pmemobj)

# alevel test
add_executable(alevel_test tests/alevel_test.cc src/alevel.cc src/blevel.cc src/clevel.cc)
target_link_libraries(alevel_test pmemobj)

# alevel test
add_executable(pmemkv_test tests/pmemkv_test.cc src/pmemkv.cc)
target_link_libraries(pmemkv_test pmemkv)

add_test(alevel_test alevel_test)
add_test(blevel_test blevel_test)
add_test(clevel_test clevel_test)
add_test(pmemkv_test pmemkv_test)