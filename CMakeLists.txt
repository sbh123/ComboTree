project(ComboTree)
cmake_minimum_required(VERSION 3.0)

# -march=native used to detect CPU flags and use corrosponding flush instruction
set(CMAKE_CXX_FLAGS -march=native)