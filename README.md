# Combo Tree

## Compile

There are two types of build: Debug and Release. Default is Debug build, which has -g option and has no optimization. Release build remove -g option, add NDEBUG definition and has -O3 optimization.

For Debug build:

    mkdir build && cd build
    cmake -DCMAKE_BUILD_TYPE=Debug ..
    make

For Release build:

    mkdir build && cd build
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make
