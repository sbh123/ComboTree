#!/usr/bin/env bash
mkdir -p build;
cd build;
rm -rf *
if [[ "$#" -ne 0 && $1 == "server" ]]
then
    cmake -DCMAKE_C_COMPILER=/usr/local/bin/gcc -DSERVER:BOOL=ON ..;
else
    cmake -DCMAKE_C_COMPILER=/usr/local/bin/gcc ..;
fi
make;
cd ..;