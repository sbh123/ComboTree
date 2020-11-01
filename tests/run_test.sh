#!/bin/bash
# need to run cmake first

BUILDDIR=$(dirname "$0")/../build/

cd $BUILDDIR
for expand_buf_key in 2 4 6 8 10
do
for expansion_factor in 2 4 8 16
do
  make clean
  make CXX_DEFINES="-DBLEVEL_EXPAND_BUF_KEY=$expand_buf_key -DEXPANSION_FACTOR=$expansion_factor" -j $((`nproc`*2))
  ./combotree_test 2>&1 | tee "$expand_buf_key-$expansion_factor.txt"
done
done