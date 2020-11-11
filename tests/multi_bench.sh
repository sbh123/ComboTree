#!/bin/bash
for thread in 4 8 12 16 24 48
do
    ./multi_benchmark --use-data-file --test-size 410000000 --last-expand 400000000\
        --scan-test-size 500000000 --get-size 10000000\
        -s 10 -s 100 -s 1000 -s 10000\
        --sort-scan 10 --sort-scan 100 --sort-scan 1000 --sort-scan 10000\
        -t $thread | tee multi-${thread}.txt
done