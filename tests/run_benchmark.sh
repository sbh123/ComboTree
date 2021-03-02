#!/bin/bash
BUILDDIR=$(dirname "$0")/../build/

function Run() {
    dbname=$1
    loadnum=$2
    opnum=$3
    scansize=$4
    thread=$5
    ${BUILDDIR}/multi_benchmark --dbname ${dbname} --load-size ${loadnum} \
        --put-size ${opnum} --get-size ${opnum} --delete-size ${opnum}\
        --scan-test-size ${scansize} -s 10 -s 100 -s 1000 -s 10000\
        --sort-scan 10 --sort-scan 100 --sort-scan 1000 --sort-scan 10000\
        -t $thread | tee multi-${dbname}-${thread}.txt
}

# DBName: combotree fastfair pgm xindex alex
dbs="combotree fastfair pgm xindex alex"
for dbname in $dbs; do
    echo "Run: " $dbname
    Run $dbname 4000000 100000 4000000 1
    # sleep 100
done