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
function run_all() {
    dbs="combotree fastfair pgm alex xindex"
    for dbname in $dbs; do
        echo "Run: " $dbname
        Run $dbname $1 $2 $3 1
        # sleep 100
    done
}

dbname="combotree"
loadnum=4000000
opnum=100000
scansize=4000000
thread=1
if [ $# -ge 1 ]; then
    dbname=$1
fi
if [ $# -ge 2 ]; then
    loadnum=$2
fi
if [ $# -ge 3 ]; then
    opnum=$3
fi
if [ $# -ge 4 ]; then
    scansize=$4
fi
if [ $# -ge 5 ]; then
    thread=$5
fi
if [ $dbname == "all" ]; then
    run_all $loadnum $opnum $scansize $thread
else
    Run $dbname $loadnum $opnum $scansize $thread
fi 
# Run combotree 4000000 100000 4000000 1