#!/bin/bash
BUILDDIR=$(dirname "$0")/../build/
WorkLoad="/home/sbh/asia-latest.csv"
Loadname="longlat-insertio"
function Run() {
    dbname=$1
    loadnum=$2
    opnum=$3
    scansize=$4
    thread=$5

    date | tee microbench-${dbname}-${Loadname}.txt
    # gdb --args #
    # numactl --cpubind=1 --membind=1 \
    numactl --cpubind=1 --membind=1 ${BUILDDIR}/microbench --dbname ${dbname} --load-size ${loadnum} \
    --put-size ${opnum} --get-size ${opnum} --workload ${WorkLoad} \
    --loadstype 3 -t $thread | tee -a microbench-${dbname}-${Loadname}.txt

    echo "${BUILDDIR}/microbench --dbname ${dbname} --load-size ${loadnum} "\
    "--put-size ${opnum} --get-size ${opnum} --workload ${WorkLoad} --loadstype 2 -t $thread"
}

# DBName: combotree fastfair pgm xindex alex
function run_all() {
    dbs="letree fastfair pgm alex xindex"
    for dbname in $dbs; do
        echo "Run: " $dbname
        Run $dbname $1 $2 $3 1
        sleep 100
    done
}

function main() {
    dbname="combotree"
    loadnum=4000000
    opnum=1000000
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
        echo "Run $dbname $loadnum $opnum $scansize $thread"
        Run $dbname $loadnum $opnum $scansize $thread
    fi 
}
main all 400000000 10000000 100000 1
