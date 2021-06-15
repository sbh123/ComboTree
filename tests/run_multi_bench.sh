#!/bin/bash
BUILDDIR=$(dirname "$0")/../build/

function Run() {
    dbname=$1
    loadnum=$2
    opnum=$3
    scansize=$4
    thread=$5
    # numactl --cpubind=1 --membind=1 
    gdb --args ${BUILDDIR}/multi_bench --dbname ${dbname} \
        --load-size ${loadnum} --put-size ${opnum} --get-size ${opnum} \
        -t $thread | tee multi-${dbname}-th${thread}.txt
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

dbname="xindex"
loadnum=400000000
opnum=10000000
scansize=4000000
# thread=4

for thread in 4 8 12 16 24 48
do
    Run $dbname $loadnum $opnum $scansize $thread
done
# if [ $# -ge 1 ]; then
#     dbname=$1
# fi
# if [ $# -ge 2 ]; then
#     loadnum=$2
# fi
# if [ $# -ge 3 ]; then
#     opnum=$3
# fi
# if [ $# -ge 4 ]; then
#     scansize=$4
# fi
# if [ $# -ge 5 ]; then
#     thread=$5
# fi
# if [ $dbname == "all" ]; then
#     run_all $loadnum $opnum $scansize $thread
# else
#     Run $dbname $loadnum $opnum $scansize $thread
# fi 

# Run combotree 4000000 100000 4000000 1