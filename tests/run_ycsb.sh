#!/bin/bash
BUILDDIR=$(dirname "$0")/../build/
WORKLOAD="workloads"  #workloads
WORKLOADDIR=$(dirname "$0")/../include/ycsb/$WORKLOAD/

function Run() {
    dbname=$1
    loadnum=$2
    opnum=$3
    scanop=$4
    thread=$5
    # ${WORKLOADDIR}/workloads_set.sh ${loadnum} ${opnum}

    ${BUILDDIR}/ycsb -db ${dbname} -threads ${thread} -P ${WORKLOADDIR} | tee ycsb-${dbname}-${WORKLOAD}-tmp1.txt
}

function run_all() {
    dbs="learngroup fastfair alex pgm xindex"
    for dbname in $dbs; do
        echo "Run: " $dbname
        Run $dbname $1 $2 $3 1
        sleep 100
    done
}
# DBName: combotree fastfair pgm xindex alex
# Run pgm 4000000 100000 10000 1
# for dbname
Run letree 4000000 100000 10000 1
# sleep 100
# Run learngroup 4000000 100000 10000 1
# run_all 4000000 100000 10000