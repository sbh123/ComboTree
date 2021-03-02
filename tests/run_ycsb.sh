#!/bin/bash
BUILDDIR=$(dirname "$0")/../build/
WORKLOADDIR=$(dirname "$0")/../include/ycsb/workloads/

function Run() {
    dbname=$1
    loadnum=$2
    opnum=$3
    scanop=$4
    thread=$5
    # ${WORKLOADDIR}/workloads_set.sh ${loadnum} ${opnum}

    ${BUILDDIR}/ycsb -db ${dbname} -threads ${thread} -P ${WORKLOADDIR} | tee ycsb-${dbname}-${thread}.txt
}

# DBName: combotree fastfair pgm xindex alex
Run pgm 4000000 100000 10000 1