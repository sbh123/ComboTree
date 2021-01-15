#!/bin/bash
RecordCount=10000000
OpCount=1000000
WorkLoads="0 10 20 50 80 100"
if [ $# -gt 1 ]; then
    RecordCount=$1
fi

if [ $# -gt 2 ]; then
    OpCount=$2
fi

for workload in $WorkLoads; do
    sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" workload_insert_${workload}.spec
    sed -r -i "s/operationcount=.*/operationcount=$OpCount/1" workload_insert_${workload}.spec
done