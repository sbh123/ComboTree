#!/bin/bash

# get_iops filename dbname
function microbench_get_iops()
{
    echo $1 $2
    tail $1 -n 100 | grep "Metic-Load" | awk '{print $7/1e3}'
    tail $1 -n 100 | grep "Metic-Operate" | awk '{print $9/1e3}'
}

dbname=xindex
workload=longlat
logfile="microbench-$dbname-$workload.txt"

microbench_get_iops $logfile $dbname