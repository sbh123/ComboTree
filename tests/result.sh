#!/bin/bash

# get_iops filename dbname
function microbench_get_iops()
{
    echo $1 $2
    tail $1 -n 100 | grep "Metic-Load" | awk '{print $7/1e3}'
    tail $1 -n 100 | grep "Metic-Operate" | awk '{print $9/1e3}'
}

function scalability_get_write_iops()
{
    echo $1 $2
    cat $1 | grep "Metic-Write" | grep -v 'Read' | grep "iops" | awk '{print $9/1e3}'
}

function scalability_get_read_iops()
{
    echo $1 $2
    cat $1 | grep "Metic-Read"  | grep "iops" | awk '{print $9/1e3}'
}

function mult_th_iops_result()
{
    
    cat $1 | grep $2  | grep "iops" | awk '{print $7/1e6}'
}

dbname=letree
workload=longlat-insertio
logfile="microbench-$dbname-$workload.txt"

# microbench_get_iops $logfile $dbname

dbs="fastfair alex pgm xindex letree"

# for dbname in $dbs; do
#     echo "$dbname"
#     logfile="microbench-$dbname-$workload.txt"
#     microbench_get_iops $logfile $dbname
# done

if [ $# -ge 1 ]; then
    dbname=$1
fi

# logfile="scalability-$dbname-longlat-400m.txt"
# scalability_get_read_iops $logfile $dbname

for thread in 4 8 12 16 24 48
do
    logfile=multi-${dbname}-th${thread}.txt
    mult_th_iops_result $logfile "Metic-Get"
done