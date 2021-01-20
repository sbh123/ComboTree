#!/bin/bash

# 测试不同数据量的FastFair的读写性能: YCSB uniform

LoadDatas="100000 1000000 10000000"
WorkLoadsDir="./include/ycsb/workloads"
ExecDir="./build"
DBName="fastfair"

for RecordCount in $LoadDatas;
do
    sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" ${WorkLoadsDir}/workload_read.spec
    sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" ${WorkLoadsDir}/workload_insert.spec
    sudo ${ExecDir}/ycsb -db $DBName -threads 1 -P $WorkLoadsDir
    sleep 60
done
