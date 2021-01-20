#!/bin/bash

# 测试不同数据量的FastFair的读写性能: YCSB uniform

LoadDatas="100000 1000000 10000000"
DBs="pgm alex xindex"
WorkLoadsDir="./include/ycsb/workloads"
ExecDir="./build"
DBName="fastfair"
RecordCount="10000000"

function OperateDiffLoad() {
    for RecordCount in $LoadDatas;
    do
        sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" ${WorkLoadsDir}/workload_read.spec
        sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" ${WorkLoadsDir}/workload_insert.spec
        ${ExecDir}/ycsb -db $DBName -threads 1 -P $WorkLoadsDir 
        sleep 60
    done
}

function OperateDiffDB() {
    RecordCount="10000000"
    sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" ${WorkLoadsDir}/workload_read.spec
    sed -r -i "s/recordcount=.*/recordcount=$RecordCount/1" ${WorkLoadsDir}/workload_insert.spec
    for DBName in $DBs;
    do
        ${ExecDir}/ycsb -db $DBName -threads 1 -P $WorkLoadsDir
        sleep 60
    done
}

function OperateDiffInsert() {
    WorkLoadsDir="./include/ycsb/insert_ratio"
    for DBName in $DBs;
    do
        ${ExecDir}/ycsb -db $DBName -threads 1 -P $WorkLoadsDir > ${DBName}_insert_ratio.log
        sleep 60
    done
}

# OperateDiffLoad
# OperateDiffDB
OperateDiffInsert