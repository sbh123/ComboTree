#!/bin/bash

function GetLoadLat()
{
    cat $1 | grep 'Metic-Load' | awk '{print $7/1e3}'
}

function GetOperateLat()
{
    cat $1 | grep 'Metic-Operate' | awk '{print $9/1e3}'
}


GetLoadLat $1

GetOperateLat $1
