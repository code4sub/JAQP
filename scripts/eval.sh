#!/bin/bash
dir=$1
key=$2
for f in `ls $dir/*`
do
    echo $f
    ../python/acceval.py $f ../python/cache/taxi-percentage.json $key
done
