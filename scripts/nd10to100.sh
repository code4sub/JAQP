#!/bin/bash

# measureing the performance of insertion/deletion/query processing end2end.
# collecting the throughput of insertions and deletions to show.
# we measure the query acc and latency in sync mode.
# Need to record the accuracy of both the query and reservoir at the same time, while the query perf needs to be measured again without reservoir sampling.


# Continuous baseline: this file
# Repartition baseline: repartitionPDT.sh

source ./utils.sh

percentage=0.1
resultPath=$1
sp=$(echo ${percentage}|sed s/\\.//g)
echo $sp
if [ "$#" != "1" ]
then
    echo "Usage: $0: resultpath"
    exit
fi

flag="/tmp/compass.PASS"
rm -f $flag

expclass="End2End"

ttopic=ticktock
deltopic=etf-deletion
qtopic=etf-query
dataset=etf
datatopic="etf-data-${sp}"

datasetpath=${COMPASS}/data/etf-noheader.csv

querysize=2000

queryfile=${COMPASS}/files/ETF-RangeSumQuery-close_high_idate_low_open-volume-2000.txt
echo $datasetpath
totalsize=`wc -l $datasetpath|cut -d' ' -f1`
sz=`python -c "print(int($percentage*$totalsize))"`
output="${expclass}_${sz}_${batchsize}_${querysize}_${percentage}.log"
batchsize=`python -c "print(int(0.1*$totalsize))"`

#always populate the data, IDQ might polluted it.
echo "Populating $datatopic: $percentage * $totalsize = $sz"
populate $datatopic $datasetpath $sz

#sync mode, with reservoir
echo "Starting PASS: `date`"
cd $COMPASS/compass
cmd="./run.sh $expclass 13000 -dataset $dataset -tattr volume -pattr open,close,low,high,idate -rpath $resultPath/csv -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic -k 256 -baselines pass -async -preload > $resultPath/log/$output &"

#> $resultPath/log/$output

# for debugging
# cmd="./run.sh $expclass 10000 -dataset $dataset -tattr trip_distance -pattr pickup_datetime -rpath $resultPath -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic -k 128 -baselines pass -async -preload &"

echo $cmd
eval $cmd
cd -

while [ ! -f $flag ]
do
    printf "Waiting for PASS to be ready: `date`\r"
    sleep 1
done
# sleep 100
cmdfile=/tmp/pass/10tofullND
echo "Generated 10tofullIQ cmd file: "$cmdfile
genIQ10 $cmdfile $batchsize $sz $querysize $totalsize
cat $cmdfile
cd ${COMPASS}/producer
echo "PASS is ready, starting conductor"
./run.sh -csvpath $datasetpath -querypath $queryfile -cmdpath ${cmdfile} -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic
cd -
