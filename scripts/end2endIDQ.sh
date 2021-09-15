#!/bin/bash

# measureing the performance of insertion/deletion/query processing end2end.
# collecting the throughput of insertions, deletions and queries to show.
# we also measure the query acc and latency in sync mode (end2endQ)
# for i in 0.5 0.6 0.7 0.8 0.9; do  ./end2endQ.sh $i ../results/end2end128/syncQ; ./end2endIDQ.sh $i 0 ../results/end2end128/IDQ; done

source ./utils.sh

percentage=$1
dopopulate=$2
resultPath=$3
sp=$(echo ${percentage}|sed s/\\.//g)
echo $sp
if [ "$#" != "3" ]
then
    echo "Usage: $0: percentage dopopulate resultpath"
    exit
fi

flag="/tmp/compass.PASS"
rm -f $flag

expclass="End2End"

ttopic=ticktock0
deltopic=taxi-deletion
qtopic=taxi-query
dataset=taxi
datatopic="taxi-data-${sp}"

datasetpath=${COMPASS}/data/taxi.csv
batchsize=766000
querysize=2000

queryfile=${COMPASS}/files/Taxi-RangeSumQuery-pickup_time-trip_distance-2000.txt
echo $datasetpath
totalsize=`wc -l $datasetpath|cut -d' ' -f1`
sz=`python -c "print(int($percentage*$totalsize))"`
output="${expclass}_${sz}_${batchsize}_${querysize}_${percentage}.log"

if [ "$dopopulate" == "1" ]
then
    echo "Populating $datatopic: $percentage * $totalsize = $sz"
    populate $datatopic $datasetpath $sz
fi

mkdir -p logresults

echo "Starting PASS: `date`"
cd $COMPASS/compass
cmd="./run.sh $expclass 10000 -dataset $dataset -tattr trip_distance -pattr pickup_time -rpath $resultPath/csv -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic -k 128 -async -preload -baselines pass > $resultPath/log/$output &"
echo $cmd
eval $cmd
cd -

while [ ! -f $flag ]
do
    printf "Waiting for PASS to be ready: `date`\r"
    sleep 1
done

cmdfile=/tmp/pass/end2endIDQ
echo "Generated Qonly cmd file: "$cmdfile
genIDQ $cmdfile $batchsize $sz $querysize
cat $cmdfile
cd ${COMPASS}/producer
echo "PASS is ready, starting conductor"
./run.sh -csvpath $datasetpath -querypath $queryfile -cmdpath ${cmdfile} -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic
cd -
