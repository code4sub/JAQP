#!/bin/bash

# measureing the performance of insertion/deletion/query processing end2end.
# collecting the throughput of insertions and deletions to show.
# we measure the query acc and latency in sync mode.
# Need to record the accuracy of both the query and reservoir at the same time, while the query perf needs to be measured again without reservoir sampling.


source ./utils.sh

percentage=$1
resultPath=$2
sp=$(echo ${percentage}|sed s/\\.//g)
echo $sp
if [ "$#" != "2" ]
then
    echo "Usage: $0: percentage resultpath"
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
querysize=2000
queryfile=${COMPASS}/files/Taxi-RangeSumQuery-pickup_time-trip_distance-2000.txt

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
cmd="./run.sh $expclass 10000 -dataset $dataset -tattr trip_distance -pattr pickup_time -rpath $resultPath/csv -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic -k 128 -baselines pass -async -preload > $resultPath/log/$output &"
# cmd="./run.sh $expclass 10000 -dataset $dataset -tattr trip_distance -pattr pickup_time -rpath $resultPath -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic -k 128 -baselines pass,reservoir &"
echo $cmd
eval $cmd
cd -

while [ ! -f $flag ]
do
    printf "Waiting for PASS to be ready: `date`\r"
    sleep 1
done

cmdfile=/tmp/pass/repartitionPT
echo "Generated Qonly cmd file: "$cmdfile
genQonly $cmdfile $querysize
cat $cmdfile
cd ${COMPASS}/producer
echo "PASS is ready, starting conductor"
./run.sh -csvpath $datasetpath -querypath $queryfile -cmdpath ${cmdfile} -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic
cd -
