#!/bin/bash
source ./utils.sh

percentage=$1
dopopulate=$2

if [ "$1" == "" ]
then
    echo "Percentage of existing data is not specified"
    exit
fi

flag="/tmp/compass.PASS"
rm -f $flag

expclass="End2End"

ttopic=ticktock0
deltopic=taxi-deletion
qtopic=taxi-query
dataset=taxi
datatopic=taxi-data

resultPath=${COMPASS}/results/debug/
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
./run.sh $expclass 10000 -dataset $dataset -tattr trip_distance -pattr pickup_time -rpath $resultPath -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic -k 64 -async -qdebug -baselines pass  -oversamplex 1
cd -

# while [ ! -f $flag ]
# do
#     printf "Waiting for PASS to be ready: `date`\r"
#     sleep 1
# done

# cmdfile=/tmp/pass/qdebug
# echo "Generated Qonly cmd file: "$cmdfile
# genQonlyPerf $cmdfile $querysize
# cat $cmdfile
# cd ${COMPASS}/producer
# echo "PASS is ready, starting conductor"
# ./run.sh -csvpath $datasetpath -querypath $queryfile -cmdpath ${cmdfile} -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic
# cd -
