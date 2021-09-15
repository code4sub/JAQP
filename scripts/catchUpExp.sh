#!/bin/bash

#Use a for loop to wrap this script from 0.1 to 1.0

source ./utils.sh

catchup=$1
resultPath=$2

if [ "$resultPath" == "" ]
then
    echo "result path not specified"
    exit
fi
mkdir -p $resultPath
mkdir -p /tmp/pass

flag="/tmp/compass.PASS"
rm -f $flag
expclass="End2End"

ttopic=ticktock0
deltopic=intel-deletion
qtopic=intel-query
dataset=intelwireless
datatopic=intel-data-full

cd $COMPASS/compass
echo "Starting PASS"

if true
then
    cmd="./run.sh $expclass 10000 -dataset $dataset -tattr light -pattr itime -rpath $resultPath/csv/ -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic -k 128 -async -preload -oversamplex 1 -catchup $catchup -baselines pass > ${resultPath}/log/catchup${catchup}.log &"
    echo $cmd
    eval $cmd
else
    dbgcmd="./run.sh $expclass 10000 -dataset $dataset -tattr light -pattr itime -rpath $resultPath/debug/ -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic -k 64 -async -oversamplex 1 -catchup $catchup -baselines pass &"
    echo "Debugging"
    echo $dbgcmd
    eval $dbgcmd
fi

cd -

while [ ! -f $flag ]
do
    printf "Waiting for PASS to be ready: `date`\r"
    sleep 1
done

datasetpath=${COMPASS}/data/intel.csv
queryfile=${COMPASS}/files/IntelWireless-RangeSumQuery-itime-light-2000.txt
cmdfile=/tmp/pass/catchupcmd
querysize=2000
echo "Generated Qonly cmd file: "$cmdfile
genQonly $cmdfile $querysize
cat $cmdfile
cd ${COMPASS}/producer
echo "PASS is ready, starting conductor"
./run.sh -csvpath $datasetpath -querypath $queryfile -cmdpath ${cmdfile} -datatopic $datatopic -querytopic $qtopic -deltopic $deltopic -ticktopic $ttopic
cd -

