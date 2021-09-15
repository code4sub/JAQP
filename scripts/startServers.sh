#!/bin/bash
echo "Zookeeper"
echo "./kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties"
echo "Kafka server"
echo "./kafka/bin/kafka-server-start.sh kafka/config/server.properties"
echo "Taxi Producer"
echo "./run.sh ../data/taxi.csv ../files/Taxi-RangeSumQuery-pickup_time-trip_distance-2000.txt stdin dummy0 taxi-query taxi-deletion ticktock0"
echo "Stdin Producer"
echo "./run.sh -csvpath ../data/intel.csv -querypath ../files/Taxi-RangeSumQuery-pickup_time-trip_distance-2000.txt -cmdpath stdin -datatopic intel-data-full -querytopic taxi-query -deltopic taxi-deletion -ticktopic ticktock0"