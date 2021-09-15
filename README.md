# JanusAQP 

## Requirements

Java 8+.

Apache Maven 3.6.3.

Python 3.6.

kafka_2.12-2.7.0

Tested in a Ubuntu 20.04 system, other Linux systems should work, too.

## Basic Kafka operations

* Start Kafka and Zookeeper server with default configuration:

* ./kafka/bin/kafka-server-start.sh kafka/config/server.properties

* ./kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties

* Create a topic named 'topic1':

  * ./kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092  --replication-factor 1 --partitions 2 --topic topic1

* Delete a topic named 'topic1':

  * ./kafka/bin/kafka-topics.sh --delete  --bootstrap-server localhost:9092  --topic topic1

* Start a Kafka console producer to insert data to topic1:

  * ./kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic topic1

* Start a console consumer to read data from topic1:

  * ./kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1  --from-beginning

## This repo.

0. Files:
   1. notebooks: contains figures and raw results.
   2. compass: contains the core code for JanusAQP.
   3. producer: contains code that simulate clients that pump requests into Kafka topics.
   4. scripts: contains shell scripts that are used to automate experiments.
   5. python: contains some helper scripts, e.g. generate random queries, etc.

1. Datasets. [Intel wireless dataset][intel], [NYC Taxi][taxi], [NASDAQ ETF Prices][nasdaq].

2. An experiment involves:
   * a sequence of commands that conduct the experiment (commands are generated on the fly).
   * a producer that pump a series of requests into Kafka according to the commands.
   * a JanusAQP instance (a.k.a. a compass instance) that process the requests.
   * datasets and kafka topics.

3. Please refer to the files in the scripts folder for the commands and workflows used in each experiment.


[intel]: http://db.csail.mit.edu/labdata/labdata.html
[taxi]: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
[nasdaq]: https://www.kaggle.com/jacksoncrow/stock-market-dataset

