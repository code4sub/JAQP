package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;


class Producer {
  String dataTopic;
  String queryTopic;
  String deletionTopic;
  String ticktockTopic;
  String serverAddr;
  Properties properties;
  KafkaProducer<String, String> producer;

  public Producer(String dataTopic, String queryTopic,
                  String deletionTopic, String tickTopic){
    this.dataTopic = dataTopic;
    this.ticktockTopic = tickTopic;
    this.queryTopic = queryTopic;
    this.deletionTopic = deletionTopic;
    this.serverAddr = "localhost:9092";
    this.properties = getStreamConfig();
    producer = new KafkaProducer<String, String>(properties);
    System.out.println("Initialized producer: " + serverAddr + ", " + dataTopic + ", " + queryTopic + ", " + deletionTopic + ", " + tickTopic);
  }

  Properties getStreamConfig() {
    final Properties props = new Properties();
    props.put("bootstrap.servers", serverAddr);
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }

  private void insertLinesToTopic(List<String> lines, String topic){
    for(String line: lines){
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, line);
        producer.send(record);
    }
  }

  public void tick(String s){ticktock("t0 " + s);}
  public void tock(){ticktock("t1");}

  public void insertData(List<String> lines, boolean setTick){
    if(setTick) tick("Insertion");
    insertLinesToTopic(lines, dataTopic);
  }

  public void insertQuery(List<String> lines, boolean setTick){
    if(setTick) tick("Query");
    insertLinesToTopic(lines, queryTopic);
  }

  public void insertDeletion(List<String> lines, boolean setTick){
    if(setTick) tick("Deletion");
    insertLinesToTopic(lines, deletionTopic);
  }

  public void ticktock(String s){
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(ticktockTopic, s);
    producer.send(record);
  }

}