package compass;

import java.util.*;
import java.time.Duration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.protocol.Message;

import static java.util.stream.Collectors.joining;

interface DataSource {
    public abstract void registerConsumer(Consumer c);
    public abstract void removeConsumer(Consumer c);
    public abstract void notifyConsumers(TopicMessage msg);
    public void subscribeAndRead(String dataTopic, String queryTopic, String deletionTopic, String tickTopic);
}

class KafkaDataSource implements DataSource{
    boolean isDebugging = false;
    Random randGen = new Random(123);
    Thread dispatcher;
    private List<Consumer> consumers;
    KafkaConsumer<String, String> kafkaConsumer;
    boolean stop = false;
    volatile Queue<TopicMessage> msgQueue = new LinkedList<TopicMessage>();
    public KafkaDataSource(){
        consumers = new ArrayList<Consumer>();
        kafkaConsumer = new KafkaConsumer<String, String>(getStreamConfig());
    }
    public void setDebug(boolean b){
        isDebugging = b;
    }
    public synchronized void enqueMsg(List<TopicMessage> msgs){
	for(TopicMessage m: msgs)
	    msgQueue.add(m);
	if(isDebugging)
	    System.out.println("queue: " + msgQueue.size());
    }
    public synchronized TopicMessage popQueue(){
        return msgQueue.remove();
    }
    public void registerConsumer(Consumer c){
        consumers.add(c);
    }
    public void removeConsumer(Consumer c){
        consumers.remove(c);
    }
    public void notifyConsumers(TopicMessage msg){
        for(Consumer c:consumers){
            if(msg.messageType == MessageType.TICK && msg.message.startsWith("t1")){
                while(System.currentTimeMillis() - ((Solver)(c)).getStopTick() < 1000){
                    System.out.print("Tock: wait prev tasks to finish on " + c.getClass().getName() + ", diff: " + (System.currentTimeMillis() - ((Solver)(c)).getStopTick())+"\r");
                    try{Thread.sleep(100L);}catch(Exception e){}
                }
            }
            c.update(msg);
        }
    }

    public void dispatchMessage(){
        System.out.println("Message dispatcher started");
        while(true){
            while(msgQueue.size() > 0){
                notifyConsumers(popQueue());
            }
            if(stop && msgQueue.size() == 0)break;
        }
    }

    Properties getStreamConfig() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("fetch.max.bytes", 102400000);
        props.put("fetch.min.bytes", 1);
        props.put("max.partition.fetch.bytes", 102400000);
        props.put("max.poll.records", 102400);
        props.put("group.id", "PASSDS"+randGen.nextInt());
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public void seekToEnd(){
        //seek to end so we ignore previous query/ctrl commands.
        kafkaConsumer.poll(0);
        kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
    }

    public void subscribeAndRead(String dataTopic, String queryTopic, String deletionTopic, String tickTopic){
        List<String> topics = Arrays.asList(dataTopic, queryTopic, deletionTopic, tickTopic);
        kafkaConsumer.subscribe(topics);
        seekToEnd();

        System.out.println("\n================================\nKafkaDataSource subscribed to: " + topics.stream().collect(joining(", ")));
        dispatcher = new Thread(()->{
            dispatchMessage();
        });
        dispatcher.start();
        try{
            while(!stop){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                if(isDebugging && records.count()>0)
                    System.out.print("Received " + records.count()+" records from Kafka.\n");
                List<TopicMessage> msgs = new ArrayList<TopicMessage>();
                for(ConsumerRecord<String, String> record : records){
                    MessageType mt = MessageType.DATA;

                    if (record.topic().equals(queryTopic))
                        mt = MessageType.QUERY;
                    else if(record.topic().equals(deletionTopic))
                        mt = MessageType.DELETION;
                    else if(record.topic().equals(tickTopic))
                        mt = MessageType.TICK;
                    if(mt == MessageType.QUERY && record.value().compareTo("exit") == 0){
                        msgs.add(new TopicMessage(MessageType.EXIT, record.value()));
                        stop = true;
                    }else if(mt == MessageType.QUERY && record.value().compareTo("save") == 0){
                        msgs.add(new TopicMessage(MessageType.SAVE, record.value()));
                    }else if(mt == MessageType.QUERY && record.value().compareTo("rndelete") == 0){
                        msgs.add(new TopicMessage(MessageType.RNDELETE, record.value()));
                    }else
                        msgs.add(new TopicMessage(mt, record.value()));
                }
            kafkaConsumer.commitSync();
            if(msgs.size() > 0)
                enqueMsg(msgs);
                }
            System.out.println("Kafka data source has stopped, waiting for dispatcher.");
            dispatcher.join();
        }catch(Exception e){
            System.out.println(e);
            e.printStackTrace();
        }finally{
            try{
                kafkaConsumer.commitSync();
            }catch(CommitFailedException e){
                System.out.println(e);
            }
            kafkaConsumer.close();
        }
    }
}

interface Consumer {
    public abstract void update(TopicMessage msg);
}
