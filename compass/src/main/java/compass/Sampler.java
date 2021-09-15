package compass;

import java.util.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import static java.util.stream.Collectors.joining;

class SequentialSampler {
    Random randGen;
    String serverAddr;
    final int seed = 1111;
    float sampleRate = 1.0f;
    KafkaConsumer<String, String> kafkaConsumer;
    Map<TopicPartition, Long> partitionSizes;
    Duration pollDuration = Duration.ofMillis(100);
    String dataTopic;
    public void setSampleRate(float f){
        System.out.println("Sequential sampler sample rate: " + f);
        sampleRate = f;
    }
    public SequentialSampler(String dataTopic){
        randGen = new Random(seed);
        this.dataTopic = dataTopic;
        kafkaConsumer = new KafkaConsumer<String, String>(getStreamConfig());
        partitionSizes = new HashMap<TopicPartition, Long>();
        kafkaConsumer.subscribe(Arrays.asList(dataTopic));
        System.out.println(this.getClass().getName()+" subscribed to: " + dataTopic);
    }

    public KafkaConsumer<String, String> getKafkaConsumer(){return kafkaConsumer;}
    public Properties getStreamConfig() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("fetch.max.bytes", 102400000);
        props.put("fetch.min.bytes", 1);
        props.put("max.partition.fetch.bytes", 102400000);
        props.put("max.poll.records", 10240);
        props.put("group.id", "DPASSSequentialSampler"+randGen.nextInt());
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public long getTotalPartitionSize(){
        if(partitionSizes.size() == 0)
            updatePartitionSize();
        assert partitionSizes.size() > 0: "Kafka partitions are empty, we need sample to build the tree.";
        long ret = 0;
        for(long size: partitionSizes.values())
            ret += size;
        return ret;
    }

    void updatePartitionSize(){
        System.out.println("Updating partition size...");
        kafkaConsumer.poll(0); //has to be poll(0) to work; Duration.ofMillis(0));
        kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
        for(TopicPartition tp: kafkaConsumer.assignment()){
            System.out.println(tp + " size: " + kafkaConsumer.position(tp)); // + " position: " + kafkaConsumer.position(tp));
            partitionSizes.put(tp, kafkaConsumer.position(tp));
        }
        // System.out.println(partitionSizes);
    }

    public Map<TopicPartition, Long> getPartitionSizes(){
        return partitionSizes;
    }

    public void resetOffset(){
        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
    }

    void setRandomOffset(Collection<TopicPartition> tps){
        for(TopicPartition tp: tps){
            long offset = Math.abs(randGen.nextLong()) % partitionSizes.get(tp);
            kafkaConsumer.seek(tp, offset);
        }
    }

    public List<String> sampleFromTopic(int n){
        List<String> sample = new ArrayList<String>();
        long retrived = 0;
        try{
            while(sample.size() < n){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(pollDuration);
                retrived += records.count();
                for(ConsumerRecord<String, String> record : records){
                    //add 0.01 to avoid starving
                    if(ThreadLocalRandom.current().nextFloat() <= sampleRate+0.01){
                        sample.add(record.value());
                        // System.out.println("+1");
                    }
                }
                if(records.isEmpty()){
                    break;
                }
            }
        }catch(Exception e){
            System.out.println(e);
            e.printStackTrace();
        }
        if(sample.size() > retrived*sampleRate){
            // System.out.println("Shuffle and subsample");
            // we oversample to avoid starving, without subsampling we'd bias toward the early samples.
            Collections.shuffle(sample);
            sample = sample.subList(0, (int)Math.ceil(retrived*sampleRate));
        }

        return sample;
    }

    public void close(){
        try{
            kafkaConsumer.commitSync();
        }catch(CommitFailedException e){
            System.out.println(e);
        }
        kafkaConsumer.close();
    }
}

class SingletonSampler extends SequentialSampler{
    public SingletonSampler(String d){
        super(d);
    }
    public Properties getStreamConfig() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //increase max bytes =>
        props.put("fetch.max.bytes", 10240); //
        //increase min bytes => bigger batch => larger throughput but higher latency (have to get more data to send)
        props.put("fetch.min.bytes", 1);
        props.put("max.partition.fetch.bytes", 10240);
        props.put("max.poll.records", 1);
        props.put("group.id", "DPASSSingletonSampler"+ThreadLocalRandom.current().nextInt());
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    public List<String> sampleFromTopic(int n){
        List<String> sample = new ArrayList<String>();
        if(partitionSizes.size() == 0) updatePartitionSize();
        setRandomOffset(kafkaConsumer.assignment());
        try{
            while(sample.size() < n){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(pollDuration);
                for(ConsumerRecord<String, String> record : records){
                    sample.add(record.value());
                    setRandomOffset(Arrays.asList(new TopicPartition(record.topic(),record.partition())));
                }
            }
        }catch(Exception e){
            System.out.println(e);
            e.printStackTrace();
        }
        return sample;
    }
}