package compass;

import java.util.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import static java.util.stream.Collectors.joining;

class SamplerPerfExperiment{
    static void perfTest(String dataTopic, boolean singleton, float sampleRate){
        System.out.println(dataTopic);
        Random randGen = new Random(1234);
        Duration pollDuration = Duration.ofMillis(100);
        Map<TopicPartition, Long> partitionSizes;
        KafkaConsumer<String, String> sampler = null;
        if(singleton){
            sampler = new SingletonSampler(dataTopic).getKafkaConsumer();
        }else{
            sampler = new SequentialSampler(dataTopic).getKafkaConsumer();
        }
        partitionSizes = new HashMap<TopicPartition, Long>();
        sampler.subscribe(Arrays.asList(dataTopic));
        sampler.poll(0);
        sampler.seekToEnd(sampler.assignment());
        for(TopicPartition tp: sampler.assignment()){
            System.out.println(tp + " size: " + sampler.position(tp)); // + " position: " + kafkaConsumer.position(tp));
            partitionSizes.put(tp, sampler.position(tp));
        }
        sampler.seekToBeginning(sampler.assignment());
        if(singleton)
            System.out.println("Singleton sampler");
        else
            System.out.println("Sequential sampler");


        int cs = 0;
        long startTime = System.currentTimeMillis();
        int nbatch = 0;
        int goal = 2219800;
        long offset = 0;
        TopicPartition tp = sampler.assignment().iterator().next();
        List<String> sample = new ArrayList<String>();
        // float sampleRate = 0.5f;
        while(sample.size() < goal*sampleRate){
            ConsumerRecords<String, String> records = sampler.poll(pollDuration);
            System.out.print(records.count()+"\r");
            cs += records.count();
            for(ConsumerRecord<String, String> record : records){
                if(singleton || ThreadLocalRandom.current().nextFloat() <= sampleRate+0.01){
                    sample.add(record.value()+Dataset.getStringHash(record.value()));
                }
            }
            nbatch += 1;
            if(singleton){
                offset = Math.abs(randGen.nextLong()) % partitionSizes.get(tp);
                sampler.seek(tp, offset);
            }
            System.out.print(sample.size() + "\r");
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("\nSampled  "+sample.size()+" in " + elapsedTime + "ms; " +nbatch+ " polls; " + 1.0f*elapsedTime/nbatch + " ms/poll");
        sampler.close();
    }
    public static void main(String[] args){
        String strDataTopic = args[0];
        for(float sr: Arrays.asList(0.1f, 0.2f, 0.3f, 0.4f, 0.5f)){
            // perfTest(strDataTopic, true, sr); //singleton
            perfTest(strDataTopic, false, sr); //sequential
        }
    }
}