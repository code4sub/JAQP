package compass;

import tech.tablesaw.api.*;
import java.util.*;
import org.junit.jupiter.api.*;
import java.util.stream.Stream;
import java.util.stream.Collector;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public class BackgroundSamplingTest {
    String topic = "intel-data";
    KafkaSampler sampler;
    Dataset dataset = Dataset.createDatasetByName("intelwireless");
    PATree partitionTree;

    void showTestName(TestInfo testInfo){
        String methodName = testInfo.getTestMethod().orElseThrow().getName();
        System.out.println("\n================================");
        System.out.println(methodName);
        System.out.println("================================");
    }

    @BeforeEach
    void setup(TestInfo testInfo){
        showTestName(testInfo);
        sampler = new KafkaSampler(topic);
    }

    @AfterEach
    void clean(){
        sampler.close(); //needed to avoid blocking when query partition size
    }

    @Test
    void SequentialSamplingTest(){
        Table sample;

        sampler.updatePartitionSize();
        Map<TopicPartition, Long> partitionSizes = sampler.getPartitionSizes();
        sampler.setSampleMode(SampleMode.SEQUENTIAL);
        sampler.resetOffset();
        long totalSizeFromKafka = partitionSizes.values().stream().reduce(0L, Long::sum);
        int totalSize = 0;
        try{
            while(true){
                sample = dataset.CSVStringsToTableWithoutHeader(sampler.sampleFromTopic(1024, 10));
                totalSize += sample.rowCount();
                if(sample.rowCount() == 0) break;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        assert(totalSize == totalSizeFromKafka);
    }

    @Test
    void RandomSamplingTest(){
        Optional<Table> sample = Optional.empty();
        System.out.println("Testing random sampling");

        sampler.setSampleMode(SampleMode.RANDOM);
        long startTime = System.currentTimeMillis();
        try{
            sample = Optional.of(dataset.CSVStringsToTableWithoutHeader(sampler.sampleFromTopic(1024, 10)));
            // System.out.println(sample.summary());
        }catch(Exception e){
            e.printStackTrace();
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        if(sample.isPresent())
            System.out.println(sample.get().rowCount() + " random sample drew in " + elapsedTime + " ms");

    }

    void buildMergeTree(){
        int k = 8;
        SamplePartitioner eqPartitioner = new EqualDepthPartitioner();
        System.out.println("Building MergeTree ...");
        sampler.setSampleMode(SampleMode.RANDOM);
        List<String> sampleStrings = sampler.sampleFromTopic(1024, 10);
        try{
            Table sample = dataset.CSVStringsToTableWithoutHeader(sampleStrings);
            System.out.println("Initial sample for partitioning: " + sample.shape());
            eqPartitioner.setSample(sample);
            List<String> predAttributes = Arrays.asList("itime");
            String aggrAttribute = "voltage";

            partitionTree = new MergeTree(topic, predAttributes, aggrAttribute, k);
            partitionTree.setPartitioner(eqPartitioner);
            partitionTree.buildTree();

            partitionTree.displayTree(false);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Test
    void CatchingUpTest(){
        System.out.println("Testing background catching up...");
        buildMergeTree();
        // partitionTree.displayTree();
        Optional<Table> sample = Optional.empty();
        long sampleSize = 0;
        for(int i = 0; i < 5; i++){
            // long startTime = System.currentTimeMillis();
            try{
                sample = Optional.of(dataset.CSVStringsToTableWithoutHeader(sampler.sampleFromTopic(1024, 10)));
                sampleSize += sample.get().rowCount();
                partitionTree.improveAndPropagate(sample.get(), true);
            }catch(Exception e){
                e.printStackTrace();
            }
            // long stopTime = System.currentTimeMillis();
            // long elapsedTime = stopTime - startTime;
            // if(sample.isPresent())
            //     System.out.println(sample.get().rowCount() + " random sample drew in " + elapsedTime + " ms");
            partitionTree.displayTree(false);
        }
        assert partitionTree.getRootCnt() == sampleSize: "Incorrect leaf propagation";

    }


}
