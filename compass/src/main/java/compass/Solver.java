package compass;

import tech.tablesaw.api.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.internals.Topic;

public abstract class Solver implements Consumer, Runnable{
    Dataset dataset;
    String dataTopic;
    long startTick;
    long stopTick;
    boolean preload = false;
    boolean debugQuery = false;
    String name;
    long populationSize;
    final int maxDataCacheSize = 1000;
    final int maxDeletionCacheSize = 1000;
    boolean isEager = false;
    Map<String, double[]> results;
    String resultPath;
    Random randGen;
    public boolean hasStopped = false;
    final int seed = 1234;

    public abstract void initialize();

    public String getResultPrefix(){
        return name; // + "_" + aggrAttribute + "-" + predAttributes;
    }

    public void cleanupAndExit(){
        hasStopped = true;
    }
    public void setDebugQuery(boolean b){
        debugQuery = b;
    }
    String getName(){
        return name;
    }

    public static void touch(File file) throws IOException{
        long timestamp = System.currentTimeMillis();
        touch(file, timestamp);
    }

    public static void touch(File file, long timestamp) throws IOException{
        if (!file.exists()) {
           new FileOutputStream(file).close();
        }

        file.setLastModified(timestamp);
    }
    public void handleQueryString(String s){

        Query q = new Query(s);
        long startTime = System.currentTimeMillis();
        QueryResult answers = solveQuery(q);
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        updateTock();
        addResult(q.getQueryString()+"="+q.getHash(), answers.asArrayWithTime(elapsedTime));
        if(debugQuery)
            System.out.println(String.format("%s\tQ%d %s", getName(), results.size(), answers));
    }

    public  synchronized void  addResult(String q, double[] answers){
        results.put(q, answers);
    }

    public void updateTock(){
        stopTick = System.currentTimeMillis();
    }

    public void handleDataString(String s){
        try{
            Table t = null;
            t = dataset.CSVStringsToTableWithoutHeader(new ArrayList<String> (Arrays.asList(s)));
            populationSize += t.rowCount();
            handleInsertion(t);
            updateTock();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void handleDeletionString(String s){
        try{
            Table t = dataset.CSVStringsToTableWithoutHeader(new ArrayList<String> (Arrays.asList(s)));
            populationSize -= t.rowCount();
            handleDeletion(t);
            updateTock();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void saveResults(){
        String fullPath = resultPath + "/" + getResultPrefix() + "-" + results.size() + ".csv";

        IntColumn c0 = IntColumn.create("index");
        StringColumn c1 = StringColumn.create("query");
        StringColumn c2 = StringColumn.create("qhash");
        DoubleColumn c3 = DoubleColumn.create("cnt");
        DoubleColumn c4 = DoubleColumn.create("sum");
        DoubleColumn c5 = DoubleColumn.create("avg");
        DoubleColumn c6 = DoubleColumn.create("min");
        DoubleColumn c7 = DoubleColumn.create("max");
        DoubleColumn c8 = DoubleColumn.create("time");
        int i = 0;
        for(Map.Entry<String, double[]> entry: results.entrySet()){
            c0.append(++i);
            c1.append(entry.getKey().split("=")[0]);
            c2.append(entry.getKey().split("=")[1]);
            double[] array = entry.getValue();
            c3.append(array[0]);
            c4.append(array[1]);
            c5.append(array[2]);
            c6.append(array[3]);
            c7.append(array[4]);
            c8.append(array[5]);
        }
        Table t = Table.create("results", c0, c1, c2, c3, c4, c5, c6, c7, c8);
        try{
            System.out.println("Saving " + t.rowCount() + " entries" + " to " + fullPath);
            t.write().csv(fullPath);
            results.clear();
        }catch(Exception e){
            System.out.println(e);
        }
    }

    public void setResultPath(String p){
        resultPath = p;
    }

    public abstract QueryResult solveQuery(Query q);

    public abstract void handleInsertion(Table t);
    public abstract void handleDeletion(Table t);

    public void setDataset(String ds){
        dataset = Dataset.createDatasetByName(ds, preload);
    }


    public void update(TopicMessage msg){
        processMessage(msg);
    }

    public Solver(){
        populationSize = 0;
        results = new HashMap<String, double[]>();
        name = this.getClass().getName();
        randGen = new Random(seed);
    }

    public long getStopTick(){return stopTick;}

    public void run(){}
    public void ticktock(String s){
        if(s.startsWith("t0")){
            startTick = System.currentTimeMillis();
            System.out.println("\n" + s + " clock start ticking... " + new java.util.Date());
        }else if(s.startsWith("t1")){
            System.out.println("WallClockTime (ms) elapsed since last tick vs now: " + (stopTick - startTick) + " " + (System.currentTimeMillis() - startTick));
            // System.out.println("WallClockTime from last tick till Now: " + (System.currentTimeMillis() - startTick)/1000 + "s");
        }
    }
    public void processMessage(TopicMessage msg){
        switch(msg.getMessageType()){
            case DATA:
                handleDataString(msg.getMessage());
                break;
            case QUERY:
                handleQueryString(msg.getMessage());
                break;
            case DELETION:
                handleDeletionString(msg.getMessage());
                break;
            case SAVE:
                saveResults();
                break;
            case TICK:
                ticktock(msg.getMessage());
                break;
            case EXIT:
                System.out.println(name + " Shutting down by EXIT cmd.");
                cleanupAndExit();
        }
    }
}

class SampleSolver extends Solver{
    Table sample;
    int maxSampleSize;
    List<String> predAttributes;
    String aggrAttribute;
    int catchUpSamplerBatchSize = 10240;
    long fullCatchUpSize;
    SequentialSampler sequentialSampler;
    String getName(){
        return name;
    }
    public void initialize(){}
    public SampleSolver(){}

    public SampleSolver(String data,
                            List<String> predAttributes,
                            String aggrAttribute,
                            int size){
        super();
        this.predAttributes = predAttributes;
        this.aggrAttribute = aggrAttribute;
        sample = null;
        dataTopic = data;
        maxSampleSize = size;
        sequentialSampler = new SequentialSampler(dataTopic);
    }

    public String getResultPrefix(){
        return name + "_" + StringUtils.join(predAttributes, "-") + "_" + aggrAttribute+"_"+populationSize;
    }

    public void handleInsertion(Table t){
        System.out.println("TODO: handle insertion");
    }
    public void handleDeletion(Table t){
        System.out.println("TODO: handle deletion");
    }

    public QueryResult solveQuery(Query q){
        QueryResult ret = solveQueryWithSample(q, sample, populationSize);
        return ret;
    }

    public long getBackgroundSampleSize(boolean b){
        if(sample == null) return 0;
        return sample.rowCount();
    }
    public void postCatchUpProcess(boolean b){}
    public void processCatchUpBatch(Table batch, boolean b){
        if(sample == null)
            sample = batch;
        else
            sample.append(batch);
    }

    public void catchUp(String topic, float catchUpRate, SequentialSampler sampler){
        System.out.println(sampler.getClass().getName() + ": catching up..." + catchUpSamplerBatchSize + " per batch");
        long startTime = System.currentTimeMillis();

        //this flag control the sequential mode,
        //if enabled PASS will batch the random samples to process in postCatchUpProcess
        //! otherwise, each batch will be processed on the fly, this is not correctly done yet for singletone sampler.

        boolean isSequential = false; //
        isSequential = sampler instanceof SingletonSampler == false;
        if(isSequential)sampler.setSampleRate(catchUpRate);
        try{
            fullCatchUpSize = sampler.getTotalPartitionSize();
            System.out.println("Catching up goal: " + catchUpRate + "*" + fullCatchUpSize +"="+ fullCatchUpSize*catchUpRate);
            populationSize = fullCatchUpSize;
            Table batch;
            // List<String> batch;
            sampler.resetOffset();
            while(getBackgroundSampleSize(isSequential) < fullCatchUpSize*catchUpRate){
                List<String> records = sampler.sampleFromTopic(catchUpSamplerBatchSize);
                if(records.size() == 0){ System.out.print("0.\r");}
                else{
                    batch = dataset.CSVStringsToTableWithoutHeader(records);
                    processCatchUpBatch(batch,isSequential);
                }
                System.out.print(getBackgroundSampleSize(isSequential) +"/" + (int)(fullCatchUpSize*catchUpRate) + "\r");
            }
            long stopTime = System.currentTimeMillis();
            long elapsedTime1 = stopTime - startTime;
            System.out.println("Post processing...");
            startTime = System.currentTimeMillis();
            postCatchUpProcess(isSequential);
            sampler.close();
            stopTime = System.currentTimeMillis();
            long elapsedTime2 = stopTime - startTime;
            System.out.println("retrived "+getBackgroundSampleSize(isSequential)+" tuples in " + elapsedTime1 + "ms; " + sampler.getClass().getName() + "; " + catchUpRate);
            System.out.println("processing done in " + elapsedTime2 + "ms; " + sampler.getClass().getName() + "; " + catchUpRate);
            System.out.println(name + ": total catchup time: "
                                +(elapsedTime1 + elapsedTime2)
                                + " ms\n====================\n");
            touch(new File("/tmp/"+this.getClass().getName()));
        }catch(Exception e){
            e.printStackTrace(); //getStackTrace());
        }
    }

    public QueryResult solveQueryWithSample(Query q, Table sample, double populationSize){
        long startTime = System.currentTimeMillis();
        if(sample.rowCount() == 0 || populationSize == 0){
            System.out.println("Invalid sample solver input: sampleSize="+sample.rowCount() + ", populationSize:" + populationSize);
            return null;
            // System.exit(1);
        }
        float ratio = (float)(sample.rowCount()/populationSize);
        Table t = sample;
        for(Map.Entry<String, Map.Entry<Double, Double>> entry: q.predicates.getRectangles().entrySet()){
            double l = entry.getValue().getKey();
            double r = entry.getValue().getValue();
            //queries are half closed
            t = t.where(t.numberColumn(entry.getKey()).isGreaterThanOrEqualTo(l));
            t = t.where(t.numberColumn(entry.getKey()).isLessThan(r));
            if(t.rowCount() == 0){
                break;
            }
        }
        // System.out.println(name + ", ratio: "+ratio + ", " + sample.rowCount() + ", "+populationSize + ", filtered: " + t.rowCount());
        // long stopTime = System.currentTimeMillis();
        // long elapsedTime = stopTime - startTime;
        // System.out.println(name + " solved in " + elapsedTime + "ms");
        double cnt = t.rowCount()/ratio;
        double sum = t.numberColumn(q.aggrAttribute).sum()/ratio;
        double min = t.numberColumn(q.aggrAttribute).min();
        double max = t.numberColumn(q.aggrAttribute).max();
        return new QueryResult(cnt, sum, min, max);
    }
}
