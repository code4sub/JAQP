package compass;
import tech.tablesaw.api.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.lang3.StringUtils;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.*;

public class PASS extends SampleSolver{
    SamplePartitioner partitioner;
    int nPartitions;
    List<String> predAttributes;
    String aggrAttribute;
    String dataTopic;
    PATree partitionTree;
    float catchUpRate; //0-1; 1 for fullpass.
    long fullCatchUpSize; //population size of historical/existing data.
    final int samplerBatchSize = 1024;
    final int catchUpSamplerBatchSize = 10240;
    int sampleSize4Partition = 0; //2048;
    int nStrataBatches = 10;
    int sampleSizePerBucket;
    int overSampleX;
    Table catchUpSample = null;
    ReadWriteLock readWriteLock;
    ExecutorService executor;
    boolean isReady = false; //is ready to handle query
    Queue<Query> queryQueue;
    float partitionSampleRate = 0.005f;
    float reservoirRatio; //the size of reservoir as a ratio of existing data.
    int reservoirCap;
    boolean isAsync = false;
    AtomicLong tupleCounter = new AtomicLong(0); //exist + new insertion - deletion, i.e. the total number of tuples we have seen so far.
    SequentialSampler sequentialSampler  = null;
    SequentialSampler singletonSampler = null;
    public PASS(String topic, boolean async,
                List<String> predAttrs,
                String aggrAttr, int k,
                int bucketSize,
                SamplePartitioner p, float catchUp,
                float reservoirRatio, int x,
                boolean preload){
                    this(topic, async, predAttrs, aggrAttr, k, bucketSize, p, catchUp, reservoirRatio, x);
                    this.preload = preload;
            }
    public PASS(String topic, boolean async,
                List<String> predAttrs,
                String aggrAttr, int k,
                int bucketSize,
                SamplePartitioner p, float catchUp,
                float reservoirRatio, int x
                ){
        this.reservoirRatio = reservoirRatio;
        overSampleX = x;
        nPartitions = k;
        this.preload = true;
        predAttributes = predAttrs;
        aggrAttribute = aggrAttr;
        isAsync = async;
        dataTopic = topic;
        partitioner = p;
        catchUpRate = catchUp;
        sampleSizePerBucket = bucketSize;
        sampleSize4Partition = (int)(sampleSizePerBucket*k*1.2); //10log(3000) - log(8000) gives roughly 80
        isEager = true; //process data string eagerly
        sequentialSampler = new SequentialSampler(topic);
        singletonSampler = new SingletonSampler(topic);

        if(isAsync){
            executor = Executors.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors(), (Runnable r)->{
                        Thread t = new Thread(r);
                        t.setDaemon(true);
                        return t;});
            System.out.println("Executor initialized");
        }

        System.out.println("Async mode: " + isAsync);
        System.out.println("Registering shutdown hook.");
        System.out.println(name + " Reservoir ratio (i.e. sample rate): " + reservoirRatio + ", OverSampleX: " + overSampleX + ", Catch-up ratio: " + catchUpRate + ", Leaf layer size: " + nPartitions);
    }

    public PATree getTree(){
        return partitionTree;
    }

    public String getResultPrefix(){
        return name + "_" + StringUtils.join(predAttributes, "-") + "_" + aggrAttribute + "_" + nPartitions + "-" + overSampleX + "X-" + reservoirRatio + "-" + reservoirCap + "-" + catchUpRate + "-" + tupleCounter.get();
    }

    public long getCatchUpSize(){
        return (long)(catchUpRate * fullCatchUpSize);
    }

    public boolean shouldPartition(){
        return false;
    }

    public void handleDeletion(Table deletion){
        /*
        ! No way to handle double deletion unless we keep a copy of all deletions
        ! for now assume that's not our responsibility (there's no way a user can send a deletion request for an tuple that is already deleted)
        ! but it is easy to handle anyways if deletions can be efficiently accessed.
        */
        tupleCounter.getAndAdd(-deletion.rowCount());
        partitionTree.updateReservoirAndStatsForDeletion(deletion);
        // System.out.println(name + " -= " + tupleCounter);
    }

    public void handleInsertion(Table insertion){
        //this is actually handle 1 insertion, can be the primitive to handle multiple
        // System.out.println("Insertion\n"+insertion.print());
        tupleCounter.getAndAdd(insertion.rowCount());

        long nth = ThreadLocalRandom.current().nextLong(tupleCounter.get());
        // System.out.println(nth + "; " + reservoirCap + ";" + tupleCounter);
        // update reservoir, too
        reservoirCap = (int)(tupleCounter.get() * reservoirRatio);
        if(partitionTree.getReservoirSize() < reservoirCap) nth = -1;
        if(nth < reservoirCap){
            partitionTree.updateReservoirAndStatsForInsertion((int)nth, insertion);
        }else{
            // update stats only
            partitionTree.updateStatsForInsertion(insertion);
        }

        // System.out.println(name + " += " + tupleCounter);
    }

    public void offlineInitialization(){
        try{
            Table strata = null;
            SequentialSampler sampler = singletonSampler;
            fullCatchUpSize = sampler.getTotalPartitionSize(); //size of existing data
            reservoirCap = (int)(reservoirRatio*fullCatchUpSize*overSampleX);
            maxSampleSize = (int)(reservoirRatio*fullCatchUpSize);
            tupleCounter.getAndAdd(fullCatchUpSize);
            Table partitionSample = null;
            List<String> sampleStrings = new ArrayList<String>();
            // sampleSize4Partition = Math.max((int)(partitionSampleRate*fullCatchUpSize), sampleSize4Partition);
            System.out.println(sampler.getClass().getName() + " collecting sample for partitioning");
            while(sampleStrings.size() < sampleSize4Partition){
                sampleStrings.addAll(sampler.sampleFromTopic(sampleSize4Partition));
                System.out.print(""+sampleStrings.size()+"/"+sampleSize4Partition+"\r");
            }
            partitionSample = dataset.CSVStringsToTableWithoutHeader(sampleStrings).first(sampleSize4Partition);

            System.out.println("\nInitial sample for partitioning: " + partitionSample.shape());
            partitioner.setSample(partitionSample);

            if(partitioner instanceof NBPartitioner){
                ((NBPartitioner)partitioner).initialize(
                    fullCatchUpSize, predAttributes.get(0),
                    aggrAttribute, 0.1f, sampleSizePerBucket, //theta
                    dataset.getLowerBound(aggrAttribute),
                    dataset.getUpperBound(aggrAttribute)
                );
            }

            if(predAttributes.size() == 1){
                //build a merge tree for 1D
                System.out.println("Building a merge tree for 1 dimension.");
                partitionTree = new MergeTree(dataTopic, predAttributes, aggrAttribute, nPartitions);
                partitionTree.setPartitionSample(partitionSample);
                partitionTree.setPartitioner(partitioner);
                partitionTree.buildTree();
            }else{
                //build a split tree for nD
                partitionTree = new KDTree(dataTopic, predAttributes, aggrAttribute, nPartitions);
                SamplePartitioner md = new MedianPartitioner();
                partitionTree.setPartitioner(md);
                partitionTree.setPartitionSample(partitionSample);
                partitionTree.initialize(fullCatchUpSize, 0.1f, sampleSizePerBucket, partitionSample);
                partitionTree.buildTree();
            }

            System.out.println(sampler.getClass().getName() + ": partition tree built, building strata, ratio: " + reservoirRatio*100 + "% (" +overSampleX + "X = " + reservoirCap + ")");
            int reservoirSize = 0;
            while(reservoirSize < reservoirCap){
                strata = dataset.CSVStringsToTableWithoutHeader(sampler.sampleFromTopic(samplerBatchSize));
                if(strata.rowCount() + reservoirSize > reservoirCap)
                    strata = strata.first(reservoirCap - reservoirSize);
                System.out.print(""+reservoirSize+"/"+reservoirCap+"\r");
                if(strata.rowCount() == 0) continue;
                reservoirSize += strata.rowCount(); //counting w/o header
                partitionTree.buildStrata(strata);
            }
            System.out.println("\nStrata (Reservoir) built: " + reservoirSize + "/" + reservoirCap);
        }catch(Exception e){
            e.printStackTrace(); //getStackTrace());
        }
    }

    public void initialize(){
        System.out.println("==>\n" + name + " initializing...");
        offlineInitialization();
    }

    public long getBackgroundSampleSize(boolean isSequential){
        if(isSequential){
            if(catchUpSample != null)return catchUpSample.rowCount();
            return 0;
        }
        return partitionTree.getBackgroundSampleSize();
    }

    public void processCatchUpBatch(Table batch, boolean isSequential){
        if(isSequential){
            if(catchUpSample == null){
                catchUpSample = batch;
            }else{
                catchUpSample.append(batch);
            }
        }else
            partitionTree.improveLeafStats(batch, true);
    }

    public void postCatchUpProcess(boolean isSequential){
        isReady = true;
        if(isSequential){
            System.out.println("Catch up size: " + catchUpSample.rowCount());
            partitionTree.improveLeafStats(catchUpSample, true);
        }
        if(catchUpRate < 1.0f){
            partitionTree.adjustCatchUpSummary(fullCatchUpSize);
        }
        if(debugQuery)
            partitionTree.showStrata();
    }


    public void handleQueryString(String s){
        if(isAsync)
            CompletableFuture.runAsync(()->threadSolveQuery(new Query(s)), executor).thenRun(()->updateTock());
        else{
            threadSolveQuery(new Query(s));
            updateTock();
        }
    }

    void threadSolveQuery(Query q){
        long startTime = System.currentTimeMillis();

        QueryResult r = solveQuery(q);

        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;

        addResult(q.getQueryString()+"="+q.getHash(), r.asArrayWithTime(elapsedTime));

        if(debugQuery)
            System.out.println(String.format("%s\t Q%d %s", getName(), results.size(), r));
    }

    public void run(){
        if(isAsync){
            //this let us handle insertion/deletion during catchup
            CompletableFuture.runAsync(()->catchUp(dataTopic, catchUpRate, sequentialSampler), executor);
        }else{
            catchUp(dataTopic, catchUpRate, sequentialSampler);
        }
    }

    public QueryResult solveQuery(Query q){
        try{
            QueryResult r = null;
            if(q.getNDimensions() == 1)
                r = solveByHybrid1D(q);
            else
                r = solveByHybridND(q);
            boolean checkError = false;
            if(checkError){
                System.out.println("Ground truth w/ " + sample.rowCount() + " samples");
                QueryResult gt = solveQueryWithSample(q, sample, sample.rowCount());
                double error = Math.abs(r.cnt - gt.cnt) / gt.cnt;
                if(error > 0.9){
                    System.out.println(">>> Error " + String.format("%.2f%%", error*100) + "PASS:" + q + "=" + r);
                    try{Thread.sleep(30000L);}catch(Exception e){}
                }else{
                    System.out.println("Error: " + String.format("%.2f%%", error*100));
                }
            }
            return r;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public QueryResult solveByHybrid1D(Query q){
        // System.out.println("Solving 1D query: " + q);
        // find relevant nodes via binary search.
        List<Integer> ids = partitionTree.findNodeIdRangeForQuery1D(q);
        // System.out.println("Left most node: " + ids.get(0) + "; right most node: " + ids.get(1));

        // * rPartial could be solved the same way as we did for multi-dimension.But in 1D, n_paritial is at most 2 and n_covered can be >> 2. We want to skip the samples from covered nodes for performance, too, not just for accuracy.

        // summarize stats for covered nodes
        int left = ids.get(0), right = ids.get(1);
        QueryResult rCovered = new QueryResult();
        QueryResult rPartial = new QueryResult();
        Table allSamples = null;
        double populationSize = 0;
        int nPartial = 0, nCover = 0;
        for(int i = left; i <= right; i++){
            PANode node = partitionTree.getNthLeafNode(i);
            boolean isCovered = true;
            if(i == left || i == right){
                //check if it is a partial node or not
                HyperRectangle intersection = node.getIntersection(q.getPredicates()); //right open,
                if(intersection != null){
                    Query subQuery = q.getSubquery(intersection);
                    // System.out.println("SubQuery: " + intersection + " node:" + node);
                    isCovered = node.coveredBy(subQuery.getPredicates());
                    if(!isCovered){
                        nPartial += 1;
                        if(allSamples == null){
                            allSamples = node.getStratumCopy();
                        }else{
                            allSamples.append(node.getStratum());
                        }
                        populationSize += node.getSummaryCount(true) + node.getSummaryCount(false);
                        // System.out.println("Partial node stratum size:" + node.getStratumSize() + ", populationSize: " + populationSize);
                        //* partial node, solve subquery by sample in strata.
                        continue;
                    }
                }else{
                    continue; // right node might be empty, we locate the right boarder to the next node but the intersection is null due to right open,
                }
            }
            nCover++;
            //one for catchup summary; one for online summary
            // System.out.println("Cover node:" + node);
            double[] stats = node.getSummaryStatsAsArray(true);
            if(stats != null)
                rCovered.merge(stats);
            stats = node.getSummaryStatsAsArray(false);
            if(stats!=null)
                rCovered.merge(stats);
        }
        if(allSamples != null && allSamples.rowCount() > 0){
            // System.out.println(name + " #Available sample: " + allSamples.rowCount() + " vs budget (maxSamplesize): " + maxSampleSize);
            if(allSamples.rowCount() > maxSampleSize){
                allSamples = allSamples.sampleN(maxSampleSize);
            }
            rPartial = solveQueryWithSample(q, allSamples, populationSize);
        }
        // System.out.println("Partial: " + nPartial);
        // System.out.println("Covered: " + nCover);
        rPartial.merge(rCovered.asArray());
        // System.out.println("Solving 1D query " + q + " => " + rPartial);
        return rPartial;
    }

    public QueryResult solveQueryByNodeStratum(Query q, PANode node){
        double populationSize = node.getSummaryCount(true) + node.getSummaryCount(false);
        // double ratio = (double)node.getStratumSize()/populationSize;
        return solveQueryWithSample(q, node.getStratum(), populationSize);
    }

    public QueryResult solveByHybridND(Query q){


        // System.out.println(name + " solving " + q.getNDimensions() + "D query: " + q);
        /*
        To solve a multi-dimensional query.
         1. Find a set of leaf nodes covered or intersecting with the query.
         1.1 Because the leaf layer is linked together, we only need to keep expand the partially covered node until we reach the leaf layer on two nodes. (one node is a trivial special case)
         2. Classify the nodes
         2.1 Iterate through the left most leaf node until the right most node, we have a set (denote by S_lr) of leaves. Note some node in S_lr might be irrelevant with q (depends on how the leaves are linked, e.g. can be a zipzap link).
         2.2 We check each node against the query, if it is relevant, we label it either as covered or partial.
         3. Solve a query
         3.1 Let q_partial and q_cover be the subqueries that are partially or fully covered. Solving q_cover is easy. We just need to aggregate the summaries of all the covered nodes to get r_covered.
         3.2 To solve q_partial, in static PASS, we extra each partial query and merge them together then solve with samples from partial nodes only. In this implementation we take another route by using only samples from partial nodes to solve q to get r_partial.
         3.3 Finally, we merge r_cover and r_partial as the final answer.
         */
        List<PANode> nodes = partitionTree.findNodesForQuery(q);
        HyperRectangle qhr = q.getPredicates();
        double populationSize = 0;
        // System.out.println(nodes.size() + " potential relevant nodes.");
        QueryResult rPartial = null, rCovered = null;
        Table allSamples = null;
        int[] counts = {0,0,0};
        int count = 0;
        for(PANode node: nodes){
            if(node.coveredBy(qhr)){
                if(rCovered == null) rCovered = new QueryResult();
                // System.out.println("Covered node: " + node.getPredicates());
                double[] stats = node.getSummaryStatsAsArray(true);
                if(stats != null)
                    rCovered.merge(stats);
                stats = node.getSummaryStatsAsArray(false);
                if(stats!=null)
                    rCovered.merge(stats);
                counts[0] += 1;
            }else if(node.getPredicates().intersects(qhr)){
                // System.out.println("Partial node: " + node.getPredicates());
                if(node.getStratum() == null) continue;
                if(allSamples == null)
                    allSamples = node.getStratumCopy();
                else{
                    allSamples.append(node.getStratum());
                }
                counts[1] += 1;
                populationSize += node.getSummaryCount(true) + node.getSummaryCount(false);
            }else{
                // System.out.println("Irrelevant node: " + node.getPredicates());
                counts[2] += 1;
                continue;
            }
        }
        // System.out.println("Counts: covered: " + counts[0] + " vs partial: " + counts[1] + "  vs irrelevant: " + counts[2]);
        if(allSamples != null){
            // System.out.println(name + " #Available sample: " + allSamples.rowCount() + " vs budget (maxSamplesize): " + maxSampleSize);
            if(allSamples.rowCount() > maxSampleSize){
                allSamples = allSamples.sampleN(maxSampleSize);
            }
            // System.out.println("#Sample used: " + allSamples.rowCount());

            rPartial = solveQueryWithSample(q, allSamples, populationSize);
        }else{
            rPartial = new QueryResult();
        }
        if(rCovered != null){
            rPartial.merge(rCovered.asArray());
        }
        // System.out.println("Solved " + q.getNDimensions() + "D query: " + q + " => " + rPartial);
        return rPartial;
    }
    public void setDataset(String ds){
        dataset = Dataset.createDatasetByName(ds, preload);
    }

    public void randomStratumDelete(){
        partitionTree.showStrata();
        partitionTree.randomStrataEvict(0.1f);
    }

    public void cleanupAndExit(){
        while(System.currentTimeMillis() - startTick < 1000){
            System.out.print(name + "Still working\r");
        }
        if(executor != null){
            System.out.println("PASS: shutting down executor...");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                if (!executor.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
                }
            } catch (InterruptedException ie) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        hasStopped = true;
    }

    // 10k insertion: 16s async vs 42s sync vs 16s without actually doing the heavy lifting of updating (just parsing csv and atomiclong inc, 31s for single thread)
    public void processMessage(TopicMessage msg){
        if(isAsync == false){
            //single thread processing
            super.processMessage(msg);
            return;
        }
        switch(msg.getMessageType()){
            case DATA:
                CompletableFuture.runAsync(()->handleDataString(msg.getMessage()), executor).thenRun(()->updateTock());
                break;
            case QUERY:
                if(!isReady){
                    System.out.println("Still catching up, not yet ready to process query.");
                }else
                    handleQueryString(msg.getMessage());
                break;
            case DELETION:
                CompletableFuture.runAsync(()->handleDeletionString(msg.getMessage()), executor).thenRun(()->updateTock());
                break;
            case RNDELETE:
                randomStratumDelete();
                break;
            case SAVE:
                saveResults();
                break;
            case TICK:
                ticktock(msg.getMessage());
                break;
            case EXIT:
                ticktock("t1");
                System.out.println("\n" + name + " Shutting down by EXIT cmd.");
                cleanupAndExit();
                break;
        }
    }

}
