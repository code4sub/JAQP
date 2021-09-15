package compass;
import java.util.*;
import tech.tablesaw.api.*;
import java.util.concurrent.*;

import org.apache.commons.lang3.StringUtils;

class ReservoirSampling extends SampleSolver{
    //https://en.wikipedia.org/wiki/Reservoir_sampling#An_optimal_algorithm
    float sampleRate;
    private Set<String> hashes = new HashSet<String>();
    public ReservoirSampling(String s,
                            List<String> predAttributes,
                            String aggrAttribute,
                            float sampleRate){
        super(s, predAttributes, aggrAttribute, -1);
        this.sampleRate = sampleRate;
    }

    public String getResultPrefix(){
        return name + "_" + StringUtils.join(predAttributes, "-") + "_" + aggrAttribute + "-" + sample.rowCount() + "-" + sampleRate + "-" + populationSize;
    }
    public void initialize(){
        //sequential catch up
        System.out.println("==>\nReservoirSampling solver initializing...");
        catchUp(dataTopic, 1.0f, sequentialSampler);
        maxSampleSize = (int)(sample.rowCount()*sampleRate);
        System.out.println(name + ": population size (catchUpSize): " + sample.rowCount()
                            + ", sampleRate: " + sampleRate
                            + ", maxSampleSize: " + maxSampleSize);
        sample = sample.sampleN(maxSampleSize);
        for(String hash: sample.stringColumn(Dataset.getHashColumnName())){
            hashes.add(hash);
        }
        System.out.println("Sample size:" + sample.rowCount() + "\n");
    }

    public void handleDeletion(Table deletion){
        String hashToDelete = deletion.stringColumn(Dataset.getHashColumnName()).get(0);
        if(hashes.contains(hashToDelete)){
            hashes.remove(hashToDelete);
            sample = sample.dropWhere(
                sample.stringColumn(Dataset.getHashColumnName()).isEqualTo(hashToDelete)
                );
        }
    }

    public void handleInsertion(Table newTable){
        maxSampleSize = (int)(populationSize * sampleRate);
        if (sample == null){
            sample = newTable;
        }else{
            if (sample.rowCount() < maxSampleSize){
                sample = sample.append(newTable);
                for(String hash: newTable.stringColumn(Dataset.getHashColumnName())){
                    hashes.add(hash);
                }
            }else{
                int nth = (int)ThreadLocalRandom.current().nextLong(populationSize);
                if(nth < sample.rowCount()){
                    try{
                        int[] drop = {(int)nth};
                        sample = sample.dropRows(drop);
                        sample.append(newTable);
                        System.out.print("ReservoirReplace..." + nth +", "+ sample.rowCount()+ "," + populationSize + "\r");
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}

class GroundTruthSolver extends SampleSolver{
    public GroundTruthSolver(String s,
                            List<String> predAttributes,
                            String aggrAttribute,
                            int size){
        super(s, predAttributes, aggrAttribute, size);
    }

    public String getResultPrefix(){
        return name + "_" + StringUtils.join(predAttributes, "-") + "_" + aggrAttribute + "-" + sample.rowCount();
    }
    public void handleDeletion(Table deletion){
        String hashToDelete = deletion.stringColumn(Dataset.getHashColumnName()).get(0);
        // System.out.println("Deleting: " + deletion);
        // System.out.println(sample.last(3));
        sample = sample.where(
            t->t.stringColumn(Dataset.getHashColumnName()).isNotEqualTo(hashToDelete)
            );
        populationSize = sample.rowCount();
        System.out.println(name + " -= " + populationSize);
    }

    public void initialize(){
        //sequential catch up
        System.out.println("==>\nGround truth solver initializing...");
        catchUp(dataTopic, 1.0f, sequentialSampler);
    }
    public QueryResult solveQuery(Query q){
        return solveQueryWithSample(q, sample, sample.rowCount());
    }

    public void handleInsertion(Table newTable){
        if (sample == null){
            sample = newTable;
            return;
        }
        // System.out.println("Inserting:" + newTable);
        sample = sample.append(newTable);
        populationSize = sample.rowCount();
        // System.out.println(name + " += " + sample.rowCount());
    }
}

class FirstKSampling extends SampleSolver{
    // public FirstKSampling(int size){
    //     super(size);
    // }
    public void handleDeletion(Table t){
        //TODO
    }
    public void initialize(){}

    public void handleInsertion(Table newTable){

        if (sample == null){
            sample = newTable;
        }else{
            sample = sample.append(newTable);
        }

        if(sample.rowCount() > maxSampleSize)
            sample = sample.inRange(maxSampleSize);

    }
}
