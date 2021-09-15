package compass;

import tech.tablesaw.api.*;
import java.util.*;

import java.util.concurrent.*;

import org.apache.commons.math3.stat.descriptive.moment.Variance;

public class VarianceEstimator {
    List<Double> prefixSum;
    List<Double> prefixSquaredSum;
    long totalSize;
    String predAttr;
    String targetAttr;
    boolean foundBest;
    double nmRatio;
    double lowerBoundVar;
    double upperBoundVar;
    double maxVarianceFromSearch = Double.MAX_VALUE;
    Map<String, Double> maxVarCache;
    float theta; //evaluate n^2*theta queries for variance estimation.
    int minSamplePerBucket = 999999;

    Table sample;
    public void setSample(Table s){sample = s;}

    public boolean foundBestPartition(){
        return foundBest;
    }

    public VarianceEstimator(){
    }

    double getIntervalVariance(int ith, int jth){
        int nSampleQueries = 1024;
        int n_i = jth - ith + 1;
        double maxVar = -1.0;
        String key = ith + "," + jth;
        if(prefixSum.size() == 0){
            // System.out.println("Uninitialized estimator, return 0.0");
            return 0.0;
        }
        if(maxVarCache.containsKey(key)){
            return maxVarCache.get(key);
        }
        boolean first = false;
        for(int i = ith; i <= jth - minSamplePerBucket + 1; i++){
            for(int j = jth; j >= i+minSamplePerBucket-1; j--){
                if(first == false && ThreadLocalRandom.current().nextFloat() > theta)
                    continue;
                first = false;
                if(nSampleQueries < 0) break;
                nSampleQueries -= 1;
                double sumSquare = prefixSquaredSum.get(j);
                double squaredSum = prefixSum.get(j);
                if(i > 0){
                    sumSquare -= prefixSquaredSum.get(i-1); //both inclusive.
                    squaredSum -= prefixSum.get(i-1);
                }
                squaredSum = squaredSum*squaredSum;

                //revise to CNT/SUM fomula in arxiv paper sec. A.2
                double var = (nmRatio*nmRatio*sumSquare - squaredSum/n_i);

                maxVar = Math.max(var, maxVar);

            }
        }

        if(maxVar < 0)
            maxVar = lowerBoundVar;
        maxVarCache.put(key, maxVar);
        return maxVar;
    }

    void preprocess(){
        maxVarCache = new HashMap<String, Double>();
        prefixSum = new ArrayList<Double>();
        prefixSquaredSum = new ArrayList<Double>();
        sample = sample.sortAscendingOn(predAttr);
        // minSamplePerBucket = new Double(10*Math.log(sample.rowCount())).intValue();

        DoubleColumn col = sample.doubleColumn(targetAttr);
        if(col.size() == 0){
            // System.out.println("Empty sample");
            return;
        }
        nmRatio = totalSize/col.size();
        for(int i = 0; i < col.size(); i++){
            double v = col.get(i);
            if(i > 0){
                prefixSum.add(v + prefixSum.get(i-1));
                prefixSquaredSum.add(v*v + prefixSquaredSum.get(i-1));
            }else{
                prefixSum.add(v);
                prefixSquaredSum.add(v*v);
            }
        }
    }

    public void initialize(long totalSize, String predAttr,
                            String targetAttr, float theta,
                            int minSamplePerBucket){
        this.predAttr = predAttr;
        this.targetAttr = targetAttr;
        this.totalSize = totalSize;
        this.theta = theta;
        this.minSamplePerBucket = minSamplePerBucket;
        preprocess();
    }
}

class VarianceEstimatorND {
    Map<String, VarianceEstimator> estimators;
    Table sample;
    List<String> predAttrs;
    String targetAttr;
    long totalSize;
    int minSamplePerBucket;
    float theta;
    public void setSample(Table s){
        sample = s;
    }

    public VarianceEstimatorND(){}
    public void initialize(long totalSize, List<String> predAttrs,
                                String targetAttr, float theta,
                                int minSamplePerBucket){
        this.estimators = new HashMap<String, VarianceEstimator>();
        this.predAttrs = predAttrs;
        this.targetAttr = targetAttr;
        this.totalSize = totalSize;
        this.theta = theta;
        this.minSamplePerBucket = minSamplePerBucket;
        for(String attr: predAttrs){
            VarianceEstimator est =  new VarianceEstimator();
            est.setSample(sample);
            est.initialize(totalSize, attr, targetAttr, theta, minSamplePerBucket);
            estimators.put(attr, est);
        }
    }


    public void resetSample(Table sample){
        for(String attr: predAttrs){
            VarianceEstimator est = estimators.get(attr);
            est.setSample(sample);
            est.preprocess();
        }
    }

    public double getIntervalVariance(int ith, int jth){
        double varSum = 0.0;
        double maxVar = 0.0;
        for(String attr: predAttrs){
            double var = estimators.get(attr).getIntervalVariance(ith, jth);
            maxVar = Math.max(var, maxVar);
            varSum += var;
        }
        //add a random float to avoid same var in heap
        return maxVar + ThreadLocalRandom.current().nextFloat();
        // return varSum/predAttrs.size() + ThreadLocalRandom.current().nextFloat();
    }
}