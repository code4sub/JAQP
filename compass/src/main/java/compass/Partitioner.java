package compass;

import tech.tablesaw.api.*;
import java.util.*;

//partitioner here is for 1D, we use KD tree for higher dimension, which does not require a partitioner to build tree top down.
public abstract class Partitioner{
    public abstract List<HyperRectangle> partitionTo(int k, String predAttribute);

    public List<HyperRectangle> splitData1D(PANode node, Table sample, String attr, boolean split){
        return null;
    }

    //cross product of each dimension
    private void _splitDataND(PANode node, Table sample, List<String> attrs, int ith,
                            HyperRectangle hr, List<HyperRectangle> hrs,
                            int ithOnly){
        if(ith == attrs.size()){
            hrs.add(new HyperRectangle(hr));
            return;
        }

        String attr = attrs.get(ith);
        boolean split = true;
        if(ithOnly >= 0) split = (ith == ithOnly);
        List<HyperRectangle> aHrs = splitData1D(node, sample, attr, split);
        for(HyperRectangle ahr: aHrs){
            hr.addDimension(attr, ahr.getDimension(attr).getKey(), ahr.getDimension(attr).getValue());
            _splitDataND(node, sample, attrs, ith+1, hr, hrs, ithOnly);
            hr.removeDimension(attr);
        }
    }

    public List<HyperRectangle> splitDataNDSimultaneously(PANode node, Table sample,
                                List<String> attrs){
        List<HyperRectangle> hrs = new ArrayList<HyperRectangle>();
        _splitDataND(node, sample, attrs, 0, new HyperRectangle(), hrs, -1);
        return hrs;
    }

    public List<HyperRectangle> splitNodeDataNDAlternatively(PANode node,
                                Table sample,
                                int ithOnly,
                                List<String> attrs){
        List<HyperRectangle> hrs = new ArrayList<HyperRectangle>();
        _splitDataND(node, sample, attrs, 0, new HyperRectangle(), hrs, ithOnly);
        return hrs;
    }

    public boolean foundBestPartition(){
        return true;
    }

}

abstract class NDPartitioner extends Partitioner{

}


abstract class SamplePartitioner extends Partitioner{
    Table sample;
    public void setSample(Table s){sample = s;}
    public Table getSample(){return sample;}

}

abstract class VariancePartitioner extends SamplePartitioner{
    long totalSize;
    boolean initialized = false;
    String predAttr;
    String targetAttr;
    boolean foundBest;
    double lowerBoundVar;
    double upperBoundVar;
    double maxVarianceFromSearch = Double.MAX_VALUE;
    Map<String, Double> maxVarCache;
    int minSamplePerBucket = 999999;

    VarianceEstimator estimator;

    public void setSample(Table s){
        sample = s;
        estimator.setSample(s);
    }

    public boolean foundBestPartition(){
        return foundBest;
    }

    public VariancePartitioner(){
        estimator = new VarianceEstimator();
    }

    public void initialize(long totalSize, String predAttr,
                            String targetAttr, float theta,
                            int minSamplePerBucket,
                            double minTargetAttrValue,
                            double maxTargetAttrValue){
        this.predAttr = predAttr;
        this.targetAttr = targetAttr;
        this.totalSize = totalSize;
        this.minSamplePerBucket = minSamplePerBucket;
        this.lowerBoundVar = minTargetAttrValue*minTargetAttrValue/2.0f;
        this.upperBoundVar =
        maxTargetAttrValue*maxTargetAttrValue*totalSize*totalSize;
        estimator.initialize(totalSize, predAttr, targetAttr,
                                theta, minSamplePerBucket);
        this.initialized = true;
    }
}

class MedianPartitioner extends SamplePartitioner{
    public List<HyperRectangle> partitionTo(int k, String predAttribute){
        //iterate the sample then split then into equal depth partition.
        System.out.println("Unimplemented: median partitioner is ONLY used by kdtree for splitting, not partitioning.");
        System.exit(1);
        return null;
        // List<HyperRectangle> hr = new ArrayList<HyperRectangle>();
        // return hr;
    }

    public List<HyperRectangle> splitData1D(PANode node, Table sample, String attr,
                                            boolean split){
        List<HyperRectangle> hr = new ArrayList<HyperRectangle>();
        sample = sample.sortAscendingOn(attr);
        DoubleColumn c = sample.doubleColumn(attr);
        HyperRectangle nhr = node.getPredicates();
        double min = nhr.getDimension(attr).getKey();
        double max = nhr.getDimension(attr).getValue();

        if(min == max || split == false){
            HyperRectangle h = new HyperRectangle();
            h.addDimension(attr, min, max);
            hr.add(h);
        }else{
            double median = c.get(sample.rowCount()/2);
            HyperRectangle h = new HyperRectangle();
            h.addDimension(attr, min, median);
            hr.add(h);
            h = new HyperRectangle();
            h.addDimension(attr, median, max);
            hr.add(h);
        }
        return hr;
    }
}
