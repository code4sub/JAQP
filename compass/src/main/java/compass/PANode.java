package compass;

import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import static tech.tablesaw.api.QuerySupport.and;
import java.util.*;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Predicate;
import com.google.common.collect.MinMaxPriorityQueue;

@ThreadSafe
class PANode{
    private List<PANode> children;
    private PreAgg preAgg;
    private Table stratum = null;
    private int depth = 0;
    private PANode lleaf = null; //left most leaf of an internal node
    private PANode rleaf = null; //right most leaf of an internal node
    private PANode next = null; //link leaves together
    public int getDepth(){return depth;}
    public PANode getNext(){return next;}
    public void setNext(PANode n){next = n;}
    public void setLLeaf(PANode n){lleaf = n;}
    public void setRLeaf(PANode n){rleaf = n;}
    private Set<String> hashes = new HashSet<String>();

    public PANode getLeftMostLeaf(){
        return lleaf;
    }
    public PANode getRightMostLeaf(){
        return rleaf;
    }

    public synchronized void randomRemoveHalfSampleFromStratum(){
        //in case the node get modified by another thread, we will fallback to remove the last row.
        Table todel = stratum.sampleX(0.5);

        // System.out.println("deleting: " + todel.rowCount());
        // System.out.println("before: " + this);
        for(String hashToDelete: todel.stringColumn(Dataset.getHashColumnName())){

            hashes.remove(hashToDelete);
            stratum = stratum.dropWhere(
                stratum.stringColumn(Dataset.getHashColumnName()).isEqualTo(hashToDelete)
                );
        }
        updateStatsForDeletion(todel);
        // System.out.println("after: " + this);
    }

    public synchronized void removeNthSampleFromStratum(int nth){
        //in case the node get modified by another thread, we will fallback to remove the last row.
        try{
            nth = Math.min(stratum.rowCount()-1, nth);
            int[] drop = {nth};
            stratum = stratum.dropRows(drop);
        }catch(Exception e){
            e.printStackTrace();
            System.out.println(nth + "th at size " + stratum.rowCount());
        }
    }

    public void scaleCatchUpSummary(double populationSize, double catchUpSize){
        /*
        when catch up is not complete, we need to estimate the population size of each stratum
        N_i = N*n_i/n
        where N is the population size of the entire existing data, i.e. populationSize
        n is the total number of background sampling, i.e. cathUpSize
        and n_i is the number of background samples assigned to the ith node
        */

        // double estSize = populationSize*preAgg.getSummary(true).getCount()/catchUpSize;
        // double ratio = estSize/preAgg.getSummary(true).getCount();
        double ratio = populationSize/catchUpSize;
        preAgg.scaleCatchUpSummary(ratio);
    }

    public Table getStratumCopy(){
        return stratum.copy();
    }

    public Table getStratum(){
        return stratum;
    }

    public synchronized int getStratumSize(){
        if (stratum == null) return 0;
        return stratum.rowCount();
    }

    public synchronized void setSummary(Summary s, boolean b){
        preAgg.setSummary(s, b);
    }

    public synchronized double getSummaryCount(boolean b){
        return preAgg.getSummary(b).getCount();
    }
    public synchronized double getSummarySum(boolean b){
        return preAgg.getSummary(b).getSum();
    }

    public synchronized double[] getSummaryStatsAsArray(boolean b){
        Summary s = preAgg.getSummary(b);
        if(s.isEmpty() == true) return null;
        double[] ret = new double[]{s.getCount(), s.getSum(), s.getMin(), s.getMax()};
        return ret;
    }

    public synchronized void mergeSummary(Summary s, boolean b){
        preAgg.mergeSummary(s, b);
    }

    public synchronized List<PANode> getChildren(){
        return children;
    }

    public synchronized void updateStatsForInsertion(Table newSample){
            Summary s = new Summary(preAgg.cSummary.targetAttr, newSample);
            // System.out.println("Merging:" + s +"\n" + preAgg.uSummary);
            preAgg.uSummary.merge(s);
            // System.out.println("=" + "\n" + preAgg.uSummary);
            //it is possible usummary count smaller than stratum size.
            //since stratum is built beofre online updating.
    }

    public synchronized void updateStatsForDeletion(Table todel){
        Summary s = new Summary(preAgg.cSummary.targetAttr, todel);
        preAgg.uSummary.remove(s);
    }

    public void removeSampleFromStratum(Table deletion){
        String hashToDelete = deletion.stringColumn(Dataset.getHashColumnName()).get(0);
        if(hashes.contains(hashToDelete)){
            hashes.remove(hashToDelete);
            stratum.dropWhere(
                stratum.stringColumn(Dataset.getHashColumnName()).isEqualTo(hashToDelete)
                );
        }
    }

    public synchronized boolean addSampleToStratum(Table s){
        if(s.rowCount() == 0) return false;
        try{
            if(stratum == null) stratum = s;
            else{
                stratum = stratum.append(s);
            }
            for(String hash: s.stringColumn(Dataset.getHashColumnName())){
                hashes.add(hash);
            }
        }catch(Exception e){
            System.out.println("Fail to add sample to stratum\n"+s.print());
        }
        // System.out.println(s.first(10).print());
        return true;
    }

    public PANode(PreAgg pa){
        children = new ArrayList<PANode>();
        preAgg = pa;
    }

    public PANode(PreAgg pa, int depth){
        this.depth = depth;
        children = new ArrayList<PANode>();
        preAgg = pa;
    }

    public String getValue(){
        return preAgg.predicates.toString();
    }
    public PANode getLeft(){
        if(children!=null && children.size() > 0) return children.get(0);
        return null;
    }
    public PANode getRight(){
        if(children!=null && children.size() > 0) return children.get(children.size()-1);
        return null;
    }

    public synchronized boolean containsSample(Table sample){
        /*
        return 0 if sample is covered by node
        return -1 if sample is on the left side (i.e. smaller)
        return 1 if larger
        */
        for(String attr: preAgg.getPredicates().getDimensionNames()){
            // System.out.println("attr " + attr);
            double value = sample.doubleColumn(attr).get(0);
            Map.Entry<Double, Double> range = preAgg.getPredicates().getDimension(attr);
            if( value < range.getKey() || value > range.getValue() ) return false;
        }
        return true;
    }

    public synchronized int locateValue(double value, String attr){
        /*
        return 0 if sample is covered by node
        return -1 if sample is on the left side (i.e. smaller)
        return 1 if larger
        */

        Map.Entry<Double, Double> range = preAgg.getPredicates().getDimension(attr);
        if( value < range.getKey() ) return -1;
        if( value >= range.getValue() ) return 1; //right open, equal means not contained.
        return 0;

    }

    //* this function does not need to be synchronized
    //* because the structure of the tree (i.e. the hyper rectangles of each node) will not change (and is not open to change) after construction
    //! but the summary or preAgg could be modifying by others during read.
    //! could cause exceptions
    public Table filterSample(Table sample){
        Table filtered = sample; //.copy();
        HyperRectangle hr = preAgg.getPredicates();
        for(String attr: hr.getDimensionNames()){
            Map.Entry<Double, Double> range = hr.getDimension(attr);
            filtered =  filtered.where(
                and(
                    t->t.doubleColumn(attr).isGreaterThanOrEqualTo(range.getKey()),
                    t->t.doubleColumn(attr).isLessThan(range.getValue())
                    )
                );
        }
        return filtered;
    }

    public synchronized PANode merge(PANode other){
        HyperRectangle hr1 = preAgg.getPredicates();
        HyperRectangle hr2 = other.getPredicates();
        PreAgg pa = new PreAgg(hr1.merge(hr2), new Summary(""), new Summary(""));
        PANode parent = new PANode(pa);
        return parent;
    }

    public synchronized void addChild(PANode child){
        children.add(child);
    }

    synchronized HyperRectangle getPredicates(){
        return preAgg.getPredicates();
    }

    public HyperRectangle getIntersection(HyperRectangle hr){
        return preAgg.getPredicates().intersection(hr);
    }

    public synchronized boolean intersects(PANode nodeA, PANode nodeB){
        return nodeA.getPredicates().intersects(nodeB.getPredicates());
    }

    public synchronized boolean covers(PANode nodeA, PANode nodeB){
        return nodeA.getPredicates().covers(nodeB.getPredicates());
    }

    public synchronized boolean coveredBy(HyperRectangle hr){
        return hr.covers(preAgg.getPredicates());
    }

    public String toString(){
        String s = preAgg.toString();
        if(stratum != null)
            s += "; Stratum size: " + stratum.rowCount();
        return s;
    }

}
