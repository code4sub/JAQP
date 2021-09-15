package compass;

import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import static tech.tablesaw.api.QuerySupport.and;
import java.util.*;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Predicate;
import com.google.common.collect.MinMaxPriorityQueue;

class HeapItem implements Comparable<HeapItem>{
    private double value;
    private String hash;
    public HeapItem(double v, String h){
        value = v;
        hash = h;
    }
    public double getValue(){
        return value;
    }
    @Override
    public boolean equals(Object o){
        if (o == this) {
            return true;
        }
        if (!(o instanceof HeapItem)) {
            return false;
        }
        HeapItem other = (HeapItem) o;
        return Double.compare(value, other.value)==0
                && hash.compareTo(other.hash) == 0;
    }
    public int compareTo(HeapItem o){
        return Double.compare(value, o.value);
    }
    public String toString(){
        return hash+":"+value;
    }
}

class Summary{
    /*
    * To handle deletion for min and max, we store k top min and max with their Hash.
    * Insertion and deletion are both lg(k).
    * the minHeap keeps the top k largest value and the maxHeap keeps the top k smallest
    * the min is then the min of the maxHeap, and the max is the max of the minHeap
    TODO: that's why we need minMaxPriorityQueue instead of just the PriorityQueue
    */
    double cnt;
    double sum;
    double avg;
    final int heapSize = 10;
    //TODO: in this setting the minHeap keeps the top k largest value
    MinMaxPriorityQueue<HeapItem> minHeap;
    MinMaxPriorityQueue<HeapItem> maxHeap;
    String targetAttr; //the target or the aggregate attribute
    public Summary(String s){
        targetAttr = s;
        minHeap = MinMaxPriorityQueue.maximumSize(heapSize).create();
        maxHeap = MinMaxPriorityQueue
            .orderedBy(Comparator.comparing(HeapItem::getValue).reversed())
            .maximumSize(heapSize).create();
    }
    public Summary(String attr, Table t){
        DoubleColumn col = t.doubleColumn(attr);
        targetAttr = attr;
        cnt = col.size();
        sum = col.sum();
        avg = col.mean();
        minHeap = MinMaxPriorityQueue.maximumSize(heapSize).create();
        maxHeap = MinMaxPriorityQueue
            .orderedBy(Comparator.comparing(HeapItem::getValue).reversed())
            .maximumSize(heapSize).create();
        buildHeaps(t);
    }

    public double getMin(){
        if(minHeap == null || minHeap.size() == 0)
            return Double.MIN_VALUE;
        return minHeap.peek().getValue();
    }

    public double getMax(){
        if(maxHeap == null || maxHeap.size() == 0)
            return Double.MAX_VALUE;
        return maxHeap.peek().getValue();
    }

    private void buildHeaps(Table t){
        /*
        Iterate through the entire table to build the min and max heap.
        This cost O(nlgk), k is the size of heap -- better than nlgn from sorting.
        */
        Consumer<Row> processRow =
        row -> {
            double v = row.getDouble(targetAttr);
            String h = row.getString(Dataset.getHashColumnName());
            HeapItem hi = new HeapItem(v, h);
            minHeap.add(hi);
            maxHeap.add(hi);
        };
        t.stream().forEach(processRow);
    }

    public void scale(double r){
        cnt *= r;
        sum *= r;
    }

    public String toString(){
        if(targetAttr == "")
            return "";
        return targetAttr + " Cnt:" + String.format("%.2f",cnt) + ", Sum: " + String.format("%.2f",sum) + ", Avg: " + String.format("%.2f",avg) + "; "; //+ " Min: " + minHeap.peek() + ", Max: " + maxHeap.peek() +
    }

    public boolean isEmpty(){
        return targetAttr == "";
    }
    double getCount(){
        return cnt;
    }
    double getSum(){
        return sum;
    }

    public void merge(Summary other){
        assert targetAttr == "" || other.targetAttr == targetAttr: "Cannot merge summaries with different name/attribute";
        targetAttr = other.targetAttr;
        mergeHeaps(other);
        cnt = cnt + other.cnt;
        sum = sum + other.sum;
        avg = sum/cnt;
    }

    private void mergeHeaps(Summary other){
        Iterator<HeapItem> it = other.minHeap.iterator();
        while(it.hasNext()){
            minHeap.add(it.next());
        }
        it = other.maxHeap.iterator();
        while(it.hasNext()){
            maxHeap.add(it.next());
        }
    }

    private void removeFromHeaps(Summary other){
        //O(heapSize*2)
        Iterator<HeapItem> it = other.minHeap.iterator();
        while(it.hasNext()){
            minHeap.remove(it.next());
        }
        it = other.maxHeap.iterator();
        while(it.hasNext()){
            maxHeap.remove(it.next());
        }
    }

    public void remove(Summary other){
        assert targetAttr == "" || other.targetAttr == targetAttr: "Cannot remove summaries with different name/attribute";
        targetAttr = other.targetAttr;
        removeFromHeaps(other);
        cnt = cnt - other.cnt;
        sum = sum - other.sum;
        avg = sum/cnt;
    }
}

class PreAgg{
    HyperRectangle predicates;
    Summary cSummary; //for catch up
    Summary uSummary; //for online update

    public void setSummary(Summary s, boolean isCatchUp){
        if(isCatchUp){
            cSummary = new Summary("");
            cSummary.merge(s);
        }else{
            uSummary = new Summary("");
            uSummary.merge(s);
        }
    }

    public Summary getSummary(boolean isCatchUp){
        // System.out.println("csummary:" + (cSummary==null) + ", usummary:" + (uSummary == null));
        if(isCatchUp)
            return cSummary;
        else
            return uSummary;
    }

    public void scaleCatchUpSummary(double ratio){
        cSummary.scale(ratio);
    }

    public void mergeSummary(Summary other, boolean isCatchUp){
        if (isCatchUp)
            cSummary.merge(other);
        else
            uSummary.merge(other);
    }

    public PreAgg(HyperRectangle pred, Summary sc, Summary su){
        cSummary = sc;
        uSummary = su;
        predicates = pred;
    }

    public String toString() {
        return predicates.toString() + "; cSummary: " + cSummary;
    }

    public HyperRectangle getPredicates(){
        return predicates;
    }
}
