package compass;

import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import static tech.tablesaw.api.QuerySupport.and;
import java.util.*;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import com.google.common.base.Predicate;
import com.google.common.collect.MinMaxPriorityQueue;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

enum SplitBy {MEDIAN};

class KDTreeHeapComparator implements Comparator{
    public int compare(Object oa, Object ob){
        SimpleEntry<Double, SimpleEntry<PANode, Table>> a = (SimpleEntry<Double, SimpleEntry<PANode, Table>>)oa;
        SimpleEntry<Double, SimpleEntry<PANode, Table>> b = (SimpleEntry<Double, SimpleEntry<PANode, Table>>)ob;
        int d1 = a.getValue().getKey().getDepth();
        int d2 = b.getValue().getKey().getDepth();
        if(d1 < d2) return 1;
        else if(d1 > d2) return -1;

        double v1 = a.getKey();
        double v2 = b.getKey();
        if(v1 > v2) return 1;
        else if(v1 < v2) return -1;
        return 0;
    }
}

public class KDTree extends PATree {

    //keep a copy of sample in the heap so we don't need to re-filter them again.
    int minSamplePerBucket;
    List<PANode> leaves;
    VarianceEstimatorND estimator;
    public void buildTree(){
        if(partitioner == null){
            System.out.println("Error: partitioner not specified.");
            System.exit(1);
        }
        try{
            long startTime = System.currentTimeMillis();
            buildTreeTopDown();
            buildLeafLayer();
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            System.out.println("\nKDTree built in " + elapsedTime + "ms");
            // print(root);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public List<PANode> getLeafLayer(){
        return leaves;
    }

    private void buildLeafLayer(){
        PANode node = getFirstLeaf();
        leaves = new ArrayList<PANode>();
        while(node!=null){
            leaves.add(node);
            node = node.getNext();
        }
    }


    public void initialize(long totalSize, float theta,
                                int minSamplePerBucket, Table sample){
        this.minSamplePerBucket = minSamplePerBucket;
        estimator.setSample(sample);
        estimator.initialize(totalSize, predAttributes, aggrAttribute, theta, minSamplePerBucket);
    }

    PANode buildNode(HyperRectangle fromHr, int depth){
        HyperRectangle hr = fromHr;
        if(hr == null){
            hr = new HyperRectangle();
            for(String attr: predAttributes){
                hr.addDimension(attr, -Double.MAX_VALUE, Double.MAX_VALUE);
            }
        }
        PreAgg pa = new PreAgg(hr, new Summary(""), new Summary(""));
        return new PANode(pa, depth);
    }

    SimpleEntry<Double, SimpleEntry<PANode, Table>> buildHeapItem(PANode node, Table sample){
        estimator.resetSample(sample);

        double scale = -node.getDepth(); //(1000000 - node.getDepth()*10000);
        double variance = 1 * estimator.getIntervalVariance(0, sample.rowCount()-1);
        // System.out.println(String.format("%s Var est %.2f", node.getValue(), variance));
        return new SimpleEntry<Double, SimpleEntry<PANode, Table>>(variance, new SimpleEntry<PANode, Table>(node, sample));
    }

    private List<PANode> linkInternal2Leaves(PANode node){
        if(node == null) return null;
        List<PANode> leaves = new ArrayList<PANode>();
        List<PANode> children = node.getChildren();
        if(children.size() == 0){
            leaves.add(node);
        }else{
                assert children.size() == 2: "Children size should be 2";
                List<PANode> lleaves = linkInternal2Leaves(children.get(0));
                PANode lmost = lleaves.get(0);

                List<PANode> rleaves = linkInternal2Leaves(children.get(node.getChildren().size()-1));
                PANode rmost = rleaves.get(rleaves.size()-1);
                leaves.add(lmost);
                leaves.add(rmost);
                lleaves.get(lleaves.size()-1).setNext(rleaves.get(0));
                node.setLLeaf(lmost);
                node.setRLeaf(lmost);
        }
        return leaves;
    }

    public void linkNodes(){
        /*
         link each inner node to its boundary leaf nodes
         link all leaf nodes together as a list.
         after this the leaf layer can be accessed as a linked list by
         root.leftLeaf
         */
        linkInternal2Leaves(root);
    }

    public PANode getFirstLeaf(){
        return root.getLeftMostLeaf();
    }
    public PANode getLastLeaf(){
        return root.getLeftMostLeaf();
    }

    public void buildTreeTopDown(){
        MinMaxPriorityQueue<SimpleEntry<Double, SimpleEntry<PANode, Table>> > maxHeap = MinMaxPriorityQueue.orderedBy(new KDTreeHeapComparator()).create();
        root = buildNode(null, 0);
        maxHeap.add(buildHeapItem(root, partitionSample));
        int leafCount = 1;
        while(leafCount < leafSize){
            // String attr = predAttributes.get(attrId%predAttributes.size());
            // alternatively over all predicate attributes
            SimpleEntry<Double, SimpleEntry<PANode, Table>> heapItem = maxHeap.pollLast(); //use as max heap.
            leafCount -= 1;
            PANode node = heapItem.getValue().getKey();
            Table sample = heapItem.getValue().getValue();
            if(sample.rowCount() <= minSamplePerBucket){
                leafCount += 1;
                continue;
            }
            List<HyperRectangle> hrs = partitioner.splitNodeDataNDAlternatively(node, sample, node.getDepth() % predAttributes.size(), predAttributes);
            if(hrs.size() == 1) {
                leafCount += 1;
                continue;
            }
            for(HyperRectangle hr: hrs){
                PANode child = buildNode(hr, node.getDepth()+1);
                node.addChild(child);
                Table childSample = node.filterSample(sample);
                maxHeap.add(buildHeapItem(child, childSample));
                leafCount += 1;
            }
        }
        System.out.println("Adding linkes between internal node and its boundary leaves; adding links to leaves...");
        linkNodes();
        System.out.println("Linked.");
    }

    public KDTree(String dataTopic, List<String> predAttrs, String targetAttr,
                            int leafSize){
        super(dataTopic, predAttrs, targetAttr, leafSize);
        estimator = new VarianceEstimatorND();
    }
}
