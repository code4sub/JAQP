package compass;

import tech.tablesaw.api.*;
import tech.tablesaw.columns.Column;
import static tech.tablesaw.api.QuerySupport.and;
import java.util.*;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Predicate;
import com.google.common.collect.MinMaxPriorityQueue;

public abstract class PATree {
    List<String> predAttributes;
    String aggrAttribute;
    long backgroundSampleSize;
    Table catchUpSample;
    int leafSize;
    AtomicLong reservoirSize = new AtomicLong(0);
    int traversedLeafCnt;
    Map<Integer, List<PANode>> layers = new HashMap<Integer, List<PANode>>();
    PANode root;
    Partitioner partitioner;
    Table partitionSample; //kd tree needs it.
    public void setPartitionSample(Table s){partitionSample = s;};
    public PATree(){}
    public PATree(String dataTopic, List<String> predAttrs, String targetAttr,
                            int leaf){
        predAttributes = predAttrs;
        aggrAttribute = targetAttr;
        leafSize = leaf;
    }
    public List<PANode> getLeafLayer(){
        return layers.get(0);
    }
    public void initialize(long totalSize,
                                float theta,
                                int minSamplePerBucket, Table sample){}

    public long getRootCnt(){
        return (long)root.getSummaryCount(true) + (long)root.getSummaryCount(false);
    }
    public PANode getFirstLeaf(){return null;}
    public void setPartitioner(Partitioner p){
        partitioner = p;
    }
    public boolean loadTreeFromFile(String path){
        //todo
        return false;
    }
    public void saveTreeToFile(String path){
        //todo

    }
    public long getBackgroundSampleSize(){
        return backgroundSampleSize;
    }

    public void updateReservoirAndStatsForDeletion(Table deletion){
        PANode node = findLeafNodeForSample(deletion);
        node.removeSampleFromStratum(deletion);
        node.updateStatsForDeletion(deletion);
    }

    //randomly selct percent of leaf nodes and remove half of their stratum.
    public synchronized void randomStrataEvict(float percent){
        reservoirSize.set(0);
        for(PANode node: getLeafLayer()){
            if(ThreadLocalRandom.current().nextFloat() < percent){
                node.randomRemoveHalfSampleFromStratum();
            }
            reservoirSize.getAndAdd(node.getStratumSize());
        }
        showStrata();
    }

    public void updateReservoirAndStatsForInsertion(int nth, Table sample){
        //remove nth sample from reservoir
        //reservoirSize is the same
        if(nth > 0)
            removeNthSampleFromStrata(nth);

        PANode node = findLeafNodeForSample(sample);
        // System.out.println("Adding and updating node stats for new sample before:\n" + node);
        node.addSampleToStratum(sample);
        node.updateStatsForInsertion(sample);
        if(nth < 0)
            reservoirSize.getAndAdd(sample.rowCount());
        // System.out.println("After:\n" + node);
    }

    public long getReservoirSize(){return reservoirSize.get();}

    public void adjustCatchUpSummary(double populationSize){
        for(PANode node: getLeafLayer()){
            node.scaleCatchUpSummary(populationSize, backgroundSampleSize);
        }
        System.out.println("Catch-up summary scaled.");
    }

    public void updateStatsForInsertion(Table sample){
        PANode node = findLeafNodeForSample(sample);
        // System.out.println("Update node stats for insertion:\n" + node);
        node.updateStatsForInsertion(sample);
        // System.out.println("Updated:\n" + node);
    }

    public List<Integer> findNodeIdRangeForQuery1D(Query q){
        List<Integer> nodes = new ArrayList<Integer>();
        String attr = predAttributes.get(0);
        Map.Entry<Double, Double> range = q.predicates.getDimension(attr);
        nodes.add(findLeafNodeIdxForValue(range.getKey(), attr));
        nodes.add(findLeafNodeIdxForValue(range.getValue(), attr));
        return nodes;
    }

    public PANode findLeafNodeForSample(Table sample){
        if(predAttributes.size() == 1){
            String attr = predAttributes.get(0);
            return getNthLeafNode(findLeafNodeIdxForValue(
                    sample.doubleColumn(attr).get(0),
                    attr
                    ));
        }

        PANode node = findNDLeafNodeForSample(root, sample);
        if(node.containsSample(sample) == false){
            //sanity check
            System.out.println("ERROR: PANode does not contain sample: " + node + ", " + sample);
            System.exit(1);
        }
        // System.out.println("Node for insertion " + node + ", " + sample);
        return node;
    }

    public PANode findNDLeafNodeForSample(PANode node, Table sample){
        if(node.getLeft() == null && node.getRight() == null){
            return node;
        }else{
            if(node.getLeft() != null && node.getLeft().containsSample(sample)){
                return findNDLeafNodeForSample(node.getLeft(), sample);
            }else
                return findNDLeafNodeForSample(node.getRight(), sample);
        }
    }

    public PANode getNthLeafNode(int nth){
        return getLeafLayer().get(nth);
    }

    public int findLeafNodeIdxForValue(double value, String attr){
        //works for 1 dimension
        List<PANode> leaves = getLeafLayer();
        int l = 0, r = leaves.size()-1;
        while( l <= r){
            int mid = l + (r-l)/2;
            PANode midnode = leaves.get(mid);
            int location = midnode.locateValue(value, attr);
            if(location == 0){
                return mid;
            }else if(location < 0){
                r -= 1;
            }else{
                l += 1;
            }
        }
        return -1;
    }

    public void removeNthSampleFromStrata(int nth){
        // System.out.println("Removing "+nth+ "th sample from strata: ");
        List<PANode> leaves = getLeafLayer();
        for(PANode node: leaves){
            if(nth < node.getStratumSize()){
                node.removeNthSampleFromStratum(nth);
                return;
            }
            nth -= node.getStratumSize();
        }
        assert false: "Failed to remove sample from strata";
    }

    public PANode findLRPartialLeaf(PANode node, HyperRectangle hr, boolean findLeftMost){
        if(node == null) return null;

        PANode l = node.getLeft(), r = node.getRight();
        if(l == null || r == null) return node;
        //both children are valid
        if(findLeftMost){
            if(l.getPredicates().intersects(hr)){
                return findLRPartialLeaf(l, hr, findLeftMost);
            }
            return findLRPartialLeaf(r, hr, findLeftMost);
        }
        //searching for the rightMost
        if(r.getPredicates().intersects(hr)){
            return findLRPartialLeaf(r, hr, findLeftMost);
        }

        return findLRPartialLeaf(l, hr, findLeftMost);
    }

    public List<PANode> findNodesInBetween(PANode l, PANode r){
        List<PANode> nodes = new ArrayList<PANode>();
        PANode n = l;
        while(n != null){
            nodes.add(n);
            // System.out.println("=>" + n.getPredicates());
            if(n == r) break;
            n = n.getNext();
        }
        return nodes;
    }

    public List<PANode> findNodesForQuery(Query q){
        PANode l = findLRPartialLeaf(root, q.getPredicates(), true);
        PANode r = findLRPartialLeaf(root, q.getPredicates(), false);
        if(l == r) return Arrays.asList(l);
        // System.out.println("L: " + l.getPredicates());
        // System.out.println("R: " + r.getPredicates());
        return findNodesInBetween(l, r);
    }

    public void showStrata(){
        List<PANode> leaves = getLeafLayer();
        int total = 0;
        for(PANode node: leaves){
            total += node.getStratumSize();
            System.out.println(node);
        }
        System.out.println("Total strata size: " + total);
    }

    public void buildStrata(Table sample){
        List<PANode> leaves = getLeafLayer();
        reservoirSize.set(sample.rowCount());
        // System.out.print(sample.shape() + sample.columnNames());

        for(PANode node: leaves){
            node.addSampleToStratum(node.filterSample(sample));
            // System.out.println(node);
        }
    }

    public void printLeafStats(boolean isCatchUp){
        //without replacement
        //1. apply the sample to each node
        Summary s = null;
        double[] stats = null;
        // Column<Double> c = sample.doubleColumn(aggrAttribute);
        for(PANode node: getLeafLayer()){
            if(stats == null) stats = node.getSummaryStatsAsArray(isCatchUp);
            else{
                double[] ns = node.getSummaryStatsAsArray(isCatchUp);
                stats[0] += ns[0];
                stats[1] += ns[1];
                stats[2] = Math.min(stats[2], ns[2]);
                stats[3] = Math.max(stats[3], ns[3]);
            }
        }
        System.out.println("Count:"+stats[0]+", Sum:"+stats[1] + ", Min:"+stats[2]+", Max:"+stats[3]);
    }
    public void applyCatchupToNode(PANode node){
        Table filtered = node.filterSample(catchUpSample);
        // cnt += filtered.rowCount();
        Summary s = new Summary(aggrAttribute, filtered);
        node.mergeSummary(s, true);
    }
    public void improveLeafStats(Table sample, boolean isCatchUp){
        //no change to stratum
        //without replacement
        //1. apply the sample to each node
        backgroundSampleSize += sample.rowCount();
        List<PANode> leaves = getLeafLayer();
        // Column<Double> c = sample.doubleColumn(aggrAttribute);
        long cnt  = 0;
        boolean parallel = true;
        if (parallel){
            catchUpSample = sample;
            leaves.stream().parallel().forEach(this::applyCatchupToNode);
        }else{
            for(PANode node: leaves){
                Table filtered = node.filterSample(sample);
                cnt += filtered.rowCount();
                Summary s = new Summary(aggrAttribute, filtered);
                node.mergeSummary(s, isCatchUp);
            }
            if(cnt != sample.rowCount()){
                System.out.println("Failed to imrpove: " + cnt + ", " + sample.rowCount());
            }
        }
    }

    public abstract void buildTree();

    public void improveAndPropagate(Table sample, boolean isCatchUp){
        improveLeafStats(sample, isCatchUp);
    }

    public void displayTree(boolean verbose){
        System.out.println("Partition Tree: " + layers.size()
                            + " layers, leaf size: " + getLeafLayer().size()
                            + ", Background sample size: " + backgroundSampleSize);
        if (verbose == false) return;
        int height = layers.size();
        for(int i = height; i > 0; i--){
            List<PANode> nodes = layers.get(i-1);
            String s = nodes.size() + "@L" + (height-i) + "\t";
            for(PANode node : nodes){
                s += node;
            }
            System.out.println(s);
        }
    }

    public void printLinkedList(PANode head){
        StringBuilder sb = new StringBuilder();
        while(head != null){
            sb.append(head.getValue()+" => ");
            head = head.getNext();
        }
        sb.append(" null ");
        System.out.println(sb.toString());
    }

    public void print(PANode node){
        traversedLeafCnt = 0;
        System.out.println(traversePreOrder(node));
    }

    public String traversePreOrder(PANode root) {

        if (root == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(root.getValue());

        String pointerRight = "`--";
        String pointerLeft = (root.getRight() != null) ? "|--" : "`--";

        traverseNodes(sb, "", pointerLeft, root.getLeft(), root.getRight() != null);
        traverseNodes(sb, "", pointerRight, root.getRight(), false);

        return sb.toString();
    }

    public void traverseNodes(StringBuilder sb, String padding,
                        String pointer, PANode node,
                        boolean hasRightSibling) {
        if (node != null) {
            sb.append("\n");
            sb.append(padding);
            sb.append(pointer);
            sb.append(node.getValue());
            if(node.getLeft() == null && node.getRight() == null){
                sb.append("<= #"+traversedLeafCnt+" @ "+node.getDepth());
                traversedLeafCnt ++;
            }

            StringBuilder paddingBuilder = new StringBuilder(padding);
            if (hasRightSibling) {
                paddingBuilder.append("|  ");
            } else {
                paddingBuilder.append("   ");
            }

            String paddingForBoth = paddingBuilder.toString();
            String pointerRight = "`--";
            String pointerLeft = (node.getRight() != null) ? "|--" : "`--";

            traverseNodes(sb, paddingForBoth, pointerLeft, node.getLeft(), node.getRight() != null);
            traverseNodes(sb, paddingForBoth, pointerRight, node.getRight(), false);
        }
    }
}

class MergeTree extends PATree {
    // for 1D, with optimal partitioning.

    public void buildTree(){
        if(partitioner == null){
            System.out.println("Error: partitioner not specified.");
            System.exit(1);
        }
        try{
            buildTreeFromLeaf();
            root = layers.get(layers.size()-1).get(0);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void buildTreeFromLeaf(){
        long startTime = System.currentTimeMillis();
        int trial = 1;
        System.out.println("Partitioning...");
        List<HyperRectangle> partitions = partitioner.partitionTo(leafSize, predAttributes.get(0));
        while((partitions.size() != leafSize
            ||partitioner.foundBestPartition() == false) && trial <= 3){
            trial += 1;
            System.out.println("#" + trial + ": try again to find a better partition, current size: " + partitions.size());
            partitions = partitioner.partitionTo(leafSize, predAttributes.get(0));
        }
        if(partitioner.foundBestPartition() == false){
            /*
            in any case, we always have a partition to work with, just being cautious here for bad sampling.
            i.e. if due to bad luck on query sampling of variance estimator,
            it could keep over estimate the variance for a small interval,
            therefore the first k-1 buckets are very small and leads to a
            skewed kth bucket. Usually, this is not the best we can find.
            */
            System.out.println("Cannot find best partition, fallback to equal depth.");
            SamplePartitioner sp = new EqualDepthPartitioner();
            sp.setSample(partitionSample);
            partitions = sp.partitionTo(leafSize, predAttributes.get(0));
        }
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println(partitioner.getClass().getName() + ": best partition found in " + elapsedTime + "ms" + ", " + trial + " trial(s)");
        // System.exit(0);
        // * build the tree bottom up as of now we don't have stats available. we will update and propagate once we receive sample.
        List<PANode> leafLayer = new ArrayList<PANode>();
        for(HyperRectangle hr: partitions){
            PreAgg pa = new PreAgg(hr, new Summary(""), new Summary(""));
            PANode node = new PANode(pa);
            leafLayer.add(node);
        }

        List<PANode> currentLayer = leafLayer;
        int height = 0;
        layers.put(height, currentLayer);

        while(currentLayer.size() > 1){
            height++;
            List<PANode> newLayer = new ArrayList<PANode>();
            int checked = 0;
            for(int i = 0; i < currentLayer.size(); i++){
                if(i % 2 == 1){
                    checked = i;
                    PANode node1 = currentLayer.get(i);
                    PANode node2 = currentLayer.get(i-1);
                    PANode parent = node1.merge(node2);
                    parent.addChild(node1);
                    parent.addChild(node2);
                    newLayer.add(parent);
                }
            }
            if(checked != currentLayer.size()-1){
                assert(checked == currentLayer.size() - 2);
                //have some left over nodes that is not merged. i.e. leaf size is odd or not exponential of 2.
                newLayer.add(currentLayer.get(currentLayer.size()-1));
            }
            layers.put(height, newLayer);
            currentLayer = newLayer;
        }
    }

    public MergeTree(String dataTopic, List<String> predAttrs, String targetAttr,
                            int leafSize){
        super(dataTopic, predAttrs, targetAttr, leafSize);
    }

}

