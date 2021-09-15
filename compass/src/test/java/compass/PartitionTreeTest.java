package compass;

// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertFalse;
// import static org.junit.jupiter.api.Assertions.assertNull;
// import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.tablesaw.api.*;

import java.util.*;
import org.junit.jupiter.api.*;
// import org.junit.jupiter.api.Test;

import compass.EqualDepthPartitioner;

public class PartitionTreeTest {
    private Table table;
    List<String> colNames = Arrays.asList("a", "b", "c");

    void showTestName(TestInfo testInfo){
        String methodName = testInfo.getTestMethod().orElseThrow().getName();
        System.out.println("\n================================");
        System.out.println(methodName);
        System.out.println("================================");
    }

    @BeforeEach
    void setup(TestInfo testInfo){
        showTestName(testInfo);
        double[] a = {3,  4,  5,  5,  5,  7,  8,  9,  10,  12,  13,  14,  21,  7,  71,  81, 1,  14, 2};
        double[] b = {13, 14, 15, 15, 15, 17, 81, 19, 110, 112, 113, 114, 121, 17, 711, 81, 11, 114,12};
        double[] c = {13, 14, 53, 13, 14, 12, 12, 13, 1,   19,  342, 259, 29,  29, 20,  29, 22, 22, 29};
        table =
            Table.create(
                "test",
                DoubleColumn.create("a", a),
                DoubleColumn.create("b", b),
                DoubleColumn.create("c", c));
    }

    @Test
    public void equalDepthPartitionerTest(){
        //build a sample table.
        //build partitions
        int k = 5;
        System.out.println("testEqualPartitioner k="+k);
        SamplePartitioner eq = new EqualDepthPartitioner();
        eq.setSample(table);
        for(String attr: colNames){
            List<HyperRectangle> hrs = eq.partitionTo(k, attr);
            System.out.println(k+ " Partition over " + attr + ":");
            int idx = 0;
            for(HyperRectangle hr : hrs){
                System.out.println("\t" + hr);
                if(idx>0){
                    assert(hr.intersects(hrs.get(idx-1))==false); //should be disjoint
                }
            }
        }
    }

    @Test
    public void testMergeTree(){
        //build a sample table.
        //build partitions
        int k = 4;
        SamplePartitioner eq = new EqualDepthPartitioner();
        eq.setSample(table);
        for(String attr: colNames){
            PATree tree = new MergeTree("", Arrays.asList(attr), "d", k);
            tree.setPartitioner(eq);
            tree.buildTree();
            tree.displayTree(true);
        }
    }

    @Test
    public void searializationTest(){

    }
    @Test
    public void desearializationTest(){

    }
}
