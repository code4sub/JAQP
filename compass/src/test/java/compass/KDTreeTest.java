package compass;

import static tech.tablesaw.aggregate.AggregateFunctions.*;
import tech.tablesaw.api.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

public class KDTreeTest {
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
                DoubleColumn.create("c", c),
                DoubleColumn.create("d", c),
                DoubleColumn.create("e", c),
                DoubleColumn.create("f", c)
                );
    }

    // @Test
    public void medianPartitionerTest1D(){
        medianPartitionerTestND(Arrays.asList("a"));
    }

    public void medianPartitionerTestND(List<String> attrs){
        //build a sample table.
        //build partitions
        int k = 5;
        SamplePartitioner eq = new MedianPartitioner();
        // List<String> attrs = Arrays.asList{"a", "b");

        List<HyperRectangle> hrs = eq.splitDataNDSimultaneously((PANode)null, table, attrs);
        System.out.println(" Split over " + attrs + ":");
        int idx = 0;
        for(HyperRectangle hr : hrs){
            System.out.println("\t" + hr);
            if(idx>0){
                assert(hr.intersects(hrs.get(idx-1))==false); //should be disjoint
            }
        }
    }

    // @Test
    public void medianPartitionerTest2D(){
        medianPartitionerTestND(Arrays.asList("a", "b"));
    }

    // @Test
    public void medianPartitionerTest3D(){
        medianPartitionerTestND(Arrays.asList("a", "b", "c"));
    }

    @Test
    public void testKDTree(){
        //build a sample table.
        //build partitions
        Table dup = table;
        System.out.println("\nBuilding synthetic dataset");
        for(int i = 0; i < 12; i++){
            for(Row r: dup.copy()){
                for(int j = 0; j < 5; j++)
                r.setDouble(j, dup.rowCount()*ThreadLocalRandom.current().nextFloat());
                dup.addRow(r);
            }
        }

        for(String a: Arrays.asList("a", "b", "c", "d", "e")){
            System.out.println(dup.summarize(a, max, min, median).apply());
        }

        table = dup;
        System.out.println("Built, #rows = " + dup.rowCount());

        KDTree kdtree = new KDTree("", Arrays.asList("a", "b"), "e", 8);
        SamplePartitioner md = new MedianPartitioner();
        // md.setSample(table);
        // md.initialize(table.rowCount(), "a", "c", 0.4f, 2, 1.0, 10.0);
        kdtree.setPartitioner(md);
        kdtree.setPartitionSample(table);
        kdtree.initialize(table.rowCount(), 0.4f, 2, table);
        long startTime = System.currentTimeMillis();
        System.out.println("\nBuilding KD Tree...");
        kdtree.buildTree();
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("KDTree built in " + elapsedTime + "ms");
        System.out.println("Visite leaf layer via leaf layer links:");
        kdtree.printLinkedList(kdtree.getFirstLeaf());
    }

    // @Test
    public void searializationTest(){

    }
    // @Test
    public void desearializationTest(){

    }
}
