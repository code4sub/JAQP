package producer;

import java.util.*;
import org.junit.jupiter.api.*;
import java.util.stream.Stream;
import java.util.stream.Collector;
import java.util.concurrent.*;

public class ConductorTest {
    String datasetPath = "../data/10k-intel.csv";
    String queryPath = "../files/intel_queries.txt";
    String commandFile = "../files/simple_nonsleep.txt";
    String dataTopic = "intel-data";
    String queryTopic = "intel-query";
    String deletionTopic = "intel-deletion";

    void showTestName(TestInfo testInfo){
        String methodName = testInfo.getTestMethod().orElseThrow().getName();
        System.out.println("\n================================");
        System.out.println(methodName);
        System.out.println("================================");
    }

    @BeforeEach
    void setup(TestInfo testInfo){
        showTestName(testInfo);
    }

    @Test
    public void InsertionTest(){

    }
}