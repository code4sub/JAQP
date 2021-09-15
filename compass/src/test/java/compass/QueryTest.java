package compass;

import tech.tablesaw.api.*;
import java.util.*;
import org.junit.jupiter.api.*;
import java.util.stream.Stream;
import java.util.stream.Collector;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public class QueryTest {
    String dbName = "intelwireless";
    String topic = "intel-data";
    PASS pass;

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

    @AfterEach
    void clean(){
    }
    //TODO generate random query and test the accuracy of query results
}