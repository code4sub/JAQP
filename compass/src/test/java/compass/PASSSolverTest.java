package compass;

import tech.tablesaw.api.*;
import java.util.*;
import org.junit.jupiter.api.*;
import java.util.stream.Stream;
import java.util.stream.Collector;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public class PASSSolverTest {
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

    // @Test
    void fullScanInitializationTest(){
        pass = new PASS(
            topic,
            true,
            Arrays.asList("itime"),
            "voltage",
            16, 64,
            new EqualDepthPartitioner(),
            1.0f
        );
        pass.setDataset(dbName);
        pass.offlineInitialization();
        pass.catchUp();
        assert pass.getTree().getRootCnt() == pass.getCatchUpSize(): "CatchUp size does not match: "+pass.getTree().getRootCnt()+"(actual) vs "+pass.getCatchUpSize()+" (expected)";
    }

    // @Test
    void partialScanInitializationTest(){
        pass = new PASS(
            topic,
            true,
            Arrays.asList("itime"),
            "voltage",
            16,
            64,
            new EqualDepthPartitioner(),
            0.5f
        );
        pass.setDataset(dbName);
        pass.offlineInitialization();
        pass.catchUp();
        assert pass.getTree().getRootCnt() >= pass.getCatchUpSize(): "CatchUp size is too small: "+pass.getTree().getRootCnt()+"(actual) vs "+pass.getCatchUpSize()+" (expected)";
    }
    @Test
    void eventLoopTest(){
        pass = new PASS(
            topic,
            true,
            Arrays.asList("itime"),
            "voltage",
            16,64,
            new EqualDepthPartitioner(),
            0.5f
        );
        pass.setDataset(dbName);
        pass.offlineInitialization();
        pass.run();
        System.out.println("Main thread sleeping 10s...");
        try{Thread.sleep(10000L);}catch(Exception e){e.printStackTrace();}
        System.out.println("Main thread sleep done.");
    }
}
