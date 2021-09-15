package compass;

import tech.tablesaw.api.*;
import java.util.*;
import org.junit.jupiter.api.*;
import java.util.stream.Stream;
import java.util.stream.Collector;
import java.util.concurrent.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

public class BaselineTest {
    int seed = 0;
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
    // @Test
    void ScheduledExecutorTest(){
        try{
            ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
            work1();
            ScheduledFuture f =
            ses.schedule(new Callable() {
                public Object call() throws Exception {
                    System.out.println("Executed!");
                    return "Called!";
                }
            }, 10, TimeUnit.SECONDS);

            System.out.println(f.get());
            ses.shutdown();
        }catch(Exception e){e.printStackTrace();}
    }
    static void work1(){
        System.out.println("Work1");
    }
    static int work2(){
        System.out.println("Work2");
        return 10;
    }
    @Test
    void CompletableFutureTest(){

        CompletableFuture<String>  data = createCompletableFuture()
                .thenApply((Integer count) -> {
                    int transformedValue = count * 10 + seed;
                    return transformedValue;
                }).thenApply(transformed -> "Finally creates a string: " + transformed);

            try {
                System.out.println(data.get());
            } catch (InterruptedException | ExecutionException e) {

            }
    }
    public CompletableFuture<Integer> createCompletableFuture() {
        CompletableFuture<Integer>  result = CompletableFuture.supplyAsync(BaselineTest::work2);
        seed = 100;
        return result;
    }

    double work3(){
        try{
        Thread.sleep(5000L);
        return 100.39;
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        return 0.0;
    }

    @Test
    public void createQueryCF(){
        try{
            ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
            Executor exe = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), (Runnable r)->{
                Thread t = new Thread(r);
                t.setDaemon(true); //use daemon thread can avoid non-terminated process due to blocked thread
                return t;
                }
            );
            CompletableFuture<Double> cf = CompletableFuture.supplyAsync(()->work3(), exe).thenApply(name -> {
                System.out.println("Job is done by a thread.");
                return 100 + name;
            });
            readWriteLock.readLock().lock();
            System.out.println("Submitted and pretend to working on seomthing else..."+Runtime.getRuntime().availableProcessors());
            Thread.sleep(10000L);
            readWriteLock.readLock().unlock();
            readWriteLock.writeLock().lock();
            readWriteLock.writeLock().unlock();
            System.out.println("Done witht the other things, collecting results.");
            System.out.println("Results: " + cf.get());
        }catch(Exception e){
            e.printStackTrace();
        }
        // CompletableFuture<Double>  result = CompletableFuture.supplyAsync(work3);
        // seed = 100;
        // return result;
    }
}