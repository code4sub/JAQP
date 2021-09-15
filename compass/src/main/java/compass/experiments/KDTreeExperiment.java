package compass;

import java.util.*;

public class KDTreeExperiment {
    public static void main(String[] args){

        String dataset = args[0];
        String strDataTopic = args[1];
        String strQueryTopic = args[2];
        String strDeletionTopic = args[3];
        String strTick = args[4];
        String resultPath = args[5];
        int k = Integer.parseInt(args[6]);

        DataSource kafkaDataSource = new KafkaDataSource();

        // int sampleSize = 2000;

        // return;
        int overSampleX = 2;
        Solver [] solvers = new Solver[]{
                // new GroundTruthSolver(strDataTopic,
                //                     Arrays.asList("itime"),
                //                     "light", -1),
                // new ReservoirSampling(strDataTopic,
                //                     Arrays.asList("itime"),
                //                     "light", 0.01f),
                new PASS(
                    strDataTopic,
                    true, //isAsync
                    // false, //sync
                    Arrays.asList("itime"),
                    "light",
                    k,
                    64, //n sample per bucket
                    // new EqualDepthPartitioner(),
                    new NBPartitioner(),
                    0.1f, //catch up percentage
                    0.01f, //sample rate
                    overSampleX //oversampleX
                ),new PASS(
                    strDataTopic,
                    true, //isAsync
                    // false, //sync
                    Arrays.asList("itime"),
                    "light",
                    k,
                    64, //n sample per bucket
                    // new EqualDepthPartitioner(),
                    new NBPartitioner(),
                    0.2f, //catch up percentage
                    0.01f, //sample rate
                    overSampleX //oversampleX
                ),new PASS(
                    strDataTopic,
                    true, //isAsync
                    // false, //sync
                    Arrays.asList("itime"),
                    "light",
                    k,
                    64, //n sample per bucket
                    // new EqualDepthPartitioner(),
                    new NBPartitioner(),
                    0.3f, //catch up percentage
                    0.01f, //sample rate
                    overSampleX //oversampleX
                ),new PASS(
                    strDataTopic,
                    true, //isAsync
                    // false, //sync
                    Arrays.asList("itime"),
                    "light",
                    k,
                    64, //n sample per bucket
                    // new EqualDepthPartitioner(),
                    new NBPartitioner(),
                    0.4f, //catch up percentage
                    0.01f, //sample rate
                    overSampleX //oversampleX
                ),new PASS(
                    strDataTopic,
                    true, //isAsync
                    // false, //sync
                    Arrays.asList("itime"),
                    "light",
                    k,
                    64, //n sample per bucket
                    // new EqualDepthPartitioner(),
                    new NBPartitioner(),
                    0.5f, //catch up percentage
                    0.01f, //sample rate
                    overSampleX //oversampleX
                )
        };

        for(Solver solver : solvers){
            solver.setDataset(dataset);
            solver.setResultPath(resultPath);
            kafkaDataSource.registerConsumer(solver);
            solver.initialize();

            // detach from main thread and work as a subscriber of the topics.
            // PASS do catch-up in background then get notified when new msg arrives
            // The thread initialized here is not the worker, the job is done by a threadpool.
            Thread solverThread = new Thread(solver);
            solverThread.start();
        }

        kafkaDataSource.subscribeAndRead(strDataTopic, strQueryTopic, strDeletionTopic, strTick);
    }
}
