package compass;
import java.util.*;


import com.beust.jcommander.JCommander;

import compass.Solver;

public class End2EndExperiment {
    public static void main(String[] argv){
        ExpArgs args = new ExpArgs();
        JCommander.newBuilder().addObject(args).build().parse(argv);
        List<String> baselines = Arrays.asList(args.baselines.split(","));
        String dataset = args.dataset;
        String dataTopic = args.datatopic;
        String queryTopic = args.querytopic;
        String delTopic = args.deltopic;
        String tickTopic = args.ticktopic;
        String resultPath = args.resultpath;
        int k = args.k;
        float sampleRate = args.samplerate;
        float catchUpRatio = args.catchUpRatio;
        boolean isAsync = args.isAsync;
        boolean preload = args.preload;
        int overSampleX = args.oversample;
        List<String> predAttrs = Arrays.asList(args.predAttrs.split(","));
        String targetAttr = args.targetAttr;
        DataSource kafkaDataSource = new KafkaDataSource();
        boolean qDebug = args.qdebug;

        List<Solver>  solvers = new ArrayList<Solver>();
        if(baselines.contains("groundtruth")){
            solvers.add(new GroundTruthSolver(dataTopic, predAttrs, targetAttr, -1));
        }
        if(baselines.contains("reservoir")){
            solvers.add(new ReservoirSampling(dataTopic, predAttrs, targetAttr, sampleRate));
        }
        if(baselines.contains("pass")){
            solvers.add(
                new PASS(
                    dataTopic,
                    isAsync,
                    predAttrs,
                    targetAttr,
                    k,
                    64,
                    new NBPartitioner(),
                    catchUpRatio,
                    sampleRate,
                    overSampleX, //oversampleX
                    preload //no preload to speed up accuracy exp
                )
            );
        }
        List<Thread> solverThreads = new ArrayList<Thread>();
        for(Solver solver : solvers){
            solver.setDataset(dataset);
            solver.setResultPath(resultPath);
            kafkaDataSource.registerConsumer(solver);
            solver.initialize();
            solver.setDebugQuery(qDebug);

            Thread solverThread = new Thread(solver);
            solverThreads.add(solverThread);
            solverThread.start();
        }
        ((KafkaDataSource)kafkaDataSource).setDebug(qDebug);
        kafkaDataSource.subscribeAndRead(dataTopic, queryTopic, delTopic, tickTopic);
        try{
            System.out.println("Waiting for baselines to exit");
            for(Solver s: solvers){
                s.cleanupAndExit();
                while(s.hasStopped == false){
                    System.out.print("Waiting for to stop: " + s.getClass().getName() + "\r");
                }
            }
            System.out.println("All baselines stopped.");
        }   catch(Exception e){e.printStackTrace();}
    }
}
