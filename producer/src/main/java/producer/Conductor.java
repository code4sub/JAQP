package producer;

import java.io.*;
import java.util.*;
import java.nio.file.*;
import java.util.stream.*;
import java.text.SimpleDateFormat;

import static java.util.stream.Collectors.toList;

public class Conductor {
    InputFile dataset;
    InputFile queryFile;
    Producer producer;
    List<Command> commandQueue;

    public Conductor(String datasetPath, String queryPath, String commandFilePath,
                    String dataTopic, String queryTopic, String deletionTopic, String tickTopic){
        dataset = new InputFile(datasetPath);
        System.out.println("Dataset: " + datasetPath);
        System.out.println("Commands: " + commandFilePath);
        producer = new Producer(dataTopic, queryTopic, deletionTopic, tickTopic);
        queryFile = new InputFile(queryPath);
        System.out.println("Query: " + queryPath);
        Command.setInputFiles(dataset, queryFile);
        Command.setProducer(producer);
        buildCommandQueue(commandFilePath);
    }

    void buildCommandQueue(String commandFilePath){
        try(Stream<String> stream = Files.lines(Paths.get(commandFilePath))){
            commandQueue = stream.map(Command::stringToCommand).collect(toList());
            System.out.println("Loaded " + commandQueue.size() + " commands from " + commandFilePath);
        }catch(IOException e){
            System.out.println(e);
        }
    }

    public void executeAllCommands(){
        System.out.println("Executing " + commandQueue.size() + " commands...");
        int i = 0;
        for(Command command : commandQueue){
            String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());;
            System.out.println("\n#" + i++ + "\t" + time);
            command.execute();
        }
        System.out.println("\nAll commands executed.");
    }
}