package producer;
import com.google.common.base.Stopwatch;
import java.util.*;
import com.beust.jcommander.JCommander;
public class Experiment{
    public static void main(String[] argv){
        ProducerArgs args = new ProducerArgs();
        JCommander.newBuilder().addObject(args).build().parse(argv);

        String csvPath = args.csvPath;
        String dataTopic = args.datatopic;
        String queryTopic = args.querytopic;
        String delTopic = args.deltopic;
        String tickTopic = args.ticktopic;
        String cmdPath = args.cmdPath;
        String queryPath = args.queryPath;

        Conductor conductor = new Conductor(csvPath, queryPath, cmdPath, dataTopic, queryTopic, delTopic, tickTopic);

        if(cmdPath.equals("stdin")){
            System.out.println("Mannual mode, enter command below:");
            System.out.println("E.g.: query: 5 | sleep: 1 | insert: 1 | delete: 1 | query: save | query: exit | exit | tick | tock");
            Scanner scanner = new Scanner(System.in);

            while(true){
                String s = scanner.nextLine();
                try{
                    Command cmd = Command.stringToCommand(s);
                    Stopwatch timer = Stopwatch.createStarted();
                    cmd.execute();

                    System.out.println("Executed in " + timer.stop());
                }catch(Exception e){
                }
                if(s.equals("exit")) break;
            }
            scanner.close();
        }else{
            conductor.executeAllCommands();
        }
    }
}