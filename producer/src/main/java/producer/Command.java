package producer;
import javax.script.ScriptEngineManager;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;

public abstract class Command{
    static InputFile dataset = null;
    static InputFile queryFile = null;
    static Producer producer = null;
    static ScriptEngine engine = new ScriptEngineManager().getEngineByExtension("js");
    public static void setInputFiles(InputFile d, InputFile q){
        dataset = d;
        queryFile = q;
    }
    public static void setProducer(Producer p){
        producer = p;
    }

    public abstract void execute();
    public static int eval(String s){
        try{
            Double d = Double.parseDouble(engine.eval(s).toString());
            return d.intValue();
        }catch(Exception e){}//e.printStackTrace();}
        return -1;
    }
    public static Command stringToCommand(String s){

        if(s.startsWith("sleep")){
            int seconds =  Integer.parseInt(s.split(":")[1].trim());
            return new SleepCommand(seconds);
        }else if(s.startsWith("insert") || s.startsWith("t_insert")){
            boolean t = false;
            if(s.startsWith("t_insert")) t = true;
            String toks[] = s.split(":")[1].trim().split(" ");
            int nRows = 0, offset = -1;
            if(toks.length == 1)
                nRows =  eval(toks[0].trim());
            else{
                nRows =  eval(toks[0].trim());
                offset = eval(toks[1].trim());
            }
            if(nRows == -1)return new DummyCommand();
            else
            return new InsertDataCommand(nRows, offset, t);
        }else if(s.startsWith("delete") || s.startsWith("t_delete")){
            boolean t = false;
            if(s.startsWith("t_delete")) t = true;
            String toks[] = s.split(":")[1].trim().split(" ");
            int nRows = 0, offset = -1;
            if(toks.length == 1)
                nRows =  eval(toks[0].trim());
            else{
                nRows =  eval(toks[0].trim());
                offset = eval(toks[1].trim());
            }
            if(nRows == -1)return new DummyCommand();
            else
            return new DeleteDataCommand(nRows, offset, t);
        }else if(s.startsWith("tick")){
            return new TickCommand();
        }else if(s.startsWith("tock")){
            return new TockCommand();
        }else if(s.startsWith("exit")){
            return new ExitCommand();
        }else if(s.startsWith("query") || s.startsWith("t_query")){
            boolean t = false;
            if(s.startsWith("t_query")) t = true;

                String toks[] = s.split(":")[1].trim().split(" ");
                int nRows = 0, offset = -1;
                if(toks.length == 1)
                    nRows =  eval(toks[0].trim());
                else{
                    nRows =  eval(toks[0].trim());
                    offset = eval(toks[1].trim());
                }
                if(nRows != -1)
                    return new InsertQueryCommand(nRows, offset, t);
                String ctrl =  s.split("query: ")[1].trim();
                return new InsertStringCommand(ctrl);

        }else{
            System.out.println("Unsupported command: " + s);
            return new DummyCommand();
        }
    }

}

class TickCommand extends Command{
    String name;
    public TickCommand(){
    }
    public void execute(){
        producer.tick("");
    }
}

class DummyCommand extends Command{
    public DummyCommand(){}
    public void execute(){}
}

class ExitCommand extends Command{
    public ExitCommand(){}
    public void execute(){
        System.exit(0);
    }
}

class TockCommand extends Command{
    String name;
    public TockCommand(){
    }
    public void execute(){
        producer.tock();
    }
}

class SleepCommand extends Command{
    int interval;
    public SleepCommand(int seconds){
        interval = seconds;
    }
    public void execute(){
        System.out.println("Sleeping " + interval + " seconds");
        try{
            Thread.sleep(interval*1000);
        }catch(InterruptedException e){
        }
    }
}

class InsertStringCommand extends Command{

    String cmd;
    public InsertStringCommand(String c){
        cmd = c;
    }

    public void execute(){
        System.out.println("Control command: " + cmd);
        producer.insertQuery(Arrays.asList(cmd), false);
    }
}

class InsertDataCommand extends Command{
    int nRows;
    int offset;
    boolean setTick = false;

    public InsertDataCommand(int n, int o, boolean b){
        nRows = n;
        offset = o;
        setTick = b;
    }

    public void execute(){
        System.out.println("Inserting " + nRows + " rows from " + dataset.toString());
        producer.insertData(dataset.getNextNRows(nRows, offset), setTick);
    }
}

class DeleteDataCommand extends Command{
    int nRows;
    int offset;
    boolean setTick = false;


    public DeleteDataCommand(int n, int o, boolean b){
        nRows = n;
        offset = o;
        setTick = b;
    }

    public void execute(){
        System.out.println("Deleting " + nRows + " rows from "+ dataset.toString());
        if(offset == -1){
            //offset = 0 for other command means to do something from current offset.
            //but it does not make sense for deletion because the next tuples are not inserted.
            //so we hack it to apply an negative nrows and indicate to delete the last nrows.
            producer.insertDeletion(dataset.getNextNRows(-nRows, offset), setTick);
        }else
            producer.insertDeletion(dataset.getNextNRows(nRows, offset), setTick);

    }
}

class InsertQueryCommand extends Command{
    int nRows;
    int offset;
    boolean setTick = false;

    public InsertQueryCommand(int n, int o, boolean b){
        nRows = n;
        offset = o;
        setTick = b;
    }
    public void execute(){
        System.out.println("Inserting " + nRows + " queries from " + queryFile.toString() + " ticking: "+setTick);
        producer.insertQuery(queryFile.getNextNRows(nRows, offset), setTick);
    }
}
