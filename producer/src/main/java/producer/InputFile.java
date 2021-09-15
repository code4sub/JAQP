package producer;

import java.io.*;
import java.util.*;
import java.nio.file.*;
import java.util.stream.*;
import static java.util.stream.Collectors.toList;

public class InputFile {
    String path;
    int offset;

    List<String> getNextNRows(int n, int o){
        if(o >= 0) offset = o;
        else{
            if(n < 0){
                System.out.print("Rewind offset from " + offset);
                offset += n;
                n *= -1;
                System.out.println(" to " + offset);
            }
        }
        System.out.print("Work from offset: " + offset);
        try(Stream<String> stream = Files.lines(Paths.get(path)).skip(offset).limit(n)){
            List<String> lines = stream.collect(toList());
            offset += lines.size();
            System.out.println(" to " + offset);
            return lines;
        }catch(IOException e){
            System.out.println(e);
        }
        return new ArrayList<String>();
    }

    public String toString(){
      return path;
    }

    public InputFile(String p){
        path = p;
    }

}
