package producer;

import com.beust.jcommander.Parameter;
public class ProducerArgs {

    @Parameter(names = "-csvpath", description = "Path of csv file to insert to data topic", required=true)
    public String csvPath;

    @Parameter(names = "-datatopic", description = "Data topic", required=true)
    public String datatopic;

    @Parameter(names = "-querytopic", description = "Query topic", required=true)
    public String querytopic;

    @Parameter(names = "-deltopic", description = "Deletion topic", required=true)
    public String deltopic;

    @Parameter(names = "-ticktopic", description = "Ticktock topic", required=true)
    public String ticktopic;

    @Parameter(names = "-cmdpath", description = "Path to the command file", required = true)
    public String cmdPath;

    @Parameter(names = "-querypath", description = "Path to the query file", required = true)
    public String queryPath;

}