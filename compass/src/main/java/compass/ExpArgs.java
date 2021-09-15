package compass;


import com.beust.jcommander.Parameter;

public class ExpArgs {

    @Parameter(names = "-preload", description = "Preload dataset to increase throughpu")
    public boolean preload = false;

    @Parameter(names = "-async", description = "Enable multi-thread mode")
    public boolean isAsync = false;

    @Parameter(names = "-k", description = "Number of partitions")
    public int k = 64;

    @Parameter(names = "-dataset", description = "Name of dataset: taxi, intelwireless", required=true)
    public String dataset;

    @Parameter(names = "-datatopic", description = "Data topic", required=true)
    public String datatopic;

    @Parameter(names = "-querytopic", description = "Query topic", required=true)
    public String querytopic;

    @Parameter(names = "-deltopic", description = "Deletion topic", required=true)
    public String deltopic;

    @Parameter(names = "-ticktopic", description = "Ticktock topic", required=true)
    public String ticktopic;

    @Parameter(names = "-pattr", description = "Predicate attributes separated by comma", required = true)
    public String predAttrs;

    @Parameter(names = "-tattr", description = "Target attribute", required = true)
    public String targetAttr;

    @Parameter(names = "-rpath", description = "Folder to save results", required = true)
    public String resultpath;

    @Parameter(names = "-qdebug", description = "Debug query processing")
    public boolean qdebug = false;

    @Parameter(names = "-oversamplex", description = "OversampleX")
    public int oversample = 1;

    @Parameter(names = "-baselines", description = "Baselines separated by comma")
    public String baselines = "pass,groundtruth,reservoir";

    @Parameter(names = "-catchup", description = "Catch-up ratio")
    public float catchUpRatio = 0.1f;

    @Parameter(names = "-samplerate", description = "Sample rate")
    public float samplerate = 0.01f;
}