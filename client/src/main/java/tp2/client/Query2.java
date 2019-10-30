package tp2.client;

import org.kohsuke.args4j.Option;

import java.util.List;

public class Query2 {

    @Option(name = "-Dn", aliases = "--n", usage = "number of top airlines", required = true)
    private int n;

    @Option(name = "-Daddresses", aliases = "--ipAddresses", usage = "one or more ip directions and ports", required = true)
    private List<Integer> ips;

    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String dir;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String output;
}
