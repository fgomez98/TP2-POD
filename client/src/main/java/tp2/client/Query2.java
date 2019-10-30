package tp2.client;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Query2 {

    @Option(name = "-Dn", aliases = "--n", usage = "number of top airlines", required = true)
    private int n;

    private List<String> ips;

    @Option(name = "-Daddresses", aliases = "--ipAddresses",
            usage = "one or more ip directions and ports", required = true)
    private void setIps(String s) throws CmdLineException {
        List<String> list = Arrays.asList(s.split(";"));
        for (String ip : list) {
            if (!ip.matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})")) {
                throw new CmdLineException("Invalid ip and port address");
            }
        }
        this.ips = list;
    }


    @Option(name = "-DinPath", aliases = "--inPath", usage = "input directory path", required = true)
    private String dir;

    @Option(name = "-DoutPath", aliases = "--outPath", usage = "Output path where .txt and .csv are")
    private String output;

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public List<String> getIps() {
        return ips;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public static void main(String[] args) {
        Query2 query2 = new Query2();
        try {
            CmdParserUtils.init(args, query2);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }

        for (String ip : query2.getIps()) {
            System.out.println(ip);
        }
    }


}
