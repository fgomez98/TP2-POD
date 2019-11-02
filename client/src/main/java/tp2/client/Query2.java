package tp2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.*;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.CSVUtils;
import tpe2.api.Collators.Query2Collator;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Model.Flight;
import tpe2.api.Mappers.Query2Mapper;
import tpe2.api.Model.Tuple;
import tpe2.api.Reducers.SimpleReducerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;

public class Query2 {

    @Option(name = "-Dn", aliases = "--n", usage = "number of top airlines", required = true)
    private int n;

    private String[] ips;

    @Option(name = "-Daddresses", aliases = "--ipAddresses",
            usage = "one or more ip directions and ports"/*, required = true*/)

    private void setIps(String s) throws CmdLineException {
        ips = s.split(";");
        for (String ip : ips) {
            if (!ip.matches("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):(\\d{1,5})")) {
                throw new CmdLineException("Invalid ip and port address");
            }
        }
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

    public String[] getIps() {
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

        List<Flight> flightList = new ArrayList<>();
        try {
            flightList = CSVUtils.CSVReadFlights(query2.getDir() + "movimientos.csv");
        } catch (Exception e) {
            System.out.println("There was a problem reading the csv files");
            System.exit(1);
        }

        final HazelcastInstance hazel = Hazelcast.newHazelcastInstance();


        JobTracker jobTracker = hazel.getJobTracker("top-" + query2.getN() + "-airlines");

        IList<Flight> iList = hazel.getList("top-airlines");
        iList.clear();
        iList.addAll(flightList);

        Job<String, Flight> job = jobTracker.newJob(KeyValueSource.fromList(iList));

        ICompletableFuture<List<Tuple<String, Double>>> future = job
                .mapper(new Query2Mapper())
                .combiner(new SimpleChunkCombinerFactory())
                .reducer(new SimpleReducerFactory())
                .submit(new Query2Collator(query2.getN()));

        try {
            List<Tuple<String, Double>> result = future.get();


            List<String> list = new ArrayList<>();
            list.add("Aerolinea;Porcentaje\n");
            result.forEach((k) -> {
                        DecimalFormat numberFormat = new DecimalFormat("#.00");
                        list.add(k.getaVal() + ";" + numberFormat.format(k.getbVal()) + "%\n");
                    });
            Files.write(Paths.get(query2.getOutput()), list);

            System.out.println("done");
        } catch (Exception e) {
            System.out.println("Error calculating results");
            System.exit(1);
        }


    }


}
