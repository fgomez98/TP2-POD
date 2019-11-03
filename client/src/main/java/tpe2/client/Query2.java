package tpe2.client;

import ch.qos.logback.classic.Logger;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.*;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.CSVUtils;
import tpe2.api.Collators.Query2Collator;
import tpe2.api.Combiners.SimpleChunkCombinerFactory;
import tpe2.api.Model.Airport;
import tpe2.api.Model.Flight;
import tpe2.api.Mappers.Query2Mapper;
import tpe2.api.Model.Tuple;
import tpe2.api.Reducers.SimpleReducerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query2 implements Query {

    private int n;

    private List<String> ips;

    private String dir;

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

    public Query2(int n, List<String> ips, String dir, String output) {
        this.n = n;
        this.ips = ips;
        this.dir = dir;
        this.output = output;
    }

    public Query2() {
    }

    public static void main(String[] args) {
        Query2 query2 = new Query2();
        try {
            CmdParserUtils.init(args, query2);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }

        Logger logger = Helpers.createLoggerFor("Query2", query2.getOutput() + "query2.txt");

        List<Flight> flightList = new ArrayList<>();
        try {
            logger.info("Inicio de la lectura del archivo");
            flightList = CSVUtils.CSVReadFlights(query2.getDir() + "movimientos.csv");
            logger.info("Fin de lectura del archivo");
        } catch (Exception e) {
            System.out.println("There was a problem reading the csv files");
            System.exit(1);
        }

        final HazelcastInstance hazel = Hazelcast.newHazelcastInstance();

        logger.info("Inicio del trabajo map/reduce");

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

            logger.info("Fin del trabajo map/reduce");

            List<String> list = new ArrayList<>();
            list.add("Aerolinea;Porcentaje\n");
            result.forEach((k) -> {
                DecimalFormat numberFormat = new DecimalFormat("#.00");
                list.add(k.getaVal() + ";" + numberFormat.format(k.getbVal()) + "%\n");
            });
            Files.write(Paths.get(query2.getOutput() + "query2.csv"), list);
            System.out.println("done");
        } catch (Exception e) {
            System.out.println("Error calculating results");
            System.exit(1);
        }
        System.exit(0);

    }

    @Override
    public void runQuery(HazelcastInstance hazel, List<Airport> airports, List<Flight> flightList) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = hazel.getJobTracker("top-" + this.getN() + "-airlines");

        IList<Flight> iList = hazel.getList("top-airlines");
        iList.clear();
        iList.addAll(flightList);

        Job<String, Flight> job = jobTracker.newJob(KeyValueSource.fromList(iList));
        ICompletableFuture<List<Tuple<String, Double>>> future = job
                .mapper(new Query2Mapper())
                .combiner(new SimpleChunkCombinerFactory())
                .reducer(new SimpleReducerFactory())
                .submit(new Query2Collator(this.getN()));

        try {
            List<Tuple<String, Double>> result = future.get();

            List<String> list = new ArrayList<>();
            list.add("Aerolinea;Porcentaje\n");

            CSVUtils.CSVWrite(Paths.get(this.getOutput() + "/query2.csv"),
                    result,
                    "Aerolínea;Porcentaje\n",
                    (e) -> {
                        DecimalFormat numberFormat = new DecimalFormat("#.00");
                        return e.getaVal() + ";" + numberFormat.format(e.getbVal()) + "%\n";
                    });
        } catch (Exception e) {
            System.err.println("Error while writing results on file");
            System.exit(1);
        }
    }
}
