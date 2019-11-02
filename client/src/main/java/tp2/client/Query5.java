package tp2.client;

import ch.qos.logback.classic.Logger;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.Model.Airport;
import tpe2.api.CSVUtils;
import tpe2.api.Model.Flight;
import tpe2.api.Model.Tuple;
import tpe2.api.Collators.Query5Collator;
import tpe2.api.Combiners.Query5CombinerFactory;
import tpe2.api.Mappers.Query5Mapper;
import tpe2.api.Reducers.Query5ReducerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Query5 {

    private static final String MOVEMENTS_MAP = "g8-q5-movements";

    @Option(name = "-Dn", aliases = "--n", usage = "number of top airlines", required = true)
    private int n;

    private List<String> ips;

    @Option(name = "-Daddresses", aliases = "--ipAddresses",
            usage = "one or more ip directions and ports"/*, required = true*/)
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

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Query5 query5 = new Query5();
        try {
            CmdParserUtils.init(args, query5);
        } catch (IOException e) {
            System.out.println("There was a problem reading the arguments");
            System.exit(1);
        }
        q5(query5);
    }

    private static void q5(Query5 query5) throws ExecutionException, InterruptedException {
        List<Flight> flightList = new ArrayList<>();
        List<Airport> airports = new ArrayList<>();

        Logger logger = Helpers.createLoggerFor("Query5", query5.getOutput()+"/query5.txt");

        try {
            logger.info("Inicio de la lectura del archivo");
            flightList = CSVUtils.CSVReadFlights(query5.getDir() + "/movimientos.csv");
            airports = CSVUtils.CSVReadAirports(query5.getDir() + "/aeropuertos.csv");
            logger.info("Fin de lectura del archivo");
        } catch (Exception e) {
            System.out.println("There was a problem reading the csv files");
            System.exit(1);
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        logger.info("Inicio del trabajo map/reduce");

        JobTracker jobTracker = hz.getJobTracker("query-5-job");


        final IList<Flight> movementsList = hz.getList(MOVEMENTS_MAP);
        movementsList.clear();
        movementsList.addAll(flightList);

        // Contamos los movimientos totales que correspondan segun su destino/aterrizaje
        final KeyValueSource<String, Flight> movementSource = KeyValueSource.fromList(movementsList);

        List<String> oacis = new ArrayList<>();
        airports.forEach((a) -> {
            if (a.getOaci() != null && !a.getOaci().equals("")) {
                oacis.add(a.getOaci());
            }
        });

        final Job<String, Flight> job = jobTracker.newJob(movementSource);
        final ICompletableFuture<List<Tuple<String, Double>>> future = job
                .mapper(new Query5Mapper(oacis))
                .combiner(new Query5CombinerFactory())
                .reducer(new Query5ReducerFactory())
                .submit(new Query5Collator(query5.getN()));

        List<Tuple<String, Double>> movementsMap = future.get();

        logger.info("Fin del trabajo map/reduce");

        List<String> list = new ArrayList<>();
        list.add("OACI;Porcentaje\n");

        movementsMap.forEach((k) -> {
            DecimalFormat numberFormat = new DecimalFormat("#.00");
            list.add(k.getaVal() + ";" + numberFormat.format(k.getbVal()) + "%\n");
        });

        try {
            Files.write(Paths.get(query5.getOutput()+"query5.csv"), list);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("done");
        System.exit(0);
    }
}
