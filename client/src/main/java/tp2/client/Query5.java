package tp2.client;

import com.hazelcast.core.*;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import tpe2.api.Airport;
import tpe2.api.CSVUtils;
import tpe2.api.Flight;
import tpe2.api.Tuple;
import tpe2.api.query5.Q5Collator;
import tpe2.api.query5.Q5CombinerFactory;
import tpe2.api.query5.Q5Mapper;
import tpe2.api.query5.Q5ReducerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Query5 {

    private static final String AIRPORTS_MAP = "g8-q5-airports";
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

        try {
            flightList = CSVUtils.CSVReadFlights(query5.getDir() + "movimientos.csv");
            airports = CSVUtils.CSVReadAirports(query5.getDir() + "aeropuertos.csv");
        } catch (Exception e) {
            System.out.println("There was a problem reading the csv files");
            System.exit(1);
        }

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        JobTracker jobTracker = hz.getJobTracker("query-5-job");


        final IList<Flight> movementsList = hz.getList(MOVEMENTS_MAP);
        movementsList.clear();
        movementsList.addAll(flightList);
//        final IList<Airport> airportsList = hz.getList(AIRPORTS_MAP);
//        airportsList.clear();
//        airportsList.addAll(airports);

        // Contamos los movimientos totales que correspondan segun su destino/aterrizaje
//        final KeyValueSource<String, Airport> airportSource = KeyValueSource.fromList(airportsList);
        final KeyValueSource<String, Flight> movementSource = KeyValueSource.fromList(movementsList);


        List<String> oacis = new ArrayList<>();
        airports.forEach((a)-> oacis.add(a.getOaci()));

        final Job<String, Flight> job = jobTracker.newJob(movementSource);
        final ICompletableFuture<List<Tuple<String, Double>>> future = job
                .mapper(new Q5Mapper())
                .combiner(new Q5CombinerFactory())
                .reducer(new Q5ReducerFactory())
                .submit(new Q5Collator(oacis,query5.getN()));

        List<Tuple<String, Double>> movementsMap = future.get();

        // solo me interesan los aeropuertos presentes
        //movementsMap.removeIf((e) -> !airportSource.getAllKeys().contains(e.getaVal()));
        System.out.println("OACI;Porcentaje");

        List<String> list = new ArrayList<>();
        list.add("OACI;Porcentaje\n");

        movementsMap.forEach((k) -> {
            DecimalFormat numberFormat = new DecimalFormat("#.00");
            list.add(k.getaVal() + ";" + numberFormat.format(k.getbVal()) + "%\n");
        });
        try {
            Files.write(Paths.get(query5.getOutput()), list);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("done");
    }
}
